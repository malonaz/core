package authentication_test

import (
	"context"
	"testing"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	"github.com/malonaz/core/go/grpc/middleware"
)

// The four mapping expressions turn claims into a session: a typed list of grants built by the short name `Grant`,
// over the subject and attributes computed before it.
func TestMappingExpressions(t *testing.T) {
	f := newFixture(t)
	f.issuer.SubjectCel = `"organizations/" + claims.org + "/users/" + claims.user`
	f.issuer.AttributesCel = `{"organization": "organizations/" + claims.org, "roles": claims.roles}`
	f.issuer.ActorCel = `has(claims.impersonator) ? claims.impersonator : ""`
	f.issuer.GrantsCel = `attributes.roles.map(r, Grant{role: r, scope: "*"}) + [Grant{role: "lister", scope: attributes.organization}]`
	interceptor := f.interceptor(t)

	token := f.token(t, map[string]any{"org": "1", "user": "2", "roles": []any{"reader"}, "impersonator": "jdoe@example.com"})
	seen, err := unary(t, interceptor, getBookMethod, bearer(token), getBookRequest)
	require.NoError(t, err)
	want := &authenticationpb.Session{
		Principal: &authenticationpb.Principal{
			Subject:    "organizations/1/users/2",
			Attributes: mustStruct(map[string]any{"organization": "organizations/1", "roles": []any{"reader"}}),
		},
		Actor: &authenticationpb.Principal{Subject: "jdoe@example.com"},
		Grants: []*authenticationpb.Grant{
			{Role: "reader", Scope: "*"},
			{Role: "lister", Scope: "organizations/1"},
		},
	}
	seen.session.Client = nil
	require.True(t, proto.Equal(want, seen.session), "%v", seen.session)

	t.Run("without impersonator, no actor", func(t *testing.T) {
		token := f.token(t, map[string]any{"org": "1", "user": "2", "roles": []any{"reader"}})
		seen, err := unary(t, interceptor, getBookMethod, bearer(token), getBookRequest)
		require.NoError(t, err)
		require.Nil(t, seen.session.GetActor())
	})
	t.Run("defaults: subject is sub, attributes the claims", func(t *testing.T) {
		f := newFixture(t)
		seen, err := unary(t, f.interceptor(t), getBookMethod, bearer(f.token(t, map[string]any{"shelf": "shelves/1"})), getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "users/alice", seen.session.GetPrincipal().GetSubject())
		fields := seen.session.GetPrincipal().GetAttributes().GetFields()
		require.Equal(t, "shelves/1", fields["shelf"].GetStringValue())
		require.Equal(t, "users/alice", fields["sub"].GetStringValue())
		require.Equal(t, "test", fields["iss"].GetStringValue())
	})
}

func TestGetSession(t *testing.T) {
	_, err := authentication.GetSession(context.Background())
	require.ErrorIs(t, err, authentication.ErrSessionNotFound)
	session := &authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "users/alice"}}
	got, err := authentication.GetSession(authenticationtest.NewContext(context.Background(), session))
	require.NoError(t, err)
	require.Same(t, session, got)
}

func TestCompareClientVersion(t *testing.T) {
	session := &authenticationpb.Session{Client: &authenticationpb.Client{Version: &authenticationpb.ClientVersion{Major: 3, Minor: 12, Patch: 1}}}
	require.Equal(t, 0, authentication.CompareClientVersion(session, 3, 12, 1))
	require.Equal(t, 1, authentication.CompareClientVersion(session, 3, 11, 9))
	require.Equal(t, -1, authentication.CompareClientVersion(session, 4, 0, 0))
	require.Equal(t, -1, authentication.CompareClientVersion(&authenticationpb.Session{}, 0, 0, 0))
}

// The session is logged once authenticated, before the decision, so a denial says who was denied; with it the issuer
// whose token proved the principal and, once allowed, the role that allowed the call.
func TestLogFields(t *testing.T) {
	f := newFixture(t)
	interceptor := f.interceptor(t)
	token := f.token(t, map[string]any{"shelf": "shelves/1"})

	fields := logFields(interceptor, getBookMethod, metadata.Join(bearer(token), metadata.Pairs("x-client-platform", "ios")))
	require.Equal(t, "users/alice", fields["principal"])
	require.Equal(t, "shelves/1", fields["attribute.shelf"])
	require.Equal(t, "ios", fields["client_platform"])

	for _, test := range []struct {
		name   string
		method string
		md     metadata.MD
		want   map[string]any
	}{
		{name: "token", method: getBookMethod, md: bearer(token), want: map[string]any{"issuer": "test", "role": "reader"}},
		{name: "cookie", method: getBookMethod, md: webCookie(token), want: map[string]any{"issuer": "test", "role": "reader"}},
		{name: "api key", method: getBookMethod, md: apiKey("key-1"), want: map[string]any{"role": "reader"}},
		{name: "public method, anonymous", method: echoMethod, want: map[string]any{"role": "public"}},
		{name: "denied", method: createBookMethod, md: bearer(token), want: map[string]any{"issuer": "test"}},
		{name: "carried", method: getBookMethod, md: serverToken(sign(t, serverSecret, jwa.HS256, serverClaims())), want: map[string]any{"role": "carrier"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fields := logFields(interceptor, test.method, test.md)
			got := map[string]any{}
			for _, key := range []string{"issuer", "role"} {
				if value, ok := fields[key]; ok {
					got[key] = value
				}
			}
			require.Equal(t, test.want, got)
		})
	}
}

// logFields runs a unary call through the interceptor under the log's tag, returning the fields logged.
func logFields(interceptor *authentication.Interceptor, fullMethod string, md metadata.MD) map[string]any {
	info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
	handler := func(ctx context.Context, request any) (any, error) { return nil, nil }
	fieldMap := map[string]any{}
	ctx := metadata.NewIncomingContext(context.Background(), md)
	middleware.UnaryServerLogContextTagInitializer()(ctx, nil, info, func(ctx context.Context, request any) (any, error) {
		_, err := interceptor.Unary()(ctx, request, info, handler)
		fields, _ := middleware.GetFields(ctx)
		for i := 0; i+1 < len(fields); i += 2 {
			fieldMap[fields[i].(string)] = fields[i+1]
		}
		return nil, err
	})
	return fieldMap
}

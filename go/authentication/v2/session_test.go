package authentication_test

import (
	"context"
	"net"
	"testing"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
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
	t.Run("the subject is the token's sub when the attributes drop it", func(t *testing.T) {
		f := newFixture(t)
		f.issuer.AttributesCel = `{"role": "admin"}`
		seen, err := unary(t, f.interceptor(t), getBookMethod, bearer(f.token(t, nil)), getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "users/alice", seen.session.GetPrincipal().GetSubject())
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

// The client is read from the call's headers: the IP address is X-Forwarded-For's leftmost entry, else the peer's; the
// user agent the gateway's, else the forwarded one, else gRPC's.
func TestClient(t *testing.T) {
	interceptor := newFixture(t).interceptor(t)
	call := func(ctx context.Context, md metadata.MD) (*authenticationpb.Client, error) {
		var client *authenticationpb.Client
		handler := func(ctx context.Context, _ any) (any, error) {
			session, err := authentication.GetSession(ctx)
			client = session.GetClient()
			return nil, err
		}
		_, err := interceptor.Unary()(metadata.NewIncomingContext(ctx, md), nil, &grpc.UnaryServerInfo{FullMethod: echoMethod}, handler)
		return client, err
	}
	peerContext := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 5555}})
	for name, test := range map[string]struct {
		md   metadata.MD
		want *authenticationpb.Client
	}{
		"no headers":            {metadata.MD{}, &authenticationpb.Client{IpAddress: "192.0.2.1:5555"}},
		"one forwarded address": {metadata.Pairs("x-forwarded-for", "203.0.113.7"), &authenticationpb.Client{IpAddress: "203.0.113.7"}},
		"a chain":               {metadata.Pairs("x-forwarded-for", "203.0.113.7, 10.0.0.1, 10.0.0.2"), &authenticationpb.Client{IpAddress: "203.0.113.7"}},
		"spaces":                {metadata.Pairs("x-forwarded-for", "  203.0.113.7 ,10.0.0.1"), &authenticationpb.Client{IpAddress: "203.0.113.7"}},
		"two headers":           {metadata.Pairs("x-forwarded-for", "203.0.113.7,10.0.0.1", "x-forwarded-for", "10.0.0.2"), &authenticationpb.Client{IpAddress: "203.0.113.7"}},
		"gateway user agent": {
			metadata.Pairs("grpcgateway-user-agent", "gateway", "x-forwarded-user-agent", "forwarded", "user-agent", "grpc"),
			&authenticationpb.Client{IpAddress: "192.0.2.1:5555", UserAgent: "gateway"},
		},
		"forwarded user agent": {
			metadata.Pairs("x-forwarded-user-agent", "forwarded", "user-agent", "grpc"),
			&authenticationpb.Client{IpAddress: "192.0.2.1:5555", UserAgent: "forwarded"},
		},
		"user agent": {metadata.Pairs("user-agent", "grpc"), &authenticationpb.Client{IpAddress: "192.0.2.1:5555", UserAgent: "grpc"}},
		"platform, version, time zone": {
			metadata.Pairs("x-client-platform", "ios", "x-client-version", "3.12.1", "x-client-timezone", "America/New_York"),
			&authenticationpb.Client{
				IpAddress: "192.0.2.1:5555", Platform: "ios", TimeZone: "America/New_York",
				Version: &authenticationpb.ClientVersion{Major: 3, Minor: 12, Patch: 1},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, err := call(peerContext, test.md)
			require.NoError(t, err)
			require.True(t, proto.Equal(test.want, client), "%v", client)
		})
	}
	t.Run("no peer", func(t *testing.T) {
		client, err := call(context.Background(), metadata.MD{})
		require.NoError(t, err)
		require.Empty(t, client.GetIpAddress())
	})
	for _, version := range []string{"3.x.1", "3.1"} {
		t.Run("version "+version, func(t *testing.T) {
			_, err := call(peerContext, metadata.Pairs("x-client-version", version))
			require.Equal(t, codes.InvalidArgument, code(err), "%v", err)
		})
	}
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

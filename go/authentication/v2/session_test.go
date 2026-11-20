package authentication_test

import (
	"context"
	"testing"

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

// The session is logged once authenticated, before the decision, so a denial says who was denied.
func TestLogFields(t *testing.T) {
	f := newFixture(t)
	interceptor := f.interceptor(t)
	var fields []any
	logged := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, request)
		fields, _ = middleware.GetFields(ctx)
		return response, err
	}
	handler := func(ctx context.Context, request any) (any, error) { return nil, nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/malonaz.test.library.library_service.v1.LibraryService/CreateBook"}
	md := metadata.Join(bearer(f.token(t, map[string]any{"shelf": "shelves/1"})), metadata.Pairs("x-client-platform", "ios"))
	ctx := metadata.NewIncomingContext(context.Background(), md)
	_, err := middleware.UnaryServerLogContextTagInitializer()(ctx, nil, info, func(ctx context.Context, request any) (any, error) {
		return logged(ctx, request, info, func(ctx context.Context, request any) (any, error) {
			return interceptor.Unary()(ctx, request, info, handler)
		})
	})
	require.Error(t, err)
	fieldMap := map[string]any{}
	for i := 0; i+1 < len(fields); i += 2 {
		fieldMap[fields[i].(string)] = fields[i+1]
	}
	require.Equal(t, "users/alice", fieldMap["principal"])
	require.Equal(t, "shelves/1", fieldMap["attribute.shelf"])
	require.Equal(t, "ios", fieldMap["client_platform"])
}

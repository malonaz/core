package authentication

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

const testCookieName = "session"

var testJwtIssuer = &authenticationpb.JwtIssuer{
	Id:        "test-issuer",
	Issuer:    "test",
	Audience:  "test",
	KeySource: &authenticationpb.JwtIssuer_SymmetricKey{SymmetricKey: "0123456789abcdef0123456789abcdef"},
	// listBookMethod is private: a JWT holder may call it, an anonymous caller may not.
	MethodToAuthorizationCel: map[string]string{listBookMethod: "true"},
}

// writeConfig writes a configuration file the interceptors parse.
func writeConfig(t *testing.T, name string, config proto.Message) string {
	t.Helper()
	bytes, err := pbutil.JSONMarshal(config)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, bytes, 0o600))
	return path
}

// mintJwt mints a JWT under the test issuer, signed with the given key.
func mintJwt(t *testing.T, symmetricKey string) string {
	t.Helper()
	jwtIssuer := proto.CloneOf(testJwtIssuer)
	jwtIssuer.KeySource = &authenticationpb.JwtIssuer_SymmetricKey{SymmetricKey: symmetricKey}
	jwtMinter, err := NewJwtMinter(jwtIssuer)
	require.NoError(t, err)
	token, err := jwtMinter.MintJwt(MintJwtOpts{Subject: "users/alice", Lifetime: time.Hour})
	require.NoError(t, err)
	return token
}

// authenticate runs a call through the JWT then the permission interceptor, as
// the external gRPC server chains them, returning the session it reached the
// handler with.
func authenticate(t *testing.T, fullMethod string, md metadata.MD) (*authenticationpb.Session, error) {
	t.Helper()
	sessionManager := NewSessionManager(&SessionManagerOpts{Secret: "session-secret"})
	jwtConfig := &authenticationpb.JwtConfiguration{Issuers: []*authenticationpb.JwtIssuer{testJwtIssuer}}
	jwtOpts := &JwtAuthenticationInterceptorOpts{Config: writeConfig(t, "jwt.json", jwtConfig), CookieName: testCookieName}
	jwtInterceptor, err := NewJwtAuthenticationInterceptor(context.Background(), jwtOpts, sessionManager)
	require.NoError(t, err)
	permissionConfig := &authenticationpb.PermissionConfiguration{
		ServiceAccounts: []*authenticationpb.ServiceAccount{{
			Id:          "test-service",
			Type:        authenticationpb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_INTERNAL_SERVICE,
			Permissions: []string{getBookMethod},
		}},
		Roles:         []*authenticationpb.Role{{Id: "test-role"}},
		PublicMethods: []string{getBookMethod},
		JwtIssuers:    []*authenticationpb.JwtIssuer{testJwtIssuer},
	}
	permissionOpts := &PermissionAuthenticationInterceptorOpts{Config: writeConfig(t, "permissions.json", permissionConfig)}
	permissionInterceptor, err := NewPermissionAuthenticationInterceptor(permissionOpts, sessionManager)
	require.NoError(t, err)

	var session *authenticationpb.Session
	handler := func(ctx context.Context, request any) (any, error) {
		var err error
		session, err = GetSession(ctx)
		return nil, err
	}
	info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
	ctx := metadata.NewIncomingContext(context.Background(), md)
	_, err = jwtInterceptor.Unary()(ctx, nil, info, func(ctx context.Context, request any) (any, error) {
		return permissionInterceptor.Unary()(ctx, request, info, handler)
	})
	return session, err
}

func bearer(token string) metadata.MD { return metadata.Pairs("authorization", "Bearer "+token) }
func cookie(token string) metadata.MD { return metadata.Pairs("cookie", testCookieName+"="+token) }

func TestJwtAuthentication(t *testing.T) {
	validToken := mintJwt(t, "0123456789abcdef0123456789abcdef")
	invalidTokens := map[string]string{
		"rotated key": mintJwt(t, "fedcba9876543210fedcba9876543210"),
		"malformed":   "not-a-jwt",
	}

	t.Run("valid bearer authenticates", func(t *testing.T) {
		session, err := authenticate(t, listBookMethod, bearer(validToken))
		require.NoError(t, err)
		require.Equal(t, "test-issuer", session.GetJwtIdentity().GetIssuerId())
	})

	t.Run("valid cookie authenticates", func(t *testing.T) {
		session, err := authenticate(t, listBookMethod, cookie(validToken))
		require.NoError(t, err)
		require.Equal(t, "test-issuer", session.GetJwtIdentity().GetIssuerId())
	})

	for name, invalidToken := range invalidTokens {
		t.Run("invalid bearer is rejected: "+name, func(t *testing.T) {
			for _, fullMethod := range []string{getBookMethod, listBookMethod} {
				_, err := authenticate(t, fullMethod, bearer(invalidToken))
				require.True(t, status.HasCode(err, codes.Unauthenticated), "%s: %v", fullMethod, err)
			}
		})

		t.Run("invalid cookie is no credential: "+name, func(t *testing.T) {
			// A public method runs anonymously, as with no cookie at all.
			session, err := authenticate(t, getBookMethod, cookie(invalidToken))
			require.NoError(t, err)
			require.NotNil(t, session.GetAnonymousIdentity())
			// A private method requires a credential the caller does not have.
			_, err = authenticate(t, listBookMethod, cookie(invalidToken))
			require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
		})

		t.Run("valid bearer wins over an invalid cookie: "+name, func(t *testing.T) {
			session, err := authenticate(t, listBookMethod, metadata.Join(bearer(validToken), cookie(invalidToken)))
			require.NoError(t, err)
			require.Equal(t, "test-issuer", session.GetJwtIdentity().GetIssuerId())
		})
	}
}

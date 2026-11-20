package authentication_test

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	mockpb "github.com/malonaz/core/genproto/test/mock/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	"github.com/malonaz/core/go/pbutil"
)

const (
	echoMethod        = "/malonaz.test.mock.v1.MockService/Echo"
	echoStreamMethod  = "/malonaz.test.mock.v1.MockService/EchoStream"
	getBookMethod     = "/malonaz.test.library.library_service.v1.LibraryService/GetBook"
	listBooksMethod   = "/malonaz.test.library.library_service.v1.LibraryService/ListBooks"
	reflectionMethod  = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
	mockService       = "malonaz.test.mock.v1.MockService"
	libraryService    = "malonaz.test.library.library_service.v1.LibraryService"
	reflectionService = "grpc.reflection.v1.ServerReflection"

	serverSecret   = "server-secret-0123456789abcdef01"
	issuerSecret   = "issuer-secret-0123456789abcdef01"
	outsiderSecret = "outsider-secret-0123456789abcdef"
	cookieName     = "session"
	carrierCEL     = `caller.subject != "" && principal.subject != caller.subject`
)

// Every message the tests name, registered in the process.
var (
	_ = librarypb.LibraryService_ServiceDesc
	_ = mockpb.MockService_ServiceDesc
)

// fixture is a configuration the tests adjust, an issuer whose tokens they sign, and the interceptor built from them.
type fixture struct {
	configuration *authenticationpb.Configuration
	opts          *authentication.Opts
	issuer        *authenticationpb.Issuer
	signer        *authentication.Signer
}

// newFixture returns the configuration most tests share:
//   - Echo is public; GetBook needs the reader role; ListBooks the lister role, whose condition reads the request;
//   - the test issuer grants reader and lister over "*";
//   - onikiri (API keys key-1, key-2) holds reader; gateway and worker hold carrier, worker also reader;
//   - outsider, with keys of its own, holds reader.
func newFixture(t testing.TB) *fixture {
	t.Helper()
	issuer := &authenticationpb.Issuer{
		Id:        "test",
		Issuer:    "test",
		Audience:  "test",
		Keys:      &authenticationpb.Issuer_Jwks{Jwks: authenticationtest.KeySet(mustSymmetricKey(issuerSecret))},
		GrantsCel: `[Grant{role: "reader", scope: "*"}, Grant{role: "lister", scope: "*"}]`,
	}
	signer, err := authentication.NewSigner(issuer, mustSymmetricKey(issuerSecret))
	require.NoError(t, err)
	return &fixture{
		issuer: issuer,
		signer: signer,
		configuration: &authenticationpb.Configuration{
			Issuers: []*authenticationpb.Issuer{issuer},
			ServiceAccounts: []*authenticationpb.ServiceAccount{
				{Id: "onikiri", Grants: []*authenticationpb.Grant{{Role: "reader", Scope: "*"}}},
				{Id: "gateway", Grants: []*authenticationpb.Grant{{Role: "carrier", Scope: "*"}}},
				{Id: "worker", Grants: []*authenticationpb.Grant{{Role: "reader", Scope: "*"}, {Role: "carrier", Scope: "*"}}},
				{
					Id:     "outsider",
					Grants: []*authenticationpb.Grant{{Role: "reader", Scope: "*"}},
					Keys:   &authenticationpb.ServiceAccount_Jwks{Jwks: authenticationtest.KeySet(mustSymmetricKey(outsiderSecret))},
				},
			},
			Roles: []*authenticationpb.Role{
				{Id: "public", Rules: []*authenticationpb.Rule{{Methods: []string{echoMethod}}}},
				{Id: "reader", Rules: []*authenticationpb.Rule{{Methods: []string{getBookMethod}}}},
				{Id: "lister", Rules: []*authenticationpb.Rule{{Methods: []string{listBooksMethod}, ConditionCel: `request.parent == principal.attributes.shelf`}}},
				{Id: "carrier", Rules: []*authenticationpb.Rule{{Methods: []string{"*"}, ConditionCel: carrierCEL}}},
			},
			PublicRoles: []string{"public"},
		},
		opts: &authentication.Opts{
			APIKeys:      "onikiri:key-1, onikiri:key-2,worker:key-3",
			APIKeyHeader: "x-api-key",
			CookieName:   cookieName,
			ServerSecret: serverSecret,
		},
	}
}

// writeConfiguration writes the configuration where the interceptor loads it: JSON is jsonnet.
func writeConfiguration(t testing.TB, configuration *authenticationpb.Configuration) string {
	t.Helper()
	bytes, err := pbutil.JSONMarshal(configuration)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "authentication.jsonnet")
	require.NoError(t, os.WriteFile(path, bytes, 0o600))
	return path
}

func (f *fixture) build(t testing.TB, authenticators ...authentication.Authenticator) (*authentication.Interceptor, error) {
	t.Helper()
	opts := *f.opts
	opts.Config = writeConfiguration(t, f.configuration)
	return authentication.NewInterceptor(context.Background(), &opts, authenticators...)
}

func (f *fixture) interceptor(t testing.TB, authenticators ...authentication.Authenticator) *authentication.Interceptor {
	t.Helper()
	interceptor, err := f.build(t, authenticators...)
	require.NoError(t, err)
	return interceptor
}

// token signs a token of the test issuer for alice, with the given claims.
func (f *fixture) token(t testing.TB, claims map[string]any) string {
	t.Helper()
	token, _, err := f.signer.Sign(authentication.Token{Subject: "users/alice", Lifetime: time.Hour, Claims: claims})
	require.NoError(t, err)
	return token
}

// handled is what a handler saw: its session and incoming metadata.
type handled struct {
	session  *authenticationpb.Session
	metadata metadata.MD
}

// unary runs a unary call through the interceptor as a server would, returning what the handler saw.
func unary(t *testing.T, interceptor *authentication.Interceptor, fullMethod string, md metadata.MD, request any) (*handled, error) {
	t.Helper()
	var seen *handled
	handler := func(ctx context.Context, _ any) (any, error) {
		session, err := authentication.GetSession(ctx)
		if err != nil {
			return nil, err
		}
		incoming, _ := metadata.FromIncomingContext(ctx)
		seen = &handled{session: session, metadata: incoming}
		return nil, nil
	}
	ctx := metadata.NewIncomingContext(context.Background(), md)
	_, err := interceptor.Unary()(ctx, request, &grpc.UnaryServerInfo{FullMethod: fullMethod}, handler)
	return seen, err
}

func bearer(token string) metadata.MD      { return metadata.Pairs("authorization", "Bearer "+token) }
func webCookie(token string) metadata.MD   { return metadata.Pairs("cookie", cookieName+"="+token) }
func apiKey(key string) metadata.MD        { return metadata.Pairs("x-api-key", key) }
func serverToken(token string) metadata.MD { return metadata.Pairs("x-server-token", token) }

// serverClaims are the claims of a valid server token from gateway to the library, carrying alice.
func serverClaims() map[string]any {
	return map[string]any{
		"iss": "serviceAccounts/gateway",
		"aud": []string{libraryService},
		"sub": "users/alice",
		"exp": time.Now().Add(time.Minute).Unix(),
		"iat": time.Now().Unix(),
		"session": encodeSession(&authenticationpb.Session{
			Principal: &authenticationpb.Principal{Subject: "users/alice", Attributes: mustStruct(map[string]any{"shelf": "shelves/1"})},
			Grants:    []*authenticationpb.Grant{{Role: "reader", Scope: "*"}},
		}),
	}
}

// encodeSession is how a server's token carries the session: the binary message, base64url-encoded.
func encodeSession(session *authenticationpb.Session) string {
	bytes, err := proto.Marshal(session)
	if err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(bytes)
}

func mustStruct(fields map[string]any) *structpb.Struct {
	value, err := structpb.NewStruct(fields)
	if err != nil {
		panic(err)
	}
	return value
}

// sign signs claims with an HMAC secret, as the platform's signer or an issuer would.
func sign(t testing.TB, secret string, algorithm jwa.SignatureAlgorithm, claims map[string]any) string {
	t.Helper()
	token := jwt.New()
	for name, value := range claims {
		require.NoError(t, token.Set(name, value))
	}
	signed, err := jwt.Sign(token, jwt.WithKey(algorithm, []byte(secret)))
	require.NoError(t, err)
	return string(signed)
}

func tokenFor(subject string) authentication.Token {
	return authentication.Token{Subject: subject, Lifetime: time.Hour}
}

// writeRaw writes a configuration file as given.
func writeRaw(t testing.TB, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "authentication.jsonnet")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func code(err error) codes.Code { return grpcstatus.Code(err) }

func mustSymmetricKey(secret string) string {
	key, err := authentication.SymmetricKey(secret)
	if err != nil {
		panic(err)
	}
	return key
}

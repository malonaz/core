package authentication_test

// v1 and v2 side by side: where v2 is meant to decide as v1 does, the same inputs through both chains.

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	authenticationv1pb "github.com/malonaz/core/genproto/authentication/v1"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	authenticationv1 "github.com/malonaz/core/go/authentication"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

const (
	equivalenceSecret   = "equivalence-secret-0123456789abc"
	internalSecret      = "internal-secret"
	v1SessionSecret     = "v1-session-secret"
	equivalenceIssuerID = "eq"
)

// v1Chain is v1's external server chain: API key, JWT, then permissions, as onikisu's manifest orders them, with
// v1's internal service authentication in front for service accounts.
type v1Chain struct {
	interceptors []grpc.UnaryServerInterceptor
}

func newV1Chain(t *testing.T, issuer *authenticationv1pb.JwtIssuer, serviceAccounts []*authenticationv1pb.ServiceAccount, publicMethods []string) (*v1Chain, error) {
	t.Helper()
	sessionManager := authenticationv1.NewSessionManager(&authenticationv1.SessionManagerOpts{Secret: v1SessionSecret})
	issuers := []*authenticationv1pb.JwtIssuer{issuer}
	jwtOpts := &authenticationv1.JwtAuthenticationInterceptorOpts{
		Config:     writeV1Configuration(t, &authenticationv1pb.JwtConfiguration{Issuers: issuers}),
		CookieName: cookieName,
	}
	jwtInterceptor, err := authenticationv1.NewJwtAuthenticationInterceptor(context.Background(), jwtOpts, sessionManager)
	if err != nil {
		return nil, err
	}
	permissionConfiguration := &authenticationv1pb.PermissionConfiguration{
		ServiceAccounts: serviceAccounts,
		Roles:           []*authenticationv1pb.Role{{Id: "unused"}},
		PublicMethods:   publicMethods,
		JwtIssuers:      issuers,
	}
	permissionPath := writeV1Configuration(t, permissionConfiguration)
	permissionInterceptor, err := authenticationv1.NewPermissionAuthenticationInterceptor(&authenticationv1.PermissionAuthenticationInterceptorOpts{Config: permissionPath}, sessionManager)
	if err != nil {
		return nil, err
	}
	internalOpts := &authenticationv1.InternalServiceAuthenticationInterceptorOpts{
		MetadataHeader:        "x-internal-service-auth",
		InternalServiceSecret: internalSecret,
		Config:                writeV1Configuration(t, &authenticationv1pb.ServiceAccountConfiguration{ServiceAccounts: serviceAccounts}),
	}
	internalInterceptor, err := authenticationv1.NewInternalServiceAuthenticationInterceptor(internalOpts, sessionManager)
	if err != nil {
		return nil, err
	}
	return &v1Chain{interceptors: []grpc.UnaryServerInterceptor{
		sessionManager.UnaryServerLocalContextInjectorInterceptor(),
		internalInterceptor.Unary(),
		jwtInterceptor.Unary(),
		permissionInterceptor.Unary(),
	}}, nil
}

func writeV1Configuration(t *testing.T, configuration proto.Message) string {
	t.Helper()
	bytes, err := pbutil.JSONMarshal(configuration)
	require.NoError(t, err)
	return writeRaw(t, string(bytes))
}

// call runs a call through v1's chain, returning the session its handler saw.
func (c *v1Chain) call(fullMethod string, md metadata.MD, request any, ctx context.Context) (*authenticationv1pb.Session, error) {
	var session *authenticationv1pb.Session
	handler := grpc.UnaryHandler(func(ctx context.Context, _ any) (any, error) {
		var err error
		session, err = authenticationv1.GetSession(ctx)
		return nil, err
	})
	info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
	for index := len(c.interceptors) - 1; index >= 0; index-- {
		interceptor, next := c.interceptors[index], handler
		handler = func(ctx context.Context, request any) (any, error) { return interceptor(ctx, request, info, next) }
	}
	_, err := handler(metadata.NewIncomingContext(ctx, md), request)
	return session, err
}

// v1Token mints a token with v1's minter.
func v1Token(t *testing.T, issuer *authenticationv1pb.JwtIssuer, claims map[string]any) string {
	t.Helper()
	minter, err := authenticationv1.NewJwtMinter(issuer)
	require.NoError(t, err)
	token, err := minter.MintJwt(authenticationv1.MintJwtOpts{Subject: "users/alice", Lifetime: time.Hour, Claims: claims})
	require.NoError(t, err)
	return token
}

// bindV1 wraps a v1 rule text, unchanged, in the bindings that give it v1's `claims` and `subject`: how a host carries
// its v1 rules onto v2.
func bindV1(rule string) string {
	return "cel.bind(claims, principal.attributes, cel.bind(subject, principal.subject, (" + rule + ")))"
}

// equivalenceIssuers returns the same issuer in v1's and v2's shapes, with one rule on ListBooks: v1's text as v1
// read it, and the same text inside bindV1.
func equivalenceIssuers(rewrite, rule string) (*authenticationv1pb.JwtIssuer, *authenticationpb.Configuration) {
	v1Issuer := &authenticationv1pb.JwtIssuer{
		Id:                       equivalenceIssuerID,
		Issuer:                   equivalenceIssuerID,
		Audience:                 equivalenceIssuerID,
		KeySource:                &authenticationv1pb.JwtIssuer_SymmetricKey{SymmetricKey: equivalenceSecret},
		ClaimsRewriteCel:         rewrite,
		MethodToAuthorizationCel: map[string]string{listBooksMethod: rule},
	}
	v2Issuer := &authenticationpb.Issuer{
		Id:            equivalenceIssuerID,
		Issuer:        equivalenceIssuerID,
		Audience:      equivalenceIssuerID,
		Keys:          &authenticationpb.Issuer_Jwks{Jwks: authenticationtest.KeySet(mustSymmetricKey(equivalenceSecret))},
		AttributesCel: rewrite,
		GrantsCel:     `[Grant{role: "eq", scope: "*"}]`,
	}
	return v1Issuer, &authenticationpb.Configuration{
		Issuers: []*authenticationpb.Issuer{v2Issuer},
		Roles: []*authenticationpb.Role{
			{Id: "eq", Rules: []*authenticationpb.Rule{{Methods: []string{listBooksMethod}, ConditionCel: bindV1(rule)}}},
			{Id: "public", Rules: []*authenticationpb.Rule{{Methods: []string{echoMethod}}}},
		},
		PublicRoles: []string{"public"},
	}
}

func buildV2(t *testing.T, configuration *authenticationpb.Configuration) (*authentication.Interceptor, error) {
	t.Helper()
	opts := &authentication.Opts{
		Config:       writeConfiguration(t, configuration),
		ServerSecret: serverSecret,
		APIKeyHeader: "x-api-key",
		APIKeys:      "sa:key",
		CookieName:   cookieName,
	}
	return authentication.NewInterceptor(context.Background(), opts)
}

func newV2(t *testing.T, configuration *authenticationpb.Configuration) *authentication.Interceptor {
	t.Helper()
	interceptor, err := buildV2(t, configuration)
	require.NoError(t, err)
	return interceptor
}

// A v1 rule text, carried unchanged inside bindV1, evaluates exactly as v1 evaluated it: strings, numbers, lists, nested
// maps, presence, a missing key (an error, INTERNAL in both), the rn.* functions, `subject` and the request. The
// attributes it reads equal v1's claims, rewritten or not.
func TestEquivalence_Conditions(t *testing.T) {
	expressions := []string{
		`claims.role == "admin"`,
		`claims["role"] == "admin"`,
		`claims.missing == "x"`,
		`claims["missing"] == "x"`,
		`has(claims.missing)`,
		`has(claims.role)`,
		`claims.count == 3`,
		`claims.count == 3.0`,
		`claims.count > 2.5`,
		`int(claims.count) == 3`,
		`string(claims.count) == "3"`,
		`type(claims.count) == double`,
		`claims.admin == true`,
		`claims.admin && claims.count == 3`,
		`claims.null_value == null`,
		`"admin" in claims.roles`,
		`size(claims.roles) == 2`,
		`claims.roles[0] == "admin"`,
		`claims.roles[5] == "admin"`,
		`claims.roles.exists(r, r == "user")`,
		`claims.nested.organization == "organizations/1"`,
		`claims.nested.missing == "x"`,
		`"organization" in claims.nested`,
		`request.parent.startsWith(claims.nested.organization)`,
		`request.parent.startsWith(claims.nested.organization + "/")`,
		`rn.hasParent(request.parent, claims.nested.organization)`,
		`rn.ancestor(request.parent, "organizations/{organization}") == claims.nested.organization`,
		`claims.aud[0] == "eq"`,
		`claims.iat <= claims.exp`,
		`claims.sub == "users/alice"`,
		`subject == "users/alice"`,
		`rn.ancestor(subject, "users/{user}") == "users/alice"`,
		`claims.email.lowerAscii() == "alice@example.com"`,
	}
	claimSets := map[string]map[string]any{
		"admin": {
			"role": "admin", "count": 3, "admin": true, "null_value": nil, "email": "Alice@Example.com",
			"roles": []any{"admin", "user"}, "nested": map[string]any{"organization": "organizations/1"},
		},
		"user": {
			"role": "user", "count": 2.5, "admin": false, "null_value": "set", "email": "bob@example.com",
			"roles": []any{}, "nested": map[string]any{"organization": "organizations/12"},
		},
		"bare": {},
	}
	requests := map[string]*librarypb.ListBooksRequest{
		"inside":  {Parent: "organizations/1/shelves/2"},
		"sibling": {Parent: "organizations/12/shelves/2"},
		"empty":   {},
	}
	rewrites := map[string]string{
		"no rewrite": "",
		"rewrite":    `{"sub": claims.sub, "aud": claims.aud, "iat": claims.iat, "exp": claims.exp, "role": has(claims.role) ? claims.role : "none", "nested": has(claims.nested) ? claims.nested : {}, "count": has(claims.count) ? claims.count : 0}`,
	}
	outcomes := map[codes.Code]int{}
	for rewriteName, rewrite := range rewrites {
		for _, expression := range expressions {
			v1Issuer, configuration := equivalenceIssuers(rewrite, expression)
			v1, err := newV1Chain(t, v1Issuer, []*authenticationv1pb.ServiceAccount{{Id: "unused", Type: authenticationv1pb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_API_KEY}}, nil)
			require.NoError(t, err)
			v2 := newV2(t, configuration)
			for claimSetName, claims := range claimSets {
				token := v1Token(t, v1Issuer, claims)
				for requestName, request := range requests {
					name := fmt.Sprintf("%s/%s/%s/%s", rewriteName, expression, claimSetName, requestName)
					v1Session, v1Err := v1.call(listBooksMethod, bearer(token), request, context.Background())
					v2Seen, v2Err := unary(t, v2, listBooksMethod, bearer(token), request)
					require.Equal(t, code(v1Err), code(v2Err), "%s: v1 %v, v2 %v", name, v1Err, v2Err)
					outcomes[code(v2Err)]++
					if v1Err == nil {
						require.True(t, proto.Equal(v1Session.GetJwtIdentity().GetClaims(), v2Seen.session.GetPrincipal().GetAttributes()), name)
					}
				}
			}
		}
	}
	// The corpus exercises allowing, denying and failing alike.
	require.Positive(t, outcomes[codes.OK])
	require.Positive(t, outcomes[codes.PermissionDenied])
	require.Positive(t, outcomes[codes.Internal])
	t.Logf("outcomes: %v", outcomes)
}

// A rule v1 refused at startup, v2 refuses too: not a bool, not compiling, reading a field the request lacks.
func TestEquivalence_RulesRefusedAtStartup(t *testing.T) {
	for _, rule := range []string{`claims.admin`, `claims.role ==`, `request.missing == ""`} {
		v1Issuer, configuration := equivalenceIssuers("", rule)
		_, v1Err := newV1Chain(t, v1Issuer, []*authenticationv1pb.ServiceAccount{{Id: "unused", Type: authenticationv1pb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_API_KEY}}, nil)
		require.Error(t, v1Err, rule)
		_, v2Err := buildV2(t, configuration)
		require.Error(t, v2Err, rule)
	}
}

// The one difference: v1's `subject` was the rewritten claims' sub, empty when a rewrite drops it; v2's is the
// issuer's subject, the token's sub by default.
func TestEquivalence_SubjectWhenTheRewriteDropsSub(t *testing.T) {
	v1Issuer, configuration := equivalenceIssuers(`{"role": "admin"}`, `subject == ""`)
	v1, err := newV1Chain(t, v1Issuer, []*authenticationv1pb.ServiceAccount{{Id: "unused", Type: authenticationv1pb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_API_KEY}}, nil)
	require.NoError(t, err)
	token := v1Token(t, v1Issuer, nil)
	request := &librarypb.ListBooksRequest{}
	_, err = v1.call(listBooksMethod, bearer(token), request, context.Background())
	require.NoError(t, err)
	_, err = unary(t, newV2(t, configuration), listBooksMethod, bearer(token), request)
	require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)
}

// Method patterns match as v1 matched a service account's permissions: every registered method, through both
// chains, for a service account holding one pattern; and the same patterns refuse to start.
func TestEquivalence_MethodPatterns(t *testing.T) {
	var methods []string
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		for i := range file.Services().Len() {
			service := file.Services().Get(i)
			for j := range service.Methods().Len() {
				methods = append(methods, "/"+string(service.FullName())+"/"+string(service.Methods().Get(j).Name()))
			}
		}
		return true
	})
	sort.Strings(methods)
	require.NotEmpty(t, methods)
	patterns := []string{
		"*",
		"/*",
		getBookMethod,
		"/malonaz.test.library.library_service.v1.LibraryService/*",
		"/malonaz.test.*",
		"*/Get*",
		"*Book*",
		"/malonaz.test.mock.v1.MockService/Echo*",
		"/grpc.*/*Info",
		"/malonaz.*.v1.*Service/List*",
	}
	for _, pattern := range patterns {
		t.Run(pattern, func(t *testing.T) {
			v1Issuer, _ := equivalenceIssuers("", "true")
			v1, err := newV1Chain(t, v1Issuer, []*authenticationv1pb.ServiceAccount{{
				Id: "sa", Type: authenticationv1pb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_INTERNAL_SERVICE, Permissions: []string{pattern},
			}}, nil)
			require.NoError(t, err)
			v2 := newV2(t, &authenticationpb.Configuration{
				ServiceAccounts: []*authenticationpb.ServiceAccount{{Id: "sa", Grants: []*authenticationpb.Grant{{Role: "pattern", Scope: "*"}}}},
				Roles:           []*authenticationpb.Role{{Id: "pattern", Rules: []*authenticationpb.Rule{{Methods: []string{pattern}}}}},
			})
			allowed := 0
			for _, method := range methods {
				_, v1Err := v1.call(method, metadata.Pairs("x-internal-service-auth", "sa/"+internalSecret), nil, context.Background())
				_, v2Err := unary(t, v2, method, apiKey("key"), nil)
				require.Equal(t, code(v1Err), code(v2Err), "%s: v1 %v, v2 %v", method, v1Err, v2Err)
				if v2Err == nil {
					allowed++
				}
			}
			t.Logf("%s allows %d of %d methods", pattern, allowed, len(methods))
		})
	}
	for _, pattern := range []string{"/nothing.*", "/malonaz.test.mock.v1.MockService/Missing"} {
		t.Run("refused: "+pattern, func(t *testing.T) {
			v1Issuer, _ := equivalenceIssuers("", "true")
			_, v1Err := newV1Chain(t, v1Issuer, []*authenticationv1pb.ServiceAccount{{
				Id: "sa", Type: authenticationv1pb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_INTERNAL_SERVICE, Permissions: []string{pattern},
			}}, nil)
			require.Error(t, v1Err)
			f := newFixture(t)
			f.configuration.Roles[1].Rules[0].Methods = []string{pattern}
			_, v2Err := f.build(t)
			require.Error(t, v2Err)
		})
	}
}

// The client is read from the same headers as v1's session metadata, the IP address being X-Forwarded-For's leftmost
// entry.
func TestEquivalence_Client(t *testing.T) {
	v1Issuer, configuration := equivalenceIssuers("", "true")
	v1, err := newV1Chain(t, v1Issuer, []*authenticationv1pb.ServiceAccount{{Id: "unused", Type: authenticationv1pb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_API_KEY}}, []string{echoMethod})
	require.NoError(t, err)
	v2 := newV2(t, configuration)
	peerContext := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 5555}})
	tests := map[string]metadata.MD{
		"no headers":                   {},
		"one forwarded address":        metadata.Pairs("x-forwarded-for", "203.0.113.7"),
		"a chain":                      metadata.Pairs("x-forwarded-for", "203.0.113.7, 10.0.0.1, 10.0.0.2"),
		"spaces":                       metadata.Pairs("x-forwarded-for", "  203.0.113.7 ,10.0.0.1"),
		"two headers":                  metadata.Pairs("x-forwarded-for", "203.0.113.7,10.0.0.1", "x-forwarded-for", "10.0.0.2"),
		"gateway user agent":           metadata.Pairs("grpcgateway-user-agent", "gateway", "x-forwarded-user-agent", "forwarded", "user-agent", "grpc"),
		"forwarded user agent":         metadata.Pairs("x-forwarded-user-agent", "forwarded", "user-agent", "grpc"),
		"user agent":                   metadata.Pairs("user-agent", "grpc"),
		"platform, version, time zone": metadata.Pairs("x-client-platform", "ios", "x-client-version", "3.12.1", "x-client-timezone", "America/New_York"),
		"malformed version":            metadata.Pairs("x-client-version", "3.x.1"),
		"short version":                metadata.Pairs("x-client-version", "3.1"),
	}
	for name, md := range tests {
		for _, ctx := range []context.Context{context.Background(), peerContext} {
			v1Session, v1Err := v1.call(echoMethod, md, nil, ctx)
			var v2Session *authenticationpb.Session
			handler := func(ctx context.Context, _ any) (any, error) {
				v2Session, _ = authentication.GetSession(ctx)
				return nil, nil
			}
			_, v2Err := v2.Unary()(metadata.NewIncomingContext(ctx, md), nil, &grpc.UnaryServerInfo{FullMethod: echoMethod}, handler)
			require.Equal(t, code(v1Err), code(v2Err), "%s: v1 %v, v2 %v", name, v1Err, v2Err)
			if v1Err != nil {
				continue
			}
			v1Metadata := v1Session.GetMetadata()
			v2Client := v2Session.GetClient()
			require.Equal(t, v1Metadata.GetIpAddress(), v2Client.GetIpAddress(), name)
			require.Equal(t, v1Metadata.GetUserAgent(), v2Client.GetUserAgent(), name)
			require.Equal(t, v1Metadata.GetClientPlatform(), v2Client.GetPlatform(), name)
			require.Equal(t, v1Metadata.GetClientTimezone(), v2Client.GetTimeZone(), name)
			require.Equal(t, v1Metadata.GetClientVersion().GetMajor(), v2Client.GetVersion().GetMajor(), name)
			require.Equal(t, v1Metadata.GetClientVersion().GetMinor(), v2Client.GetVersion().GetMinor(), name)
			require.Equal(t, v1Metadata.GetClientVersion().GetPatch(), v2Client.GetVersion().GetPatch(), name)
		}
	}
}

// v1 accepted a token without exp, valid forever; v2 refuses it (decision 2).
func TestEquivalence_TokenWithoutExpiry(t *testing.T) {
	v1Issuer, configuration := equivalenceIssuers("", "true")
	v1, err := newV1Chain(t, v1Issuer, []*authenticationv1pb.ServiceAccount{{Id: "unused", Type: authenticationv1pb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_API_KEY}}, nil)
	require.NoError(t, err)
	v2 := newV2(t, configuration)
	token := jwt.New()
	for name, value := range map[string]any{"iss": equivalenceIssuerID, "aud": equivalenceIssuerID, "sub": "users/alice"} {
		require.NoError(t, token.Set(name, value))
	}
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.HS256, []byte(equivalenceSecret)))
	require.NoError(t, err)
	request := &librarypb.ListBooksRequest{}
	_, err = v1.call(listBooksMethod, bearer(string(signed)), request, context.Background())
	require.NoError(t, err)
	_, err = unary(t, v2, listBooksMethod, bearer(string(signed)), request)
	require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
}

// Tokens keep their shape: v2's signer writes the claims v1's minter wrote, and each side verifies the other's.
func TestEquivalence_IssuedTokens(t *testing.T) {
	v1Issuer, configuration := equivalenceIssuers("", "true")
	v1, err := newV1Chain(t, v1Issuer, []*authenticationv1pb.ServiceAccount{{Id: "unused", Type: authenticationv1pb.ServiceAccountType_SERVICE_ACCOUNT_TYPE_API_KEY}}, nil)
	require.NoError(t, err)
	v2 := newV2(t, configuration)
	signer, err := authentication.NewSigner(configuration.Issuers[0], mustSymmetricKey(equivalenceSecret))
	require.NoError(t, err)
	claims := map[string]any{"scope": "view", "onikisu.com/Roles": []any{"support"}}
	v2Token, expireTime, err := signer.Sign(authentication.Token{Subject: "users/alice", Lifetime: time.Hour, Claims: claims})
	require.NoError(t, err)
	v1Signed := v1Token(t, v1Issuer, claims)

	v1Parsed, err := jwt.ParseInsecure([]byte(v1Signed))
	require.NoError(t, err)
	v2Parsed, err := jwt.ParseInsecure([]byte(v2Token))
	require.NoError(t, err)
	v1Claims, err := v1Parsed.AsMap(context.Background())
	require.NoError(t, err)
	v2Claims, err := v2Parsed.AsMap(context.Background())
	require.NoError(t, err)
	for _, timed := range []string{"iat", "nbf", "exp"} {
		require.Contains(t, v1Claims, timed)
		require.Contains(t, v2Claims, timed)
		delete(v1Claims, timed)
		delete(v2Claims, timed)
	}
	require.Equal(t, v1Claims, v2Claims)
	require.Equal(t, expireTime.Unix(), v2Parsed.Expiration().Unix())
	message, err := jws.Parse([]byte(v2Token))
	require.NoError(t, err)
	require.Empty(t, message.Signatures()[0].ProtectedHeaders().KeyID(), "today's tokens name no key")

	request := &librarypb.ListBooksRequest{}
	_, err = v1.call(listBooksMethod, bearer(v2Token), request, context.Background())
	require.NoError(t, err, "v1 verifies v2's token")
	_, err = unary(t, v2, listBooksMethod, bearer(v1Signed), request)
	require.NoError(t, err, "v2 verifies v1's token")
}

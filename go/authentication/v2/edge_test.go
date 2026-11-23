package authentication_test

// One test per row of the edge table (design, section 6). Nothing in it allows a call on an error.

import (
	"bytes"
	"context"
	"encoding/base64"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	grpcpb "github.com/malonaz/core/genproto/grpc/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	"github.com/malonaz/core/go/grpc/status"
)

var getBookRequest = &librarypb.GetBookRequest{Name: "organizations/1/shelves/1/books/1"}

func issuerClaims() map[string]any {
	return map[string]any{
		"iss": "test",
		"aud": []string{"test"},
		"sub": "users/alice",
		"exp": time.Now().Add(time.Hour).Unix(),
	}
}

func withClaims(claims map[string]any, overrides map[string]any) map[string]any {
	for name, value := range overrides {
		if value == nil {
			delete(claims, name)
			continue
		}
		claims[name] = value
	}
	return claims
}

// Row 1: token malformed, issuer and audience unknown, signature bad, expired, or signed with an algorithm its key
// does not name: UNAUTHENTICATED, on a public method too.
func TestEdge_InvalidTokenIsUnauthenticated(t *testing.T) {
	f := newFixture(t)
	_, otherSigner := authenticationtest.NewIssuer(t, "other", `[]`)
	unknownIssuer, _, err := otherSigner.Sign(tokenFor("users/alice"))
	require.NoError(t, err)
	expired, _, err := f.signer.Sign(authentication.Token{Subject: "users/alice", Lifetime: -time.Minute})
	require.NoError(t, err)
	unsignedPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"test","aud":"test","sub":"users/alice","exp":4102444800}`))
	tokens := map[string]string{
		"malformed":                       "not-a-jwt",
		"unknown issuer":                  unknownIssuer,
		"unknown audience":                sign(t, issuerSecret, jwa.HS256, withClaims(issuerClaims(), map[string]any{"aud": []string{"other"}})),
		"bad signature":                   sign(t, "another-secret-0123456789abcdef0", jwa.HS256, issuerClaims()),
		"expired":                         expired,
		"algorithm the key does not name": sign(t, issuerSecret, jwa.HS384, issuerClaims()),
		"no signature":                    base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." + unsignedPayload + ".",
	}
	interceptor := f.interceptor(t)
	for name, token := range tokens {
		t.Run(name, func(t *testing.T) {
			for _, method := range []string{echoMethod, getBookMethod} {
				_, err := unary(t, interceptor, method, bearer(token), getBookRequest)
				require.True(t, status.HasCode(err, codes.Unauthenticated), "%s: %v", method, err)
			}
		})
	}
	t.Run("control: the same claims, validly signed, authenticate", func(t *testing.T) {
		seen, err := unary(t, interceptor, getBookMethod, bearer(sign(t, issuerSecret, jwa.HS256, issuerClaims())), getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "users/alice", seen.session.GetPrincipal().GetSubject())
	})
}

// Row 2: a token without exp, which would be valid forever, is UNAUTHENTICATED.
func TestEdge_TokenWithoutExpiryIsUnauthenticated(t *testing.T) {
	f := newFixture(t)
	token := sign(t, issuerSecret, jwa.HS256, withClaims(issuerClaims(), map[string]any{"exp": nil}))
	_, err := unary(t, f.interceptor(t), getBookMethod, bearer(token), getBookRequest)
	require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
}

func gatewayCookie(t *testing.T, token string) metadata.MD {
	t.Helper()
	bytes, err := proto.Marshal(&grpcpb.HttpCookie{Name: cookieName, Value: token})
	require.NoError(t, err)
	return metadata.Pairs("set-cookie-bin", string(bytes))
}

// Row 3: an invalid token in the cookie is ignored, as if absent: public methods run anonymously, others need a
// credential. Through grpc-web's cookie header and the gateway's cookies alike.
func TestEdge_InvalidCookieIsIgnored(t *testing.T) {
	f := newFixture(t)
	f.configuration.Issuers = append(f.configuration.Issuers, &authenticationpb.Issuer{
		Id: "unmappable", Issuer: "unmappable", Audience: "unmappable",
		Keys:       f.issuer.Keys,
		SubjectCel: `claims.missing`,
		GrantsCel:  `[]`,
	})
	interceptor := f.interceptor(t)
	tokens := map[string]string{
		"bad signature":          sign(t, "another-secret-0123456789abcdef0", jwa.HS256, issuerClaims()),
		"claims that do not map": sign(t, issuerSecret, jwa.HS256, withClaims(issuerClaims(), map[string]any{"iss": "unmappable", "aud": []string{"unmappable"}})),
	}
	for name, token := range tokens {
		for cookieKind, md := range map[string]metadata.MD{"grpc-web": webCookie(token), "gateway": gatewayCookie(t, token)} {
			t.Run(cookieKind+": "+name, func(t *testing.T) {
				seen, err := unary(t, interceptor, echoMethod, md, nil)
				require.NoError(t, err)
				require.Nil(t, seen.session.GetPrincipal())
				_, err = unary(t, interceptor, getBookMethod, md, getBookRequest)
				require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
			})
		}
	}
	t.Run("a valid cookie authenticates", func(t *testing.T) {
		for _, md := range []metadata.MD{webCookie(f.token(t, nil)), gatewayCookie(t, f.token(t, nil))} {
			seen, err := unary(t, interceptor, getBookMethod, md, getBookRequest)
			require.NoError(t, err)
			require.Equal(t, "users/alice", seen.session.GetPrincipal().GetSubject())
		}
	})
	t.Run("a cookie whose grant names an unknown role is a configuration fault, not ignored", func(t *testing.T) {
		f := newFixture(t)
		f.issuer.GrantsCel = `[Grant{role: "ghost", scope: "*"}]`
		_, err := unary(t, f.interceptor(t), echoMethod, webCookie(f.token(t, nil)), nil)
		require.True(t, status.HasCode(err, codes.Internal), "%v", err)
	})
}

// Row 4 and row 5, and every other combination of credentials: TestEdge_CredentialCombinations (credentials_test.go).

// Row 6: a mapping expression fails (a claim is missing), or yields an empty subject: UNAUTHENTICATED.
func TestEdge_MappingFailureIsUnauthenticated(t *testing.T) {
	tests := map[string]func(*authenticationpb.Issuer){
		"subject reads a missing claim":     func(i *authenticationpb.Issuer) { i.SubjectCel = `claims.missing` },
		"subject is empty":                  func(i *authenticationpb.Issuer) { i.SubjectCel = `""` },
		"default subject, token has no sub": func(i *authenticationpb.Issuer) { i.SubjectCel = `` },
		"attributes are not a map":          func(i *authenticationpb.Issuer) { i.AttributesCel = `claims.sub` },
		"actor reads a missing claim":       func(i *authenticationpb.Issuer) { i.ActorCel = `claims.missing` },
		"grants read a missing claim":       func(i *authenticationpb.Issuer) { i.GrantsCel = `[Grant{role: claims.missing, scope: "*"}]` },
		"grants are not a list":             func(i *authenticationpb.Issuer) { i.GrantsCel = `claims.sub` },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			mutate(f.issuer)
			claims := issuerClaims()
			if name == "default subject, token has no sub" {
				delete(claims, "sub")
			}
			_, err := unary(t, f.interceptor(t), getBookMethod, bearer(sign(t, issuerSecret, jwa.HS256, claims)), getBookRequest)
			require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
		})
	}
	t.Run("an act claim without sub", func(t *testing.T) {
		f := newFixture(t)
		claims := withClaims(issuerClaims(), map[string]any{"act": map[string]any{"name": "jdoe"}})
		_, err := unary(t, f.interceptor(t), getBookMethod, bearer(sign(t, issuerSecret, jwa.HS256, claims)), getBookRequest)
		require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	})
}

// Row 7: a grant names a role the configuration lacks: INTERNAL, a configuration fault.
func TestEdge_GrantOfUnknownRoleIsInternal(t *testing.T) {
	t.Run("from a token", func(t *testing.T) {
		f := newFixture(t)
		f.issuer.GrantsCel = `[Grant{role: "reader", scope: "*"}, Grant{role: "ghost", scope: "*"}]`
		_, err := unary(t, f.interceptor(t), getBookMethod, bearer(f.token(t, nil)), getBookRequest)
		require.True(t, status.HasCode(err, codes.Internal), "%v", err)
	})
	t.Run("from an authenticator", func(t *testing.T) {
		f := newFixture(t)
		authenticator := &headerAuthenticator{session: &authenticationpb.Session{
			Principal: &authenticationpb.Principal{Subject: "custom/1"},
			Grants:    []*authenticationpb.Grant{{Role: "ghost", Scope: "*"}},
		}}
		_, err := unary(t, f.interceptor(t, authenticator), getBookMethod, metadata.Pairs(customHeader, "1"), getBookRequest)
		require.True(t, status.HasCode(err, codes.Internal), "%v", err)
	})
}

// Row 8: a server's token with a bad signature, past its expiry, or addressed to another service: UNAUTHENTICATED.
func TestEdge_InvalidServerTokenIsUnauthenticated(t *testing.T) {
	f := newFixture(t)
	interceptor := f.interceptor(t)
	tests := map[string]string{
		"bad signature":                            sign(t, "wrong-secret-0123456789abcdef0123", jwa.HS256, serverClaims()),
		"expired":                                  sign(t, serverSecret, jwa.HS256, withClaims(serverClaims(), map[string]any{"exp": time.Now().Add(-time.Second).Unix()})),
		"addressed to another service":             sign(t, serverSecret, jwa.HS256, withClaims(serverClaims(), map[string]any{"aud": []string{mockService}})),
		"no exp":                                   sign(t, serverSecret, jwa.HS256, withClaims(serverClaims(), map[string]any{"exp": nil})),
		"unknown service account":                  sign(t, serverSecret, jwa.HS256, withClaims(serverClaims(), map[string]any{"iss": "serviceAccounts/ghost"})),
		"algorithm the key does not name":          sign(t, serverSecret, jwa.HS512, serverClaims()),
		"platform key for an account with its own": sign(t, serverSecret, jwa.HS256, withClaims(serverClaims(), map[string]any{"iss": "serviceAccounts/outsider"})),
		"malformed":                                "not-a-jwt",
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := unary(t, interceptor, getBookMethod, serverToken(token), getBookRequest)
			require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
		})
	}
	t.Run("control: a valid token authenticates", func(t *testing.T) {
		seen, err := unary(t, interceptor, getBookMethod, serverToken(sign(t, serverSecret, jwa.HS256, serverClaims())), getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "users/alice", seen.session.GetPrincipal().GetSubject())
		require.Equal(t, "serviceAccounts/gateway", seen.session.GetCaller().GetSubject())
	})
	t.Run("control: an account with its own keys authenticates with them", func(t *testing.T) {
		token := sign(t, outsiderSecret, jwa.HS256, withClaims(serverClaims(), map[string]any{"iss": "serviceAccounts/outsider"}))
		seen, err := unary(t, interceptor, getBookMethod, serverToken(token), getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "serviceAccounts/outsider", seen.session.GetCaller().GetSubject())
	})
}

// Row 9: a scoped grant, and the call names no resource: that grant does not allow. No call's resources are derived
// yet, so a grant over anything but "*" allows nothing, even over the resource the request names; and a service
// account configured with one refuses to start.
func TestEdge_ScopedGrantDoesNotAllow(t *testing.T) {
	f := newFixture(t)
	f.issuer.GrantsCel = `[Grant{role: "reader", scope: "organizations/1"}]`
	interceptor := f.interceptor(t)
	_, err := unary(t, interceptor, getBookMethod, bearer(f.token(t, nil)), getBookRequest)
	require.True(t, status.HasCode(err, codes.PermissionDenied), "%v", err)

	t.Run("beside a grant over *", func(t *testing.T) {
		f := newFixture(t)
		f.issuer.GrantsCel = `[Grant{role: "reader", scope: "organizations/1"}, Grant{role: "reader", scope: "*"}]`
		_, err := unary(t, f.interceptor(t), getBookMethod, bearer(f.token(t, nil)), getBookRequest)
		require.NoError(t, err)
	})
	t.Run("a service account's scoped grant refuses to start", func(t *testing.T) {
		f := newFixture(t)
		f.configuration.ServiceAccounts[0].Grants[0].Scope = "organizations/1"
		_, err := f.build(t)
		require.ErrorContains(t, err, `scope "organizations/1"`)
	})
}

// Row 10: no grant allows: PERMISSION_DENIED; UNAUTHENTICATED for an anonymous caller.
func TestEdge_NoGrantAllows(t *testing.T) {
	f := newFixture(t)
	interceptor := f.interceptor(t)
	createBookMethod := "/malonaz.test.library.library_service.v1.LibraryService/CreateBook"
	tests := map[string]struct {
		md   metadata.MD
		code codes.Code
	}{
		"anonymous": {md: metadata.MD{}, code: codes.Unauthenticated},
		"token":     {md: bearer(f.token(t, nil)), code: codes.PermissionDenied},
		"a service as itself, carrier held": {md: serverToken(sign(t, serverSecret, jwa.HS256, withClaims(serverClaims(), map[string]any{
			"iss": "serviceAccounts/worker", "sub": "serviceAccounts/worker",
			"session": encodeSession(&authenticationpb.Session{Principal: &authenticationpb.Principal{Subject: "serviceAccounts/worker"}}),
		}))), code: codes.PermissionDenied},
		"an anonymous call carried by a caller without carrier": {md: serverToken(sign(t, serverSecret, jwa.HS256, withClaims(serverClaims(), map[string]any{
			"iss": "serviceAccounts/onikiri", "sub": nil, "session": encodeSession(&authenticationpb.Session{}),
		}))), code: codes.PermissionDenied},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := unary(t, interceptor, createBookMethod, test.md, &librarypb.CreateBookRequest{})
			require.True(t, status.HasCode(err, test.code), "%v", err)
		})
	}
	t.Run("public methods need no credential", func(t *testing.T) {
		seen, err := unary(t, interceptor, echoMethod, metadata.MD{}, nil)
		require.NoError(t, err)
		require.Nil(t, seen.session.GetPrincipal())
	})
}

// Row 11: a condition fails to evaluate: that rule does not allow; if nothing else does, INTERNAL.
func TestEdge_ConditionFailingToEvaluate(t *testing.T) {
	newBrokenFixture := func(t *testing.T, grants string) *fixture {
		f := newFixture(t)
		f.configuration.Roles = append(f.configuration.Roles,
			&authenticationpb.Role{Id: "broken", Rules: []*authenticationpb.Rule{{Methods: []string{getBookMethod}, ConditionCel: `principal.attributes.missing == "x"`}}},
			&authenticationpb.Role{Id: "never", Rules: []*authenticationpb.Rule{{Methods: []string{getBookMethod}, ConditionCel: `false`}}},
		)
		f.issuer.GrantsCel = grants
		return f
	}
	tests := map[string]struct {
		grants string
		code   codes.Code
	}{
		"nothing else allows":  {grants: `[Grant{role: "broken", scope: "*"}, Grant{role: "never", scope: "*"}]`, code: codes.Internal},
		"another grant allows": {grants: `[Grant{role: "broken", scope: "*"}, Grant{role: "reader", scope: "*"}]`, code: codes.OK},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			f := newBrokenFixture(t, test.grants)
			_, err := unary(t, f.interceptor(t), getBookMethod, bearer(f.token(t, nil)), getBookRequest)
			require.Equal(t, test.code, code(err), "%v", err)
		})
	}
	t.Run("an anonymous caller is told to authenticate", func(t *testing.T) {
		f := newBrokenFixture(t, `[]`)
		f.configuration.PublicRoles = append(f.configuration.PublicRoles, "broken")
		_, err := unary(t, f.interceptor(t), getBookMethod, metadata.MD{}, getBookRequest)
		require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
	})
}

// Row 12: a condition reads request on a bidirectional stream: the server refuses to start.
func TestEdge_ConditionReadingRequestOnBidirectionalStreamRefusesToStart(t *testing.T) {
	f := newFixture(t)
	f.configuration.Roles = append(f.configuration.Roles, &authenticationpb.Role{
		Id: "reflector", Rules: []*authenticationpb.Rule{{Methods: []string{reflectionMethod}, ConditionCel: `request.host == "x"`}},
	})
	_, err := f.build(t)
	require.ErrorContains(t, err, "bidirectional stream")

	t.Run("a condition not reading request starts", func(t *testing.T) {
		f.configuration.Roles[len(f.configuration.Roles)-1].Rules[0].ConditionCel = `principal.subject != ""`
		_, err := f.build(t)
		require.NoError(t, err)
	})
	t.Run("a condition reading request on a server stream starts", func(t *testing.T) {
		f.configuration.Roles[len(f.configuration.Roles)-1].Rules[0] = &authenticationpb.Rule{Methods: []string{echoStreamMethod}, ConditionCel: `request.message == "x"`}
		_, err := f.build(t)
		require.NoError(t, err)
	})
}

// Row 13: unknown method pattern, unknown role, include cycle, condition not a bool, duplicate id: the server
// refuses to start. So does every other fault the configuration can hold.
func TestEdge_FaultyConfigurationRefusesToStart(t *testing.T) {
	tests := map[string]struct {
		mutate func(*fixture)
		err    string
	}{
		"unknown method": {func(f *fixture) {
			f.configuration.Roles[1].Rules[0].Methods = []string{"/malonaz.test.mock.v1.MockService/Missing"}
		}, "is not registered"},
		"pattern matching no method": {func(f *fixture) {
			f.configuration.Roles[1].Rules[0].Methods = []string{"/nothing.*"}
		}, "matches no registered method"},
		"unknown included role": {func(f *fixture) {
			f.configuration.Roles[1].Includes = []string{"ghost"}
		}, `unknown role "ghost"`},
		"unknown role granted to a service account": {func(f *fixture) {
			f.configuration.ServiceAccounts[0].Grants[0].Role = "ghost"
		}, `unknown role "ghost"`},
		"unknown public role": {func(f *fixture) {
			f.configuration.PublicRoles = []string{"ghost"}
		}, `public role "ghost"`},
		"include cycle": {func(f *fixture) {
			f.configuration.Roles[1].Includes = []string{"lister"}
			f.configuration.Roles[2].Includes = []string{"reader"}
		}, "includes itself"},
		"condition not compiling": {func(f *fixture) {
			f.configuration.Roles[1].Rules[0].ConditionCel = `principal.subject ==`
		}, "Syntax error"},
		"condition not a bool": {func(f *fixture) {
			f.configuration.Roles[1].Rules[0].ConditionCel = `principal.subject`
		}, "must evaluate to a bool"},
		"condition reading an unknown request field": {func(f *fixture) {
			f.configuration.Roles[2].Rules[0].ConditionCel = `request.missing == ""`
		}, "undefined field"},
		"condition reading resources, not declared yet": {func(f *fixture) {
			f.configuration.Roles[1].Rules[0].ConditionCel = `size(resources) > 0`
		}, "resources"},
		"duplicate role id": {func(f *fixture) {
			f.configuration.Roles = append(f.configuration.Roles, &authenticationpb.Role{Id: "reader"})
		}, "unique_role_ids"},
		"grants_cel not a list": {func(f *fixture) {
			f.issuer.GrantsCel = `Grant{role: "reader", scope: "*"}`
		}, "must evaluate to a list"},
		"grants_cel naming a field Grant lacks": {func(f *fixture) {
			f.issuer.GrantsCel = `[Grant{rol: "reader", scope: "*"}]`
		}, "rol"},
		"inline key naming no algorithm": {func(f *fixture) {
			f.issuer.Keys = &authenticationpb.Issuer_Jwks{Jwks: `{"keys": [{"kty": "oct", "k": "c2VjcmV0"}]}`}
		}, "names no algorithm"},
		"key set URL not answering": {func(f *fixture) {
			f.issuer.Keys = &authenticationpb.Issuer_JwksUri{JwksUri: "http://127.0.0.1:1/jwks"}
		}, "fetching key set"},
		"duplicate api key": {func(f *fixture) {
			f.opts.APIKeys = "onikiri:key,worker:key"
		}, "duplicate api key"},
		"malformed api key pair": {func(f *fixture) {
			f.opts.APIKeys = "onikiri"
		}, "invalid api key pair"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			test.mutate(f)
			_, err := f.build(t)
			require.ErrorContains(t, err, test.err)
		})
	}
	t.Run("an api key of an account not configured is skipped: the server starts and the key proves nothing", func(t *testing.T) {
		f := newFixture(t)
		f.opts.APIKeys = "ghost:revoked-key,onikiri:key-1"
		logs := &bytes.Buffer{}
		defaultLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
		interceptor, err := f.build(t)
		slog.SetDefault(defaultLogger)
		require.NoError(t, err)
		require.Equal(t, 1, strings.Count(logs.String(), "level=WARN"), logs.String())
		require.Contains(t, logs.String(), "service_account_id=ghost")
		require.NotContains(t, logs.String(), "revoked-key")
		_, err = unary(t, interceptor, getBookMethod, apiKey("revoked-key"), getBookRequest)
		require.True(t, status.HasCode(err, codes.Unauthenticated), "%v", err)
		seen, err := unary(t, interceptor, getBookMethod, apiKey("key-1"), getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "serviceAccounts/onikiri", seen.session.GetPrincipal().GetSubject())
	})
	t.Run("control: the fixture starts", func(t *testing.T) {
		_, err := newFixture(t).build(t)
		require.NoError(t, err)
	})
	t.Run("an unknown field in the file", func(t *testing.T) {
		f := newFixture(t)
		opts := *f.opts
		opts.Config = writeRaw(t, `{"roles": [], "unknown": 1}`)
		_, err := authentication.NewInterceptor(context.Background(), &opts)
		require.ErrorContains(t, err, "unknown")
	})
}

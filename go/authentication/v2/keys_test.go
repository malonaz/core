package authentication_test

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/authenticationtest"
	"github.com/malonaz/core/go/grpc/status"
)

// An inline key set verifies today's tokens, which name no key id, with any of its keys; it refuses a key it lacks
// and an algorithm its keys do not name.
func TestInlineKeySet(t *testing.T) {
	const firstSecret, secondSecret = "first-secret-0123456789abcdef012", "second-secret-0123456789abcdef01"
	f := newFixture(t)
	f.issuer.Keys = &authenticationpb.Issuer_Jwks{Jwks: authenticationtest.KeySet(
		mustSymmetricKey(firstSecret),
		mustSymmetricKey(secondSecret),
	)}
	interceptor := f.interceptor(t)
	for name, test := range map[string]struct {
		secret    string
		algorithm jwa.SignatureAlgorithm
		code      codes.Code
	}{
		"first key":                      {secret: firstSecret, algorithm: jwa.HS256, code: codes.OK},
		"second key":                     {secret: secondSecret, algorithm: jwa.HS256, code: codes.OK},
		"a key the set lacks":            {secret: "third-secret-0123456789abcdef0123", algorithm: jwa.HS256, code: codes.Unauthenticated},
		"a known key, another algorithm": {secret: firstSecret, algorithm: jwa.HS512, code: codes.Unauthenticated},
	} {
		t.Run(name, func(t *testing.T) {
			token := sign(t, test.secret, test.algorithm, issuerClaims())
			_, err := unary(t, interceptor, getBookMethod, bearer(token), getBookRequest)
			require.Equal(t, test.code, code(err), "%v", err)
		})
	}
}

// A secret wraps as a key in jsonnet with std.base64 (padded, standard alphabet), as onikisu's configuration does with
// its existing secrets, and verifies the tokens signed with that secret.
func TestJsonnetConfiguration(t *testing.T) {
	const secret = "a+secret/with?symbols>0123456789"
	t.Setenv("TEST_SYMMETRIC_KEY", secret)
	path := writeRaw(t, `
local secret = std.native('env')('TEST_SYMMETRIC_KEY');
{
  issuers: [{
    id: 'magic',
    issuer: 'onikisu-magic',
    audience: 'onikisu-magic',
    jwks: std.manifestJsonMinified({ keys: [{ kty: 'oct', alg: 'HS256', k: std.base64(secret) }] }),
    grants_cel: '[Grant{role: "reader", scope: "*"}]',
  }],
  roles: [{ id: 'reader', rules: [{ methods: ['`+getBookMethod+`'] }] }],
}`)
	configuration, err := authentication.LoadConfiguration(path)
	require.NoError(t, err)
	require.Contains(t, configuration.GetIssuers()[0].GetJwks(), base64.StdEncoding.EncodeToString([]byte(secret)))
	interceptor, err := authentication.NewInterceptor(context.Background(), &authentication.Opts{Config: path, ServerSecret: serverSecret, APIKeyHeader: "x-api-key"})
	require.NoError(t, err)
	claims := map[string]any{"iss": "onikisu-magic", "aud": "onikisu-magic", "sub": "users/alice", "exp": time.Now().Add(time.Hour).Unix()}
	_, err = unary(t, interceptor, getBookMethod, bearer(sign(t, secret, jwa.HS256, claims)), getBookRequest)
	require.NoError(t, err)
}

// A key set fetched from a URL, as Google's is, verifies tokens naming one of its keys.
func TestRemoteKeySet(t *testing.T) {
	jwks := authenticationtest.KeySet(`{"kty":"oct","alg":"HS256","kid":"k1","k":"` + base64.RawURLEncoding.EncodeToString([]byte(issuerSecret)) + `"}`)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Write([]byte(jwks))
	}))
	t.Cleanup(server.Close)
	f := newFixture(t)
	f.issuer.Keys = &authenticationpb.Issuer_JwksUri{JwksUri: server.URL}
	interceptor := f.interceptor(t)
	signer, err := authentication.NewSigner(f.issuer, `{"kty":"oct","alg":"HS256","kid":"k1","k":"`+base64.RawURLEncoding.EncodeToString([]byte(issuerSecret))+`"}`)
	require.NoError(t, err)
	token, _, err := signer.Sign(authentication.Token{Subject: "users/alice", Lifetime: time.Hour})
	require.NoError(t, err)
	_, err = unary(t, interceptor, getBookMethod, bearer(token), getBookRequest)
	require.NoError(t, err)
	_, err = unary(t, interceptor, getBookMethod, bearer(f.token(t, nil)), getBookRequest)
	require.True(t, status.HasCode(err, codes.Unauthenticated), "a token naming no key: %v", err)
}

// The signer writes exactly the claims tokens have always carried, and names no key.
func TestSigner(t *testing.T) {
	f := newFixture(t)
	_, err := authentication.NewSigner(f.issuer, `{"kty":"oct","k":"c2VjcmV0"}`)
	require.ErrorContains(t, err, "names no algorithm")

	claims := map[string]any{"scope": "view", "onikisu.com/Roles": []any{"support"}}
	token, expireTime, err := f.signer.Sign(authentication.Token{Subject: "users/alice", Actor: "jdoe@example.com", Lifetime: time.Hour, Claims: claims})
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(time.Hour), expireTime, 2*time.Second)
	parsed, err := jwt.ParseInsecure([]byte(token))
	require.NoError(t, err)
	written, err := parsed.AsMap(context.Background())
	require.NoError(t, err)
	require.Equal(t, expireTime.Unix(), parsed.Expiration().Unix())
	require.Equal(t, parsed.IssuedAt(), parsed.NotBefore())
	for _, timed := range []string{"iat", "nbf", "exp"} {
		delete(written, timed)
	}
	require.Equal(t, map[string]any{
		"iss": "test", "aud": []string{"test"}, "sub": "users/alice", "act": map[string]any{"sub": "jdoe@example.com"},
		"scope": "view", "onikisu.com/Roles": []any{"support"},
	}, written)
	message, err := jws.Parse([]byte(token))
	require.NoError(t, err)
	require.Empty(t, message.Signatures()[0].ProtectedHeaders().KeyID())

	seen, err := unary(t, f.interceptor(t), getBookMethod, bearer(token), getBookRequest)
	require.NoError(t, err)
	require.Equal(t, "jdoe@example.com", seen.session.GetActor().GetSubject(), "the act claim names the actor by default")
}

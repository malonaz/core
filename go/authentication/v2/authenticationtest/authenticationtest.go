// Package authenticationtest helps test code that runs behind the authentication interceptor: a context carrying a
// session, and issuers whose tokens tests sign.
package authenticationtest

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"testing"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	authentication "github.com/malonaz/core/go/authentication/v2"
	"github.com/malonaz/core/go/authentication/v2/internal/sessioncontext"
)

// NewContext returns a context carrying the session, as a handler's context does once the interceptor let the call in.
func NewContext(ctx context.Context, session *authenticationpb.Session) context.Context {
	return sessioncontext.With(ctx, session)
}

// KeySet returns a JSON Web Key Set of the keys.
func KeySet(keys ...string) string {
	rawKeys := make([]json.RawMessage, len(keys))
	for i, key := range keys {
		rawKeys[i] = json.RawMessage(key)
	}
	bytes, err := json.Marshal(map[string]any{"keys": rawKeys})
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

// NewIssuer returns an issuer whose `iss` and `aud` are its id, verifying with a fresh HS256 key, and a signer of its
// tokens. Its sessions hold the grants grantsCEL lists.
func NewIssuer(t testing.TB, id, grantsCEL string) (*authenticationpb.Issuer, *authentication.Signer) {
	t.Helper()
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatalf("generating key: %v", err)
	}
	key, err := authentication.SymmetricKey(string(secret))
	if err != nil {
		t.Fatalf("wrapping key: %v", err)
	}
	issuer := &authenticationpb.Issuer{
		Id:        id,
		Issuer:    id,
		Audience:  id,
		Keys:      &authenticationpb.Issuer_Jwks{Jwks: KeySet(key)},
		GrantsCel: grantsCEL,
	}
	signer, err := authentication.NewSigner(issuer, key)
	if err != nil {
		t.Fatalf("creating signer: %v", err)
	}
	return issuer, signer
}

package authentication

import (
	"fmt"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
)

type JwtMinter struct {
	symmetricKey []byte
	issuer       string
	audience     string
}

func NewJwtMinter(issuerConfig *authenticationpb.JwtIssuer) (*JwtMinter, error) {
	symmetricKey := issuerConfig.GetSymmetricKey()
	if symmetricKey == "" {
		return nil, fmt.Errorf("issuer %q: symmetric_key is required for minting", issuerConfig.Id)
	}
	return &JwtMinter{
		symmetricKey: []byte(symmetricKey),
		issuer:       issuerConfig.Issuer,
		audience:     issuerConfig.Audience,
	}, nil
}

type MintJwtOpts struct {
	Subject  string
	Lifetime time.Duration
	Claims   map[string]any
}

func (m *JwtMinter) MintJwt(opts MintJwtOpts) (string, error) {
	now := time.Now()
	builder := jwt.NewBuilder().
		Issuer(m.issuer).
		Audience([]string{m.audience}).
		IssuedAt(now).
		NotBefore(now).
		Expiration(now.Add(opts.Lifetime))

	if opts.Subject != "" {
		builder = builder.Subject(opts.Subject)
	}
	for key, value := range opts.Claims {
		builder = builder.Claim(key, value)
	}

	token, err := builder.Build()
	if err != nil {
		return "", fmt.Errorf("building JWT: %w", err)
	}

	signed, err := jwt.Sign(token, jwt.WithKey(jwa.HS256, m.symmetricKey))
	if err != nil {
		return "", fmt.Errorf("signing JWT: %w", err)
	}
	return string(signed), nil
}

// actorClaim names the actor of a delegated token, as RFC 8693 (section 4.1) defines it: {"act": {"sub": actor}}.
const actorClaim = "act"

// MintDelegatedJwt mints a token for an actor acting on behalf of a subject, the principal, for a lifetime.
func (m *JwtMinter) MintDelegatedJwt(subject, actor string, lifetime time.Duration) (string, error) {
	if subject == "" || actor == "" {
		return "", fmt.Errorf("a delegated JWT requires a subject and an actor")
	}
	return m.MintJwt(MintJwtOpts{
		Subject:  subject,
		Lifetime: lifetime,
		Claims:   map[string]any{actorClaim: map[string]any{"sub": actor}},
	})
}

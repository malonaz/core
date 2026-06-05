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

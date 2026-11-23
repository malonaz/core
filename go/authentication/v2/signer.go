package authentication

import (
	"fmt"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
)

// Token is what a token issued by one of the host's issuers says.
type Token struct {
	// Subject is the `sub` claim, omitted when empty.
	Subject string
	// Actor is who acts for the subject, written as the `act` claim (RFC 8693); omitted when empty.
	Actor string
	// Lifetime is how long the token is valid from now.
	Lifetime time.Duration
	// Claims are further claims, written last.
	Claims map[string]any
}

// Signer issues tokens of an issuer the host owns.
type Signer struct {
	issuer   string
	audience string
	key      jwk.Key
}

// NewSigner returns a signer of the issuer's tokens. The key is a JSON Web Key naming its algorithm: the
// configuration holds only what verification needs, so the signing key is given to the process that issues.
func NewSigner(issuer *authenticationpb.Issuer, key string) (*Signer, error) {
	signingKey, err := jwk.ParseKey([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("parsing signing key of issuer %q: %w", issuer.GetId(), err)
	}
	if signingKey.Algorithm().String() == "" {
		return nil, fmt.Errorf("signing key of issuer %q names no algorithm", issuer.GetId())
	}
	return &Signer{issuer: issuer.GetIssuer(), audience: issuer.GetAudience(), key: signingKey}, nil
}

// Sign returns a signed token and when it expires. Its claims keep the shape tokens issued earlier have.
func (s *Signer) Sign(token Token) (string, time.Time, error) {
	// Tokens carry whole seconds: the expiry returned is the one written.
	now := time.Now().Truncate(time.Second)
	expireTime := now.Add(token.Lifetime)
	builder := jwt.NewBuilder().
		Issuer(s.issuer).
		Audience([]string{s.audience}).
		IssuedAt(now).
		NotBefore(now).
		Expiration(expireTime)
	if token.Subject != "" {
		builder = builder.Subject(token.Subject)
	}
	if token.Actor != "" {
		builder = builder.Claim(actorClaim, map[string]any{"sub": token.Actor})
	}
	for name, value := range token.Claims {
		builder = builder.Claim(name, value)
	}
	built, err := builder.Build()
	if err != nil {
		return "", time.Time{}, fmt.Errorf("building token: %w", err)
	}
	signed, err := signToken(built, s.key)
	if err != nil {
		return "", time.Time{}, err
	}
	return signed, expireTime, nil
}

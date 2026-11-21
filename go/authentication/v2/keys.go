package authentication

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// keySet verifies tokens with keys given inline or fetched from a JWKS URL.
type keySet struct {
	set   jwk.Set
	cache *jwk.Cache
	uri   string
}

// newInlineKeySet parses a JSON Web Key Set. Every key must name its algorithm: it is used with no other.
func newInlineKeySet(jwks string) (*keySet, error) {
	set, err := jwk.Parse([]byte(jwks))
	if err != nil {
		return nil, fmt.Errorf("parsing key set: %w", err)
	}
	for index := range set.Len() {
		key, _ := set.Key(index)
		if key.Algorithm().String() == "" {
			return nil, fmt.Errorf("key %d names no algorithm", index)
		}
	}
	return &keySet{set: set}, nil
}

// newRemoteKeySet fetches a JSON Web Key Set now, so that a server never starts unable to verify, and keeps it fresh.
func newRemoteKeySet(ctx context.Context, uri string) (*keySet, error) {
	cache := jwk.NewCache(ctx)
	if err := cache.Register(uri, jwk.WithMinRefreshInterval(15*time.Minute)); err != nil {
		return nil, fmt.Errorf("registering key set %q: %w", uri, err)
	}
	if _, err := cache.Refresh(ctx, uri); err != nil {
		return nil, fmt.Errorf("fetching key set %q: %w", uri, err)
	}
	return &keySet{cache: cache, uri: uri}, nil
}

// SymmetricKey wraps a shared secret as an HS256 JSON Web Key: what NewSigner takes, and what a key set lists.
func SymmetricKey(secret string) (string, error) {
	key, err := newSymmetricKey(secret)
	if err != nil {
		return "", err
	}
	bytes, err := json.Marshal(key)
	if err != nil {
		return "", fmt.Errorf("marshaling key: %w", err)
	}
	return string(bytes), nil
}

func newSymmetricKey(secret string) (jwk.Key, error) {
	key, err := jwk.FromRaw([]byte(secret))
	if err != nil {
		return nil, fmt.Errorf("wrapping secret: %w", err)
	}
	if err := key.Set(jwk.AlgorithmKey, jwa.HS256); err != nil {
		return nil, fmt.Errorf("setting algorithm: %w", err)
	}
	return key, nil
}

// verifyOption verifies a token against the set. Inline keys are tried whether or not the token names a `kid`, as
// the tokens the host issues today carry none; a fetched set requires one, as v1 did.
func (k *keySet) verifyOption(ctx context.Context) (jwt.ParseOption, error) {
	if k.cache == nil {
		return jwt.WithKeySet(k.set, jws.WithRequireKid(false)), nil
	}
	set, err := k.cache.Get(ctx, k.uri)
	if err != nil {
		return nil, fmt.Errorf("fetching key set %q: %w", k.uri, err)
	}
	return jwt.WithKeySet(set), nil
}

// signToken signs a token with a key, using the algorithm the key names.
func signToken(token jwt.Token, key jwk.Key) (string, error) {
	signed, err := jwt.Sign(token, jwt.WithKey(key.Algorithm(), key))
	if err != nil {
		return "", fmt.Errorf("signing token: %w", err)
	}
	return string(signed), nil
}

// unverifiedClaims are what choose the keys a token is verified with: read before verifying, trusted only after.
type unverifiedClaims struct {
	Issuer   string          `json:"iss"`
	Audience json.RawMessage `json:"aud"`
}

// readUnverifiedClaims decodes a compact token's `iss` and `aud` alone: a full parse would double a verification's cost.
func readUnverifiedClaims(rawToken string) (*unverifiedClaims, error) {
	parts := strings.Split(rawToken, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("expected 3 parts, got %d", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decoding payload: %w", err)
	}
	claims := &unverifiedClaims{}
	if err := json.Unmarshal(payload, claims); err != nil {
		return nil, fmt.Errorf("unmarshaling payload: %w", err)
	}
	return claims, nil
}

// audiences reads `aud`, a string or a list of strings (RFC 7519).
func (c *unverifiedClaims) audiences() []string {
	var audience string
	if err := json.Unmarshal(c.Audience, &audience); err == nil {
		return []string{audience}
	}
	var audiences []string
	if err := json.Unmarshal(c.Audience, &audiences); err != nil {
		return nil
	}
	return audiences
}

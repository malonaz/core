package authentication

import (
	"context"
	"errors"

	authenticationpb "github.com/malonaz/core/proto/authentication"
)

type localSessionKey struct{}

var ErrSessionNotFound = errors.New("session not found in context")

// GetSession retrieves the session *locally*.
func GetSession(ctx context.Context) (*authenticationpb.Session, error) {
	value := ctx.Value(localSessionKey{})
	if value == nil {
		return nil, ErrSessionNotFound
	}

	session, ok := value.(*authenticationpb.Session)
	if !ok {
		return nil, ErrSessionNotFound
	}

	return session, nil
}

// MustGetSession retrieves the session *locally*.
func MustGetSession(ctx context.Context) *authenticationpb.Session {
	session, err := GetSession(ctx)
	if err != nil {
		panic(err)
	}
	return session
}

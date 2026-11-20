// Package sessioncontext holds the context key of the session, shared by the package and its test package
// so that neither exports a way to put an unverified session on a context.
package sessioncontext

import (
	"context"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
)

type sessionKey struct{}

// With returns a context carrying the session.
func With(ctx context.Context, session *authenticationpb.Session) context.Context {
	return context.WithValue(ctx, sessionKey{}, session)
}

// From returns the session the context carries, or nil.
func From(ctx context.Context) *authenticationpb.Session {
	session, _ := ctx.Value(sessionKey{}).(*authenticationpb.Session)
	return session
}

package authentication

import (
	"context"
	"errors"

	grpc_metadata "github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	authenticationpb "github.com/malonaz/core/genproto/authentication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const metadataKeySession = "x-session-bin"

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

func WithRole(ctx context.Context, roleID string) (context.Context, error) {
	session := &authenticationpb.Session{
		RoleIds: []string{roleID},
	}
	return InjectSession(ctx, session)
}

// InjectSession into the context.
func InjectSession(ctx context.Context, session *authenticationpb.Session) (context.Context, error) {
	// We do not support multi session injection for now.
	if value := grpc_metadata.ExtractOutgoing(ctx).Get(metadataKeySession); value != "" {
		return nil, status.Errorf(codes.Internal, "multi-session context is not supported")
	}

	// We inject it to outgoing context.
	bytes, err := proto.Marshal(session)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling session: %v", err)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, metadataKeySession, string(bytes))

	// We also inject it into the local context, so that admin-api handlers can access it.
	return context.WithValue(ctx, localSessionKey{}, session), nil
}

package authentication

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/grpc/interceptor"
	"github.com/malonaz/core/go/pbutil"
)

const metadataKeySignedSession = "x-signed-session-bin"

var ErrSignedSessionNotFound = errors.New("session not found in context")

type localSignedSessionKey struct{}

type SessionManagerOpts struct {
	Secret string `long:"secret" env:"SECRET" description:"Secret key for signing sessions" required:"true"`
}

type SessionManager struct {
	secretBytes []byte
}

func NewSessionManager(opts *SessionManagerOpts) *SessionManager {
	return &SessionManager{
		secretBytes: []byte(opts.Secret),
	}
}

// verifySession checks if a session has a valid signature.
func (s *SessionManager) verify(signedSession *authenticationpb.SignedSession) (bool, error) {
	// Marshal the session copy
	data, err := proto.Marshal(signedSession.Session)
	if err != nil {
		return false, fmt.Errorf("marshaling session: %w", err)
	}

	// Compute expected HMAC
	h := hmac.New(sha256.New, s.secretBytes)
	h.Write(data)
	expectedSig := h.Sum(nil)

	// Constant-time comparison
	return hmac.Equal(signedSession.Signature, expectedSig), nil
}

// signSession signs a session with HMAC-SHA256
func (s *SessionManager) sign(session *authenticationpb.Session) (*authenticationpb.SignedSession, error) {
	// Marshal the session
	data, err := proto.Marshal(session)
	if err != nil {
		return nil, fmt.Errorf("marshaling session: %w", err)
	}

	// Compute HMAC
	h := hmac.New(sha256.New, s.secretBytes)
	h.Write(data)

	// Return signed signature.
	return &authenticationpb.SignedSession{
		Session:   session,
		Signature: h.Sum(nil),
	}, nil
}

// injectSession into local context.
func (s *SessionManager) injectSignedSessionIntoLocalContext(
	ctx context.Context, signedSession *authenticationpb.SignedSession, isUpdate bool,
) (context.Context, error) {
	// We do not support multi session injection for now.
	value := ctx.Value(localSignedSessionKey{})
	if isUpdate && value == nil {
		return nil, status.Errorf(codes.Internal, "expected to find signed session in local context")
	}
	if !isUpdate && value != nil {
		return nil, status.Errorf(codes.Internal, "unexpected signed session in local context")
	}
	return context.WithValue(ctx, localSignedSessionKey{}, signedSession), nil
}

// Get session gets the signed session from the local context, verifies it and returns the underlying session.
func (s *SessionManager) getSignedSessionFromLocalContext(ctx context.Context) (*authenticationpb.SignedSession, error) {
	value := ctx.Value(localSignedSessionKey{})
	if value == nil {
		return nil, ErrSignedSessionNotFound
	}
	signedSession, ok := value.(*authenticationpb.SignedSession)
	if !ok {
		return nil, ErrSignedSessionNotFound
	}
	return signedSession, nil
}

/////////////////////////////////////////////////// LOCAL CONTEXT INJECTOR INTERCEPTOR ////////////////////////////////

// UnaryServerLocalContextInjectorInterceptor
//  1. parses the session from the incoming context
//  2. injects it into the local context
func (s *SessionManager) UnaryServerLocalContextInjectorInterceptor() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := s.injectSignedSessionFromIncomingContextToLocalContext(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, allButHealth)
}

// StreamServerLocalContextInjectorInterceptor
//  1. parses the session from the incoming context
//  2. injects it into the local context
func (s *SessionManager) StreamServerLocalContextInjectorInterceptor() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		ctx, err := s.injectSignedSessionFromIncomingContextToLocalContext(ctx)
		if err != nil {
			return err
		}
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
	return grpc_selector.StreamServerInterceptor(interceptor, allButHealth)
}

func (s *SessionManager) injectSignedSessionFromIncomingContextToLocalContext(ctx context.Context) (context.Context, error) {
	values := metadata.ValueFromIncomingContext(ctx, metadataKeySignedSession)
	if len(values) == 0 {
		// There is no session.
		return ctx, nil
	}
	if len(values) != 1 {
		return nil, status.Errorf(codes.Internal, "expected 1 signed session header, got %d", len(values))
	}
	value := values[0]

	// Parse the session.
	signedSession := &authenticationpb.SignedSession{}
	if err := pbutil.Unmarshal([]byte(value), signedSession); err != nil {
		return nil, status.Errorf(codes.Internal, "unmarshaling session: %v", err)
	}
	isUpdate := false
	return s.injectSignedSessionIntoLocalContext(ctx, signedSession, isUpdate)
}

/////////////////////////////////////////////////// OUTGOING CONTEXT INJECTOR INTERCEPTOR ////////////////////////////////

// UnaryServerOutgoingContextInjectorInterceptor
//  1. parses the session from the local context
//  2. injects it into the outgoing context
func (s *SessionManager) UnaryServerOutgoingContextInjectorInterceptor() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := s.injectSignedSessionFromLocalContextToOutgoingContext(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, allButHealth)
}

// StreamServerOutgoingContextInjectorInterceptor
//  1. parses the session from the local context
//  2. injects it into the outgoing context
func (s *SessionManager) StreamServerOutgoingContextInjectorInterceptor() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		ctx, err := s.injectSignedSessionFromLocalContextToOutgoingContext(ctx)
		if err != nil {
			return err
		}
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
	return grpc_selector.StreamServerInterceptor(interceptor, allButHealth)
}

func (s *SessionManager) injectSignedSessionFromLocalContextToOutgoingContext(ctx context.Context) (context.Context, error) {
	signedSession, err := s.getSignedSessionFromLocalContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting signed session from local context: %v", err)
	}

	// Marshal the signed session.
	bytes, err := proto.Marshal(signedSession)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling signed session: %v", err)
	}
	return metadata.AppendToOutgoingContext(ctx, metadataKeySignedSession, string(bytes)), nil
}

// ///////////////////////////////////////////////// INJECT SESSION TO LOG SESSION INTERCEPTOR ////////////////////////////////
// injectSessionFieldsIntoLogContext extracts session information and injects it into the log context
func (s *SessionManager) injectSessionFieldsIntoLogContext(ctx context.Context) error {
	signedSession, err := s.getSignedSessionFromLocalContext(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "getting signed session from local context: %v", err)
	}

	session := signedSession.Session
	fields := make([]any, 0)

	// Add session ID if non-zero
	if session.Id != "" {
		fields = append(fields, "session_id", session.Id)
	}

	// Add basic session fields (only non-zero values)
	if len(session.RoleIds) > 0 {
		fields = append(fields, "role_ids", session.RoleIds)
	}
	if session.Authorized {
		fields = append(fields, "authorized", session.Authorized)
	}
	if ipAddr := session.GetMetadata().GetIpAddress(); ipAddr != "" {
		fields = append(fields, "ip_address", ipAddr)
	}
	if clientVersion := session.GetMetadata().GetClientVersion(); clientVersion != nil {
		fields = append(fields, "client_version", fmt.Sprintf("%d.%d.%d", clientVersion.Major, clientVersion.Minor, clientVersion.Patch))
	}
	if userAgent := session.GetMetadata().GetUserAgent(); userAgent != "" {
		fields = append(fields, "user_agent", userAgent)
	}

	// Add custom metadata fields
	for k, v := range session.GetMetadata().GetKeyToValue() {
		if v != "" {
			fields = append(fields, fmt.Sprintf("custom.%s", k), v)
		}
	}

	// Add identity-specific fields
	switch identity := session.Identity.(type) {
	case *authenticationpb.Session_UserIdentity:
		fields = append(fields, "session_type", "user")
		if identity.UserIdentity.OrganizationId != "" {
			fields = append(fields, "organization_id", identity.UserIdentity.OrganizationId)
		}
		if identity.UserIdentity.UserId != "" {
			fields = append(fields, "user_id", identity.UserIdentity.UserId)
		}
	case *authenticationpb.Session_ServiceAccountIdentity:
		fields = append(fields, "session_type", "service_account")
		if identity.ServiceAccountIdentity.ServiceAccount.Id != "" {
			fields = append(fields, "service_account_id", identity.ServiceAccountIdentity.ServiceAccount.Id)
		}
		if saType := identity.ServiceAccountIdentity.ServiceAccount.Type.String(); saType != "" {
			fields = append(fields, "service_account_type", saType)
		}
	}

	// Single call to inject all fields
	if len(fields) > 0 {
		interceptor.InjectLogFields(ctx, fields...)
	}

	return nil
}

// UnaryServerLogInjectorInterceptor injects session fields into the log context for unary RPCs
func (s *SessionManager) UnaryServerLogInjectorInterceptor() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := s.injectSessionFieldsIntoLogContext(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, allButHealth)
}

// StreamServerLogInjectorInterceptor injects session fields into the log context for streaming RPCs
func (s *SessionManager) StreamServerLogInjectorInterceptor() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		if err := s.injectSessionFieldsIntoLogContext(ctx); err != nil {
			return err
		}
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
	return grpc_selector.StreamServerInterceptor(interceptor, allButHealth)
}

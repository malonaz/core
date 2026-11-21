package authentication

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/grpc/middleware"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/uuid"
)

const (
	metadataKeySignedSession  = "x-signed-session-bin"
	metadataKeyClientPlatform = "x-client-platform"
	metadataKeyClientVersion  = "x-client-version"
	metadataKeyClientTimezone = "x-client-timezone"
)

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

func GetSession(ctx context.Context) (*authenticationpb.Session, error) {
	signedSession, err := getSignedSessionFromLocalContext(ctx)
	if err != nil {
		return nil, err
	}
	return signedSession.Session, nil
}

// verify checks that a signed session is authentic and not expired.
func (s *SessionManager) verify(signedSession *authenticationpb.SignedSession) error {
	data, err := pbutil.MarshalDeterministic(signedSession.Session)
	if err != nil {
		return status.Errorf(codes.Internal, "marshaling session: %v", err)
	}
	h := hmac.New(sha256.New, s.secretBytes)
	h.Write(data)
	// Constant-time comparison.
	if !hmac.Equal(signedSession.Signature, h.Sum(nil)) {
		return status.Errorf(codes.Unauthenticated, "invalid session signature")
	}
	if expireTime := signedSession.Session.GetExpireTime(); expireTime != nil && !time.Now().Before(expireTime.AsTime()) {
		return status.Errorf(codes.Unauthenticated, "session expired at %s", expireTime.AsTime().Format(time.RFC3339))
	}
	return nil
}

// signSession signs a session with HMAC-SHA256
func (s *SessionManager) sign(session *authenticationpb.Session) (*authenticationpb.SignedSession, error) {
	// Marshal the session
	data, err := pbutil.MarshalDeterministic(session)
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

// Delegate returns a context whose outgoing calls act as a principal on behalf of an actor, for a lifetime. Its
// outgoing metadata is replaced by the signed delegated session alone, so no other credential rides along. The
// session is never authorized: the service it reaches checks the principal's permissions and the restrictions on
// actors.
func (s *SessionManager) Delegate(
	ctx context.Context, principal *authenticationpb.Identity, actor string, lifetime time.Duration,
) (context.Context, error) {
	if actor == "" {
		return nil, fmt.Errorf("delegating requires an actor")
	}
	if lifetime <= 0 {
		return nil, fmt.Errorf("delegating requires a positive lifetime, got %s", lifetime)
	}
	now := time.Now()
	session := &authenticationpb.Session{
		Id:         uuid.MustNewV7().String(),
		CreateTime: timestamppb.New(now),
		Delegation: &authenticationpb.Delegation{Actor: actor},
		ExpireTime: timestamppb.New(now.Add(lifetime)),
	}
	if err := setIdentity(session, principal); err != nil {
		return nil, fmt.Errorf("delegating: %w", err)
	}
	signedSession, err := s.sign(session)
	if err != nil {
		return nil, fmt.Errorf("signing delegated session: %w", err)
	}
	bytes, err := pbutil.MarshalDeterministic(signedSession)
	if err != nil {
		return nil, fmt.Errorf("marshaling delegated session: %w", err)
	}
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(metadataKeySignedSession, string(bytes))), nil
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
func getSignedSessionFromLocalContext(ctx context.Context) (*authenticationpb.SignedSession, error) {
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
	return grpc_selector.UnaryServerInterceptor(interceptor, middleware.AllButHealth)
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
	return grpc_selector.StreamServerInterceptor(interceptor, middleware.AllButHealth)
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
	// The local context now owns the session: a handler forwarding its incoming metadata must not resend it.
	ctx = removeFromIncomingContext(ctx, metadataKeySignedSession)
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
	return grpc_selector.UnaryServerInterceptor(interceptor, middleware.AllButHealth)
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
	return grpc_selector.StreamServerInterceptor(interceptor, middleware.AllButHealth)
}

func (s *SessionManager) injectSignedSessionFromLocalContextToOutgoingContext(ctx context.Context) (context.Context, error) {
	signedSession, err := getSignedSessionFromLocalContext(ctx)
	if err != nil {
		if errors.Is(err, ErrSignedSessionNotFound) {
			return ctx, nil // No session to propagate, continue with original context
		}
		return nil, status.Errorf(codes.Internal, "getting signed session from local context: %v", err)
	}

	// Marshal the signed session.
	bytes, err := pbutil.MarshalDeterministic(signedSession)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshaling signed session: %v", err)
	}
	// Set, not append: a call carries exactly one session.
	outgoingMetadata, _ := metadata.FromOutgoingContext(ctx)
	outgoingMetadata = outgoingMetadata.Copy()
	outgoingMetadata.Set(metadataKeySignedSession, string(bytes))
	return metadata.NewOutgoingContext(ctx, outgoingMetadata), nil
}

// ///////////////////////////////////////////////// INJECT SESSION TO LOG SESSION INTERCEPTOR ////////////////////////////////
// injectSessionFieldsIntoLogContext extracts session information and injects it into the log context
func (s *SessionManager) injectSessionFieldsIntoLogContext(ctx context.Context) error {
	signedSession, err := getSignedSessionFromLocalContext(ctx)
	if err != nil {
		if errors.Is(err, ErrSignedSessionNotFound) {
			return nil // No session to log, skip gracefully
		}
		return status.Errorf(codes.Internal, "getting signed session from local context: %v", err)
	}

	session := signedSession.Session
	fields := make([]any, 0)

	// Add session ID if non-zero
	if session.Id != "" {
		fields = append(fields, "session_id", session.Id)
	}

	// Add basic session fields (only non-zero values)
	if session.Authorized {
		fields = append(fields, "authorized", session.Authorized)
	}
	if actor := session.GetDelegation().GetActor(); actor != "" {
		fields = append(fields, "actor", actor)
	}
	if ipAddr := session.GetMetadata().GetIpAddress(); ipAddr != "" {
		fields = append(fields, "ip_address", ipAddr)
	}
	if clientPlatform := session.GetMetadata().GetClientPlatform(); clientPlatform != "" {
		fields = append(fields, "client_platform", clientPlatform)
	}
	if clientVersion := session.GetMetadata().GetClientVersion(); clientVersion != nil {
		fields = append(fields, "client_version", fmt.Sprintf("%d.%d.%d", clientVersion.Major, clientVersion.Minor, clientVersion.Patch))
	}
	if userAgent := session.GetMetadata().GetUserAgent(); userAgent != "" {
		fields = append(fields, "user_agent", userAgent)
	}

	// Add identity-specific fields
	switch identity := session.Identity.(type) {
	case *authenticationpb.Session_AnonymousIdentity:
		fields = append(fields, "session_type", "anonymous")

	case *authenticationpb.Session_JwtIdentity:
		fields = append(fields, "session_type", "jwt")
		for claim, value := range identity.JwtIdentity.GetClaims().AsMap() {
			fields = append(fields, "claim."+claim, fmt.Sprintf("%v", value))
		}

	case *authenticationpb.Session_ServiceAccountIdentity:
		fields = append(fields, "session_type", "service_account")
		if identity.ServiceAccountIdentity.ServiceAccountId != "" {
			fields = append(fields, "service_account_id", identity.ServiceAccountIdentity.ServiceAccountId)
		}
		if saType := identity.ServiceAccountIdentity.ServiceAccountType.String(); saType != "" {
			fields = append(fields, "service_account_type", saType)
		}
	}

	// Single call to inject all fields
	if len(fields) > 0 {
		middleware.InjectLogFields(ctx, fields...)
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
	return grpc_selector.UnaryServerInterceptor(interceptor, middleware.AllButHealth)
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
	return grpc_selector.StreamServerInterceptor(interceptor, middleware.AllButHealth)
}

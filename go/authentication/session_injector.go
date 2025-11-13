package authentication

import (
	"context"
	"strconv"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/contexttag"
	"github.com/malonaz/core/go/pbutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ////////////////////////////////////////// FOR DOWNSTREAM SERVERS ////////////////////////////////////////////
type SessionInjectorInterceptor struct{}

func NewSessionInjectorInterceptor() *SessionInjectorInterceptor {
	return &SessionInjectorInterceptor{}
}

// UnarySessionInjectorInterceptor:
//  1. parses the session from the incoming context
//  2. injects it into the local context
//  3. Injects relevant log fields via context tags.
func (i *SessionInjectorInterceptor) Unary() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := sessionInjectorFn(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, allButHealth)
}

// StreamingSessionInjectorInterceptor:
//  1. parses the session from the incoming context
//  2. injects it into the local context
//  3. Injects relevant log fields via context tags.
func (i *SessionInjectorInterceptor) Stream() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		ctx, err := sessionInjectorFn(ctx)
		if err != nil {
			return err
		}
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
	return grpc_selector.StreamServerInterceptor(interceptor, allButHealth)
}

func sessionInjectorFn(ctx context.Context) (context.Context, error) {
	values := metadata.ValueFromIncomingContext(ctx, metadataKeySession)
	if len(values) == 0 {
		// Parse the session.
		return nil, status.Errorf(codes.Unauthenticated, "missing session")
	}

	// Parse the session.
	session := &authenticationpb.Session{}
	if err := pbutil.Unmarshal([]byte(values[0]), session); err != nil {
		return nil, status.Errorf(codes.Internal, "unmarshaling session: %v", err)
	}

	// Inject it into the local context so RPC handlers can grab it.
	ctx = context.WithValue(ctx, localSessionKey{}, session)

	// Inject log fields tags.
	return ctx, injectSessionIntoLogFieldsTags(ctx, session)
}

func injectSessionIntoLogFieldsTags(ctx context.Context, session *authenticationpb.Session) error {
	tags, ok := contexttag.GetLogTags(ctx)
	if !ok {
		return status.Errorf(codes.Internal, "no log fields tags found")
	}
	if session.UserId != "" {
		tags.Append("user_id", session.UserId)
	}
	if session.OrganizationId != "" {
		tags.Append("user_id", session.OrganizationId)
	}
	if ipAddress := session.GetMetadata().GetIpAddress(); ipAddress != "" {
		tags.Append("ip_address", ipAddress)
	}
	clientVersion := session.GetMetadata().GetClientVersion()
	if clientVersion != nil {
		semver := strconv.Itoa(int(clientVersion.Major)) + "." +
			strconv.Itoa(int(clientVersion.Minor)) + "." +
			strconv.Itoa(int(clientVersion.Patch))
		tags.Append("semver", semver)
	}
	return nil
}

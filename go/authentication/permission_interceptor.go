// permission_interceptor.go
package authentication

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/grpc/middleware"
	"github.com/malonaz/core/go/grpc/status"
)

type PermissionAuthenticationInterceptorOpts struct {
	Config string `long:"config" env:"CONFIG" description:"Path to the authentication configuration file" required:"true"`
}

type PermissionAuthenticationInterceptor struct {
	sessionManager              *SessionManager
	serviceAccountIDToMethodSet map[string]map[string]struct{}
	publicMethodSet             map[string]struct{}
	issuerIDToAuthorizer        map[string]*jwtAuthorizer
}

// compileWildcardPermission converts a glob-style permission pattern into a compiled regexp.
func compileWildcardPermission(pattern string) (*regexp.Regexp, error) {
	regexPattern := "^" + regexp.QuoteMeta(pattern) + "$"
	regexPattern = strings.ReplaceAll(regexPattern, `\*`, `.*`)
	compiled, err := regexp.Compile(regexPattern)
	if err != nil {
		return nil, fmt.Errorf("compiling wildcard permission pattern %q: %w", pattern, err)
	}
	return compiled, nil
}

// getMethodSet returns the set of all registered RPC full method names from the proto registry.
func getMethodSet() map[string]struct{} {
	fullMethodSet := map[string]struct{}{}
	protoregistry.GlobalFiles.RangeFiles(func(fileDescriptor protoreflect.FileDescriptor) bool {
		services := fileDescriptor.Services()
		for i := 0; i < services.Len(); i++ {
			serviceDescriptor := services.Get(i)
			serviceName := string(serviceDescriptor.FullName())
			methods := serviceDescriptor.Methods()
			for j := 0; j < methods.Len(); j++ {
				methodName := string(methods.Get(j).Name())
				fullMethod := fmt.Sprintf("/%s/%s", serviceName, methodName)
				fullMethodSet[fullMethod] = struct{}{}
			}
		}
		return true
	})
	return fullMethodSet
}

func NewPermissionAuthenticationInterceptor(
	opts *PermissionAuthenticationInterceptorOpts,
	sessionManager *SessionManager,
) (*PermissionAuthenticationInterceptor, error) {
	configuration := &authenticationpb.PermissionConfiguration{}
	if err := parseConfig(opts.Config, configuration); err != nil {
		return nil, err
	}

	// Collect all registered RPC full method names from the proto registry.
	methodSet := getMethodSet()

	// Compute a mapping of role ids to role.
	roleIDToRole := map[string]*authenticationpb.Role{}
	for _, role := range configuration.Roles {
		roleIDToRole[role.Id] = role
	}

	// Compute a mapping of service account ids to permissions.
	// Handles inherited roles.
	serviceAccountIDToPermissions := map[string][]string{}
	for _, serviceAccount := range configuration.ServiceAccounts {
		permissions := serviceAccount.Permissions
		for _, roleID := range serviceAccount.RoleIds {
			permissionSet, err := getPermissionSetForRole(roleID, roleIDToRole, make(map[string]bool))
			if err != nil {
				return nil, err
			}
			for permission := range permissionSet {
				permissions = append(permissions, permission)
			}
		}
		serviceAccountIDToPermissions[serviceAccount.Id] = permissions
	}

	// Build service account permission map.
	// Expand wildcard permissions into exact matches against registered methods.
	serviceAccountIDToMethodSet := map[string]map[string]struct{}{}

	for serviceAccountID, permissions := range serviceAccountIDToPermissions {
		serviceAccountIDToMethodSet[serviceAccountID] = map[string]struct{}{}
		for _, permission := range permissions {
			if strings.Contains(permission, "*") {
				// Wildcard permission: expand against registered methods.
				pattern, err := compileWildcardPermission(permission)
				if err != nil {
					return nil, err
				}
				matchCount := 0
				for method := range methodSet {
					if pattern.MatchString(method) {
						matchCount++
						serviceAccountIDToMethodSet[serviceAccountID][method] = struct{}{}
					}
				}
				if matchCount == 0 {
					return nil, fmt.Errorf("wildcard permission %q for service account %q matches no registered RPC methods", permission, serviceAccountID)
				}
			} else {
				method := permission // Exact match.

				// Exact permission: validate it maps to a registered method.
				if _, ok := methodSet[method]; !ok {
					return nil, fmt.Errorf("permission %q in service account %q does not match any registered RPC method", permission, serviceAccountID)
				}
				serviceAccountIDToMethodSet[serviceAccountID][method] = struct{}{}
			}
		}
	}

	// Build skip methods set.
	publicMethodSet := make(map[string]struct{}, len(configuration.PublicMethods))
	for _, method := range configuration.PublicMethods {
		publicMethodSet[method] = struct{}{}
	}

	// Compile CEL authorizers per issuer.
	issuerIDToAuthorizer := make(map[string]*jwtAuthorizer, len(configuration.GetJwtIssuers()))
	for _, jwtIssuer := range configuration.GetJwtIssuers() {
		authorizer, err := newJwtAuthorizer(jwtIssuer)
		if err != nil {
			return nil, err
		}
		issuerIDToAuthorizer[jwtIssuer.Id] = authorizer
	}

	return &PermissionAuthenticationInterceptor{
		sessionManager:              sessionManager,
		serviceAccountIDToMethodSet: serviceAccountIDToMethodSet,
		publicMethodSet:             publicMethodSet,
		issuerIDToAuthorizer:        issuerIDToAuthorizer,
	}, nil
}

// getPermissionSetForRole recursively collects all permissions for a role,
// including permissions from inherited roles.
func getPermissionSetForRole(roleID string, roleIDToRole map[string]*authenticationpb.Role, visited map[string]bool) (map[string]struct{}, error) {
	// Prevent infinite loops in case of circular inheritance.
	if visited[roleID] {
		return nil, fmt.Errorf("circular role inheritance detected: role %q", roleID)
	}
	visited[roleID] = true

	role, exists := roleIDToRole[roleID]
	if !exists {
		return nil, fmt.Errorf("unknown role %q", roleID)
	}

	permissions := make(map[string]struct{})

	// Add direct permissions.
	for _, permission := range role.Permissions {
		permissions[permission] = struct{}{}
	}

	// Add inherited permissions.
	for _, inheritedRoleID := range role.InheritedRoleIds {
		inheritedPermissions, err := getPermissionSetForRole(inheritedRoleID, roleIDToRole, visited)
		if err != nil {
			return nil, err
		}
		for permission := range inheritedPermissions {
			permissions[permission] = struct{}{}
		}
	}

	return permissions, nil
}

func (i *PermissionAuthenticationInterceptor) authenticate(ctx context.Context, fullMethod string, request any) (context.Context, error) {
	signedSession, err := getSignedSessionFromLocalContext(ctx)
	if err != nil {
		if !errors.Is(err, ErrSignedSessionNotFound) {
			return nil, status.Errorf(codes.Internal, "getting signed session from local context: %v", err).Err()
		}

		// If no session is present, instantiate an anonymous session.
		sessionMetadata, err := extractSessionMetadataFromContext(ctx)
		if err != nil {
			return nil, err
		}
		session := &authenticationpb.Session{
			Identity: &authenticationpb.Session_AnonymousIdentity{
				AnonymousIdentity: &authenticationpb.AnonymousIdentity{},
			},
			Metadata: sessionMetadata,
		}
		signedSession, err = i.sessionManager.sign(session)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "signing session: %v", err).Err()
		}
		isUpdate := false
		ctx, err = i.sessionManager.injectSignedSessionIntoLocalContext(ctx, signedSession, isUpdate)
		if err != nil {
			return nil, err
		}
	}

	// Verify the session signature.
	ok, err := i.sessionManager.verify(signedSession)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "verifying session: %v", err).Err()
	}
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "invalid session signature").Err()
	}
	session := signedSession.Session

	// The session is already authorized, we do not re-check permissions.
	if session.Authorized {
		return ctx, nil
	}

	// Check the permissions for non-public methods.
	if _, ok := i.publicMethodSet[fullMethod]; !ok {
		switch identity := session.GetIdentity().(type) {
		case *authenticationpb.Session_JwtIdentity:
			issuerID := identity.JwtIdentity.GetIssuerId()
			authorizer, ok := i.issuerIDToAuthorizer[issuerID]
			if !ok {
				return nil, status.Errorf(codes.Unauthenticated, "unknown issuer %q", issuerID).Err()
			}
			allowed, err := authorizer.authorize(fullMethod, identity.JwtIdentity.GetClaims(), request)
			if err != nil {
				if errors.Is(err, ErrMethodNotConfigured) {
					return nil, status.Errorf(codes.PermissionDenied, "no authorization rule for %q", fullMethod).Err()
				}
				return nil, status.Errorf(codes.Internal, "evaluating authorization: %v", err).Err()
			}
			if !allowed {
				return nil, status.Errorf(codes.PermissionDenied, "not authorized for %q", fullMethod).Err()
			}
		case *authenticationpb.Session_ServiceAccountIdentity:
			serviceAccountID := identity.ServiceAccountIdentity.GetServiceAccountId()
			methodSet, ok := i.serviceAccountIDToMethodSet[serviceAccountID]
			if !ok {
				return nil, status.Errorf(codes.Unauthenticated, "unknown service account %q", serviceAccountID).Err()
			}
			if _, ok := methodSet[fullMethod]; !ok {
				return nil, status.Errorf(codes.PermissionDenied, "requires permission %q", fullMethod).Err()
			}
		case *authenticationpb.Session_AnonymousIdentity:
			return nil, status.Errorf(codes.Unauthenticated, "requires permission %q", fullMethod).Err()
		default:
			return nil, status.Errorf(codes.Unauthenticated, "only service accounts are supported").Err()
		}
	}

	// Set the session to authorized, sign it and store it.
	session.Authorized = true
	signedSession, err = i.sessionManager.sign(session)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "signing session: %v", err).Err()
	}
	isUpdate := true
	return i.sessionManager.injectSignedSessionIntoLocalContext(ctx, signedSession, isUpdate)
}

// Unary implements the authentication unary interceptor.
func (i *PermissionAuthenticationInterceptor) Unary() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := i.authenticate(ctx, info.FullMethod, request)
		if err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, middleware.AllButHealth)
}

// Stream implements the authentication stream interceptor.
// For server-streaming RPCs (single client request), we peek the request message
// before the handler runs so that CEL authorization has access to it and the
// session is marked authorized before any downstream RPCs are made.
// For client-streaming and bidi-streaming RPCs, JWT authentication is not
// supported because there is no single request message to authorize against.
func (i *PermissionAuthenticationInterceptor) Stream() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Server-stream only: peek the single client request so we can authorize
		// before the handler executes. This ensures the session carries
		// Authorized=true for any downstream RPCs the handler makes.
		if info.IsServerStream && !info.IsClientStream {
			inputDescriptor, err := resolveMethodInputDescriptor(info.FullMethod)
			if err != nil {
				return status.Errorf(codes.Internal, "resolving method input descriptor: %v", err).Err()
			}
			wrappedStream := &peekedServerStream{
				WrappedServerStream: grpc_middleware.WrappedServerStream{
					ServerStream:   stream,
					WrappedContext: stream.Context(),
				},
			}
			request := dynamicpb.NewMessage(inputDescriptor)
			if err := wrappedStream.peekRequest(request); err != nil {
				return err
			}
			ctx, err := i.authenticate(stream.Context(), info.FullMethod, request)
			if err != nil {
				return err
			}
			wrappedStream.WrappedContext = ctx
			return handler(srv, wrappedStream)
		}

		// Client-streaming or bidi-streaming: reject JWT sessions since we cannot
		// authorize against a single request message.
		signedSession, _ := getSignedSessionFromLocalContext(stream.Context())
		if signedSession != nil {
			if _, isJwt := signedSession.Session.GetIdentity().(*authenticationpb.Session_JwtIdentity); isJwt {
				return status.Errorf(codes.Unimplemented, "JWT authentication is not supported for client-streaming or bidi-streaming RPCs").Err()
			}
		}

		ctx, err := i.authenticate(stream.Context(), info.FullMethod, nil)
		if err != nil {
			return err
		}
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
	return grpc_selector.StreamServerInterceptor(interceptor, middleware.AllButHealth)
}

// peekedServerStream wraps a server stream to buffer the first request message.
// This allows the interceptor to read the request for authorization before the
// generated handler calls RecvMsg. On the first RecvMsg call from the handler,
// the buffered message is replayed via proto.Merge; subsequent calls delegate
// to the underlying stream.
type peekedServerStream struct {
	grpc_middleware.WrappedServerStream
	peekedRequest proto.Message
}

// peekRequest reads the first message from the underlying stream and buffers it.
// Must be called exactly once before the handler runs.
func (s *peekedServerStream) peekRequest(message proto.Message) error {
	if err := s.ServerStream.RecvMsg(message); err != nil {
		return err
	}
	s.peekedRequest = message
	return nil
}

// RecvMsg replays the buffered request on the first call, then delegates to the
// underlying stream for any subsequent calls.
func (s *peekedServerStream) RecvMsg(message any) error {
	if s.peekedRequest != nil {
		protoMessage := message.(proto.Message)
		proto.Reset(protoMessage)
		proto.Merge(protoMessage, s.peekedRequest)
		s.peekedRequest = nil
		return nil
	}
	return s.ServerStream.RecvMsg(message)
}

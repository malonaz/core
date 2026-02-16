package authentication

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_interceptors "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
)

type PermissionAuthenticationInterceptorOpts struct {
	Config string `long:"config" env:"CONFIG" description:"Path to the authentication configuration file" required:"true"`
}

type PermissionAuthenticationInterceptor struct {
	sessionManager    *SessionManager
	methodToRoleIDSet map[string]map[string]struct{}
	publicMethodSet   map[string]struct{}
}

var (
	allButHealth = grpc_selector.MatchFunc(func(ctx context.Context, callMeta grpc_interceptors.CallMeta) bool {
		return grpc_health_v1.Health_ServiceDesc.ServiceName != callMeta.Service
	})
)

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

// allFullMethods returns the set of all registered RPC full method names from the proto registry.
func allFullMethods() map[string]struct{} {
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
	fullMethodSet := allFullMethods()

	roleIDToRole := map[string]*authenticationpb.Role{}
	for _, role := range configuration.Roles {
		roleIDToRole[role.Id] = role
	}

	// Build permission map with role inheritance.
	// Expand wildcard permissions into exact matches against registered methods.
	methodToRoleIDSet := map[string]map[string]struct{}{}

	// For each role, get all its permissions (including inherited ones)
	for _, role := range configuration.Roles {
		allPermissions := getAllPermissionsForRole(role.Id, roleIDToRole, make(map[string]bool))

		// Map each permission to this role
		for permission := range allPermissions {
			if strings.Contains(permission, "*") {
				// Wildcard permission: expand against registered methods.
				pattern, err := compileWildcardPermission(permission)
				if err != nil {
					return nil, err
				}
				matchCount := 0
				for fullMethod := range fullMethodSet {
					if pattern.MatchString(fullMethod) {
						matchCount++
						roleIDSet, ok := methodToRoleIDSet[fullMethod]
						if !ok {
							roleIDSet = map[string]struct{}{}
							methodToRoleIDSet[fullMethod] = roleIDSet
						}
						roleIDSet[role.Id] = struct{}{}
					}
				}
				if matchCount == 0 {
					return nil, fmt.Errorf("wildcard permission %q in role %q matches no registered RPC methods", permission, role.Id)
				}
			} else {
				// Exact permission: validate it maps to a registered method.
				if _, ok := fullMethodSet[permission]; !ok {
					return nil, fmt.Errorf("permission %q in role %q does not match any registered RPC method", permission, role.Id)
				}
				roleIDSet, ok := methodToRoleIDSet[permission]
				if !ok {
					roleIDSet = map[string]struct{}{}
					methodToRoleIDSet[permission] = roleIDSet
				}
				roleIDSet[role.Id] = struct{}{}
			}
		}
	}

	// Build skip methods set
	publicMethodSet := make(map[string]struct{}, len(configuration.PublicMethods))
	for _, method := range configuration.PublicMethods {
		publicMethodSet[method] = struct{}{}
	}

	return &PermissionAuthenticationInterceptor{
		sessionManager:    sessionManager,
		methodToRoleIDSet: methodToRoleIDSet,
		publicMethodSet:   publicMethodSet,
	}, nil
}

// getAllPermissionsForRole recursively collects all permissions for a role,
// including permissions from inherited roles
func getAllPermissionsForRole(roleID string, roleIDToRole map[string]*authenticationpb.Role, visited map[string]bool) map[string]struct{} {
	// Prevent infinite loops in case of circular inheritance
	if visited[roleID] {
		return map[string]struct{}{}
	}
	visited[roleID] = true

	role, exists := roleIDToRole[roleID]
	if !exists {
		return map[string]struct{}{}
	}

	permissions := make(map[string]struct{})

	// Add direct permissions
	for _, permission := range role.Permissions {
		permissions[permission] = struct{}{}
	}

	// Add inherited permissions
	for _, inheritedRoleID := range role.InheritedRoleIds {
		inheritedPermissions := getAllPermissionsForRole(inheritedRoleID, roleIDToRole, visited)
		for permission := range inheritedPermissions {
			permissions[permission] = struct{}{}
		}
	}

	return permissions
}

func (i *PermissionAuthenticationInterceptor) authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	if _, ok := i.publicMethodSet[fullMethod]; ok {
		return ctx, nil
	}

	// Grab the session and verify its signature.
	signedSession, err := i.sessionManager.getSignedSessionFromLocalContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, err.Error())
	}
	ok, err := i.sessionManager.verify(signedSession)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "verifying session: %v", err)
	}
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "invalid session signature")
	}
	session := signedSession.Session

	// The session is already authorized, we do not re-check permissions.
	if session.Authorized {
		return ctx, nil
	}

	// Process edge request.
	roleIDSet, ok := i.methodToRoleIDSet[fullMethod]
	if !ok {
		return nil, status.Errorf(codes.PermissionDenied, "no role available for method: %s", fullMethod)
	}

	found := false
	for _, roleID := range session.RoleIds {
		if _, ok := roleIDSet[roleID]; ok {
			found = true
			break
		}
	}
	if !found {
		return nil, status.Errorf(codes.PermissionDenied, "requires %s permission", fullMethod)
	}

	// Set the session to authorized, sign it and store it.
	session.Authorized = true
	signedSession, err = i.sessionManager.sign(session)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "signing session: %v", err)
	}
	isUpdate := true
	return i.sessionManager.injectSignedSessionIntoLocalContext(ctx, signedSession, isUpdate)
}

// Unary implements the authentication unary interceptor.
func (i *PermissionAuthenticationInterceptor) Unary() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := i.authenticate(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, allButHealth)
}

// Stream implements the authentication stream interceptor.
func (i *PermissionAuthenticationInterceptor) Stream() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := i.authenticate(stream.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
	return grpc_selector.StreamServerInterceptor(interceptor, allButHealth)
}

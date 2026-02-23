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
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	coregrpc "github.com/malonaz/core/go/grpc"
)

type PermissionAuthenticationInterceptorOpts struct {
	Config string `long:"config" env:"CONFIG" description:"Path to the authentication configuration file" required:"true"`
}

type PermissionAuthenticationInterceptor struct {
	sessionManager              *SessionManager
	serviceAccountIDToMethodSet map[string]map[string]struct{}
	publicMethodSet             map[string]struct{}
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

	// Build skip methods set
	publicMethodSet := make(map[string]struct{}, len(configuration.PublicMethods))
	for _, method := range configuration.PublicMethods {
		publicMethodSet[method] = struct{}{}
	}

	return &PermissionAuthenticationInterceptor{
		sessionManager:              sessionManager,
		serviceAccountIDToMethodSet: serviceAccountIDToMethodSet,
		publicMethodSet:             publicMethodSet,
	}, nil
}

// getPermissionSetForRole recursively collects all permissions for a role,
// including permissions from inherited roles
func getPermissionSetForRole(roleID string, roleIDToRole map[string]*authenticationpb.Role, visited map[string]bool) (map[string]struct{}, error) {
	// Prevent infinite loops in case of circular inheritance
	if visited[roleID] {
		return nil, nil
	}
	visited[roleID] = true

	role, exists := roleIDToRole[roleID]
	if !exists {
		return nil, fmt.Errorf("unknown role %q", roleID)
	}

	permissions := make(map[string]struct{})

	// Add direct permissions
	for _, permission := range role.Permissions {
		permissions[permission] = struct{}{}
	}

	// Add inherited permissions
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

func (i *PermissionAuthenticationInterceptor) authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	if _, ok := i.publicMethodSet[fullMethod]; ok {
		return ctx, nil
	}

	// Grab the session and verify its signature.
	signedSession, err := getSignedSessionFromLocalContext(ctx)
	if err != nil {
		return nil, coregrpc.Errorf(codes.Unauthenticated, err.Error()).Err()
	}
	ok, err := i.sessionManager.verify(signedSession)
	if err != nil {
		return nil, coregrpc.Errorf(codes.Internal, "verifying session: %v", err).Err()
	}
	if !ok {
		return nil, coregrpc.Errorf(codes.Unauthenticated, "invalid session signature").Err()
	}
	session := signedSession.Session

	// The session is already authorized, we do not re-check permissions.
	if session.Authorized {
		return ctx, nil
	}

	switch identity := session.GetIdentity().(type) {
	case *authenticationpb.Session_ServiceAccountIdentity:
		serviceAccountID := identity.ServiceAccountIdentity.GetServiceAccountId()
		methodSet, ok := i.serviceAccountIDToMethodSet[serviceAccountID]
		if !ok {
			return nil, coregrpc.Errorf(codes.Unauthenticated, "unknown service account %q", serviceAccountID).Err()
		}
		if _, ok := methodSet[fullMethod]; !ok {
			return nil, coregrpc.Errorf(codes.PermissionDenied, "requires permission %q", fullMethod).Err()
		}
	default:
		return nil, coregrpc.Errorf(codes.Unauthenticated, "only service accounts are supported").Err()
	}

	// Set the session to authorized, sign it and store it.
	session.Authorized = true
	signedSession, err = i.sessionManager.sign(session)
	if err != nil {
		return nil, coregrpc.Errorf(codes.Internal, "signing session: %v", err).Err()
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

package authentication

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_interceptors "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
)

type PermissionAuthenticationInterceptorOpts struct {
	Config        string   `long:"config" env:"CONFIG" description:"Path to the authentication configuration file" required:"true"`
	IgnoreMethods []string `long:"skip-methods" description:"some methods to skip permission checks for"`
}

type PermissionAuthenticationInterceptor struct {
	sessionManager        *SessionManager
	permissionToRoleIDSet map[string]map[string]struct{}
	ignoreMethodSet       map[string]struct{}
}

var (
	allButHealth = grpc_selector.MatchFunc(func(ctx context.Context, callMeta grpc_interceptors.CallMeta) bool {
		return grpc_health_v1.Health_ServiceDesc.ServiceName != callMeta.Service
	})
)

func NewPermissionAuthenticationInterceptor(
	opts *PermissionAuthenticationInterceptorOpts,
	sessionManager *SessionManager,
) (*PermissionAuthenticationInterceptor, error) {
	configuration := &authenticationpb.RoleConfiguration{}
	if err := parseConfig(opts.Config, configuration); err != nil {
		return nil, err
	}

	roleIDToRole := map[string]*authenticationpb.Role{}
	for _, role := range configuration.Roles {
		roleIDToRole[role.Id] = role
	}

	// Build permission map with role inheritance
	permissionToRoleIDSet := map[string]map[string]struct{}{}

	// For each role, get all its permissions (including inherited ones)
	for _, role := range configuration.Roles {
		allPermissions := getAllPermissionsForRole(role.Id, roleIDToRole, make(map[string]bool))

		// Map each permission to this role
		for permission := range allPermissions {
			roleIDSet, ok := permissionToRoleIDSet[permission]
			if !ok {
				roleIDSet = map[string]struct{}{}
				permissionToRoleIDSet[permission] = roleIDSet
			}
			roleIDSet[role.Id] = struct{}{}
		}
	}

	// Build skip methods set
	ignoreMethodSet := make(map[string]struct{}, len(opts.IgnoreMethods))
	for _, method := range opts.IgnoreMethods {
		ignoreMethodSet[method] = struct{}{}
	}

	return &PermissionAuthenticationInterceptor{
		sessionManager:        sessionManager,
		permissionToRoleIDSet: permissionToRoleIDSet,
		ignoreMethodSet:       ignoreMethodSet,
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
	if _, ok := i.ignoreMethodSet[fullMethod]; ok {
		return ctx, nil
	}

	// Grab the session and verify its signature.
	signedSession, err := i.sessionManager.getSignedSessionFromLocalContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, err.Error())
	}
	ok, err := i.sessionManager.verify(signedSession)
	if err != nil {
		return nil, err
	}
	session := signedSession.Session

	// The session is already authorized, we do not re-check permissions.
	if session.Authorized {
		return ctx, nil
	}

	// Process edge request.
	permission := fullMethod // We use method names as permissions.
	roleIDSet, ok := i.permissionToRoleIDSet[permission]
	if !ok {
		return nil, status.Errorf(codes.PermissionDenied, "no role available for permission: %s", permission)
	}

	found := false
	for _, roleID := range session.RoleIds {
		if _, ok := roleIDSet[roleID]; ok {
			found = true
			break
		}
	}
	if !found {
		return nil, status.Errorf(codes.PermissionDenied, "requires %s permission", permission)
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

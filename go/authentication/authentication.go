package authentication

import (
	"context"
	"fmt"
	"os"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_interceptors "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	authenticationpb "github.com/malonaz/core/genproto/authentication"
	"github.com/malonaz/core/go/jsonnet"
	"github.com/malonaz/core/go/pbutil"
)

type Opts struct {
	Config string `long:"config" env:"CONFIG" description:"Path to the authentication configuration file" required:"true"`
}

type Interceptor struct {
	permissionToRoleIDSet map[string]map[string]struct{}
}

var (
	allButHealth = grpc_selector.MatchFunc(func(ctx context.Context, callMeta grpc_interceptors.CallMeta) bool {
		return grpc_health_v1.Health_ServiceDesc.ServiceName != callMeta.Service
	})
)

func NewInterceptor(opts *Opts) (*Interceptor, error) {
	// Parse the configuration file
	bytes, err := os.ReadFile(opts.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", opts.Config, err)
	}
	bytes, err = jsonnet.EvaluateSnippet(string(bytes))
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate config file %s: %w", opts.Config, err)
	}
	configuration := &authenticationpb.Configuration{}
	if err := pbutil.JSONUnmarshal(bytes, configuration); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", opts.Config, err)
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

	return &Interceptor{
		permissionToRoleIDSet: permissionToRoleIDSet,
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

func (i *Interceptor) authenticate(ctx context.Context, fullMethod string) error {
	session, err := GetSession(ctx)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "getting session: %v", err)
	}

	permission := fullMethod // We use method names as permissions.
	roleIDSet, ok := i.permissionToRoleIDSet[permission]
	if !ok {
		return status.Errorf(codes.PermissionDenied, "no role available for permission: %s", permission)
	}

	found := false
	for _, roleID := range session.RoleIds {
		if _, ok := roleIDSet[roleID]; ok {
			found = true
			break
		}
	}
	if !found {
		return status.Errorf(codes.PermissionDenied, "missing permissions")
	}
	return nil
}

// Unary implements the authentication unary interceptor.
func (i *Interceptor) Unary() grpc.UnaryServerInterceptor {
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := i.authenticate(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
	return grpc_selector.UnaryServerInterceptor(interceptor, allButHealth)
}

// StreamInterceptor implements the authentication stream interceptor.
func (i *Interceptor) Stream() grpc.StreamServerInterceptor {
	interceptor := func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		if err := i.authenticate(ctx, info.FullMethod); err != nil {
			return err
		}
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
	return grpc_selector.StreamServerInterceptor(interceptor, allButHealth)
}

package authentication

import (
	"context"
	"fmt"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_interceptors "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	authenticationpb "github.com/malonaz/core/proto/authentication"
)

type Interceptor struct {
	roleIDToRole                     map[string]*authenticationpb.Role
	methodNameToCompiledRequirements map[string]*CompiledRequirements
}

var (
	allButHealth = grpc_selector.MatchFunc(func(ctx context.Context, callMeta grpc_interceptors.CallMeta) bool {
		return grpc_health_v1.Health_ServiceDesc.ServiceName != callMeta.Service
	})
)

type CompiledRequirements struct {
	*authenticationpb.Requirements
	anyPermissionSet map[string]struct{}
	anyRoleIDSet     map[string]struct{}
}

func NewInterceptor(roles []*authenticationpb.Role, fileDescriptors ...protoreflect.FileDescriptor) (*Interceptor, error) {
	roleIDToRole := map[string]*authenticationpb.Role{}
	for _, role := range roles {
		roleIDToRole[role.Id] = role
	}

	methodNameToCompiledRequirements := map[string]*CompiledRequirements{}
	for _, fileDescriptor := range fileDescriptors {
		services := fileDescriptor.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			methods := service.Methods()
			for j := 0; j < methods.Len(); j++ {
				method := methods.Get(j)
				methodName := fmt.Sprintf("/%s.%s/%s", fileDescriptor.Package(), service.Name(), method.Name())

				// Get requirementss.
				requirementsExtensionInfo := authenticationpb.E_Requirements
				requirementsExtension := proto.GetExtension(method.Options(), requirementsExtensionInfo)
				// Every method must set requirementss.
				if !proto.HasExtension(method.Options(), requirementsExtensionInfo) {
					return nil, fmt.Errorf("%s missing requirements option", methodName)
				}
				requirements, ok := requirementsExtension.(*authenticationpb.Requirements)
				if !ok {
					return nil, fmt.Errorf("unexpected extension type %T", requirementsExtension)
				}

				compiledRequirement := &CompiledRequirements{
					Requirements:     requirements,
					anyPermissionSet: make(map[string]struct{}),
					anyRoleIDSet:     make(map[string]struct{}),
				}

				for _, permission := range requirements.AnyPermissions {
					compiledRequirement.anyPermissionSet[permission] = struct{}{}
				}
				for _, roleID := range requirements.AnyRoleIds {
					compiledRequirement.anyRoleIDSet[roleID] = struct{}{}
				}

				methodNameToCompiledRequirements[methodName] = compiledRequirement
			}
		}
	}
	return &Interceptor{
		methodNameToCompiledRequirements: methodNameToCompiledRequirements,
		roleIDToRole:                     roleIDToRole,
	}, nil
}

func (i *Interceptor) authenticate(ctx context.Context, fullMethod string) error {
	requirements, ok := i.methodNameToCompiledRequirements[fullMethod]
	if !ok {
		return status.Errorf(codes.Internal, "no requirements defined")
	}

	session, err := GetSession(ctx)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "getting session: %v", err)
	}

	userPermissionSet, err := i.getUserPermissionSet(session)
	if err != nil {
		return status.Errorf(codes.Internal, "getting user permissions: %v", err)
	}
	userRoleIDSet := i.getUserRoleIDSet(session)

	// Check required permissions - ALL must be present
	for _, requiredPermission := range requirements.RequiredPermissions {
		if _, hasPermission := userPermissionSet[requiredPermission]; !hasPermission {
			return status.Errorf(codes.PermissionDenied, "missing required permission: %s", requiredPermission)
		}
	}

	// Check required roles - ALL must be present
	for _, requiredRoleID := range requirements.RequiredRoleIds {
		if _, ok := userRoleIDSet[requiredRoleID]; !ok {
			return status.Errorf(codes.PermissionDenied, "missing required role: %s", requiredRoleID)
		}
	}

	// Check any permissions - at least ONE must be present
	if len(requirements.anyPermissionSet) > 0 {
		foundAnyPermission := false
		for userPermission := range userPermissionSet {
			if _, ok := requirements.anyPermissionSet[userPermission]; ok {
				foundAnyPermission = true
				break
			}
		}
		if !foundAnyPermission {
			return status.Errorf(codes.PermissionDenied, "missing any of required permissions")
		}
	}

	// Check any roles - at least ONE must be present
	if len(requirements.anyRoleIDSet) > 0 {
		foundAnyRole := false
		for userRoleID := range userRoleIDSet {
			if _, ok := requirements.anyRoleIDSet[userRoleID]; ok {
				foundAnyRole = true
				break
			}
		}
		if !foundAnyRole {
			return status.Errorf(codes.PermissionDenied, "missing any of required roles")
		}
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

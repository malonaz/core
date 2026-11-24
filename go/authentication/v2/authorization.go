package authentication

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/grpc/status"
)

// AuthorizationService serves malonaz.authentication.v2.AuthorizationService for the services a gRPC server serves,
// from the interceptor's configuration. It must be served behind that interceptor, whose session it answers for.
type AuthorizationService struct {
	authenticationpb.UnimplementedAuthorizationServiceServer
	interceptor *Interceptor
	services    serviceInfoProvider
}

// serviceInfoProvider tells the services a server serves, as *grpc.Server does.
type serviceInfoProvider interface {
	GetServiceInfo() map[string]grpc.ServiceInfo
}

// NewAuthorizationService returns the service answering for the services the server registers.
func (i *Interceptor) NewAuthorizationService(services serviceInfoProvider) *AuthorizationService {
	return &AuthorizationService{interceptor: i, services: services}
}

// TestMethods implements malonaz.authentication.v2.AuthorizationService.
func (s *AuthorizationService) TestMethods(ctx context.Context, request *authenticationpb.TestMethodsRequest) (*authenticationpb.TestMethodsResponse, error) {
	session, err := GetSession(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reading the session: %v", err).Err()
	}
	serviceToInfo := s.services.GetServiceInfo()
	response := &authenticationpb.TestMethodsResponse{}
	for _, method := range request.GetMethods() {
		service, name, _ := strings.Cut(strings.TrimPrefix(method, "/"), "/")
		if !served(serviceToInfo[service], name) {
			continue
		}
		if s.interceptor.reachable(session, method) {
			response.Methods = append(response.Methods, method)
		}
	}
	return response, nil
}

// served reports whether a service serves a method.
func served(info grpc.ServiceInfo, name string) bool {
	for _, method := range info.Methods {
		if method.Name == name {
			return true
		}
	}
	return false
}

// reachable reports whether some call of the method could be allowed the session: a grant of its principal, or a
// public role, lists it over a scope that can cover the resources its calls name, under a rule whose condition reads
// the request or holds for the session alone. It never decides what authorize would allow.
func (i *Interceptor) reachable(session *authenticationpb.Session, method string) bool {
	activation := conditionActivation(session, nil)
	for _, grants := range [][]*authenticationpb.Grant{session.GetGrants(), i.publicGrants} {
		for _, grant := range grants {
			conditions := i.roleIDToMethodToConditions[grant.GetRole()][method]
			if len(conditions) == 0 {
				continue
			}
			if scope := grant.GetScope(); scope != wildcardScope && (!validScope(scope) || i.methodToResources[method] == nil) {
				continue
			}
			for _, condition := range conditions {
				if condition == nil || condition.readsRequest {
					return true
				}
				if allowed, err := evaluateCondition(condition.program, activation); err == nil && allowed {
					return true
				}
			}
		}
	}
	return false
}

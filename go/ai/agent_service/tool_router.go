package agent_service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

// toolSchemaTTL bounds how long a target's reflected schema is reused: a redeployed target may change its API.
const toolSchemaTTL = 5 * time.Minute

// toolRouter routes the tool sets of gRPC services to the endpoints serving them, their configured targets.
type toolRouter struct {
	serviceToEndpoint map[string]string
	connections       *connections
}

func newToolRouter(serviceToEndpoint map[string]string, connections *connections) (*toolRouter, error) {
	for service, endpoint := range serviceToEndpoint {
		if service == "" {
			return nil, fmt.Errorf("tool target %q names no service", endpoint)
		}
		if service == genuiToolSet {
			return nil, fmt.Errorf("tool target %q: %s is the genui components' tool set", endpoint, genuiToolSet)
		}
		if _, err := grpc.ParseClientOpts(endpoint); err != nil {
			return nil, fmt.Errorf("parsing the endpoint of tool service %s: %w", service, err)
		}
	}
	return &toolRouter{serviceToEndpoint: serviceToEndpoint, connections: connections}, nil
}

// schema returns the schema an endpoint serves, resolved through its gRPC server reflection on ctx.
func (r *toolRouter) schema(ctx context.Context, endpoint string) (*pbreflection.Schema, error) {
	connection, err := r.connections.get(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	reflectionClient := grpc_reflection_v1.NewServerReflectionClient(connection.Get())
	schema, err := pbreflection.ResolveSchema(ctx, reflectionClient, pbreflection.WithMemCache("agent-service-tools:"+endpoint, toolSchemaTTL))
	if err != nil {
		return nil, fmt.Errorf("resolving the schema served at %s: %w", endpoint, err)
	}
	return schema, nil
}

// service returns a routed service's descriptor and the schema holding it, read from its target as ctx.
func (r *toolRouter) service(ctx context.Context, service string) (protoreflect.ServiceDescriptor, *pbreflection.Schema, error) {
	endpoint, ok := r.serviceToEndpoint[service]
	if !ok {
		return nil, nil, status.Errorf(codes.InvalidArgument, "no tool target configured for tool set %s: add --tool-target for it", service).Err()
	}
	schema, err := r.schema(ctx, endpoint)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "reading tool set %s: %v", service, err).Err()
	}
	descriptor, err := schema.FindDescriptorByName(protoreflect.FullName(service))
	serviceDescriptor, ok := descriptor.(protoreflect.ServiceDescriptor)
	if err != nil || !ok {
		return nil, nil, status.Errorf(codes.Unavailable, "%s serves no service %s", endpoint, service).Err()
	}
	return serviceDescriptor, schema, nil
}

// invoke calls a method of a routed service on ctx.
func (r *toolRouter) invoke(ctx context.Context, method protoreflect.MethodDescriptor, request proto.Message) (proto.Message, error) {
	endpoint := r.serviceToEndpoint[string(method.Parent().FullName())]
	connection, err := r.connections.get(ctx, endpoint)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connecting to %s: %v", endpoint, err).Err()
	}
	return pbreflection.NewMethodInvoker(connection.Get()).Invoke(ctx, method, request)
}

// testMethods asks a routed service's target which of the named methods of the service the session ctx carries could
// call (AuthorizationService.TestMethods), returning their names.
func (r *toolRouter) testMethods(ctx context.Context, service protoreflect.ServiceDescriptor, names []string) ([]string, error) {
	endpoint := r.serviceToEndpoint[string(service.FullName())]
	connection, err := r.connections.get(ctx, endpoint)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connecting to %s: %v", endpoint, err).Err()
	}
	prefix := "/" + string(service.FullName()) + "/"
	testMethodsRequest := &authenticationpb.TestMethodsRequest{}
	for _, name := range names {
		testMethodsRequest.Methods = append(testMethodsRequest.Methods, prefix+name)
	}
	testMethodsResponse, err := authenticationpb.NewAuthorizationServiceClient(connection.Get()).TestMethods(ctx, testMethodsRequest)
	if err != nil {
		return nil, err
	}
	reachable := make([]string, 0, len(testMethodsResponse.GetMethods()))
	for _, method := range testMethodsResponse.GetMethods() {
		reachable = append(reachable, strings.TrimPrefix(method, prefix))
	}
	return reachable, nil
}

// unaryMethod returns a service's method by name if it is a tool: unary.
func unaryMethod(service protoreflect.ServiceDescriptor, name string) (protoreflect.MethodDescriptor, bool) {
	method := service.Methods().ByName(protoreflect.Name(name))
	if method == nil || method.IsStreamingClient() || method.IsStreamingServer() {
		return nil, false
	}
	return method, true
}

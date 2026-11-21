package agent_service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

// toolSchemaTTL bounds how long a target's reflected schema is reused: a redeployed target may change its API.
const toolSchemaTTL = 5 * time.Minute

// toolRouter routes tool services to the endpoints serving them, their configured targets. A tool whose service has
// no target is refused.
type toolRouter struct {
	serviceToEndpoint map[string]string
	connections       *connections
}

func newToolRouter(serviceToEndpoint map[string]string, connections *connections) (*toolRouter, error) {
	for service, endpoint := range serviceToEndpoint {
		if service == "" {
			return nil, fmt.Errorf("tool target %q names no service", endpoint)
		}
		if _, err := grpc.ParseClientOpts(endpoint); err != nil {
			return nil, fmt.Errorf("parsing the endpoint of tool service %s: %w", service, err)
		}
	}
	return &toolRouter{serviceToEndpoint: serviceToEndpoint, connections: connections}, nil
}

// endpoint returns the endpoint serving a tool: a service, or one of its methods.
func (r *toolRouter) endpoint(tool string) (string, error) {
	if endpoint, ok := r.serviceToEndpoint[tool]; ok {
		return endpoint, nil
	}
	if i := strings.LastIndex(tool, "."); i > 0 {
		if endpoint, ok := r.serviceToEndpoint[tool[:i]]; ok {
			return endpoint, nil
		}
	}
	return "", status.Errorf(codes.InvalidArgument, "no tool target configured for tool %s: add --tool-target for its service", tool).Err()
}

// validate checks that the service of every RPC tool of a spec has a target; genui components need none.
func (r *toolRouter) validate(spec *agentpb.AgentSpec, genuiComponents *genuiComponents) error {
	for _, tool := range spec.GetTools() {
		if _, ok := genuiComponents.nameToComponent[tool.GetName()]; ok {
			continue
		}
		if _, err := r.endpoint(tool.GetName()); err != nil {
			return err
		}
	}
	return nil
}

// schema returns the schema an endpoint serves, resolved through its gRPC server reflection with the credentials of
// ctx alone.
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

// invoke calls a method at an endpoint, with the credentials of ctx alone: the service's own never reach a tool
// target.
func (r *toolRouter) invoke(ctx context.Context, endpoint string, method protoreflect.MethodDescriptor, request proto.Message) (proto.Message, error) {
	connection, err := r.connections.get(ctx, endpoint)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connecting to %s: %v", endpoint, err).Err()
	}
	return pbreflection.NewMethodInvoker(connection.Get()).Invoke(ctx, method, request)
}

// withoutIdentity strips a call's outgoing metadata, the service's own identity included: tool targets are never
// trusted with it. Cancellation and deadlines are kept.
func withoutIdentity(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.MD{})
}

// toolSchema returns the schema a tool target serves, resolved as the service itself.
func (s *Service) toolSchema(ctx context.Context, endpoint string) (*pbreflection.Schema, error) {
	ctx, cancel, err := s.asSelf(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return s.toolRouter.schema(ctx, endpoint)
}

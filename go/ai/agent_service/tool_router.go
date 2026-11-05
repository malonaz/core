package agent_service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"

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

// validate checks that the service of every tool of a spec has a target.
func (r *toolRouter) validate(spec *agentpb.AgentSpec) error {
	for _, tool := range spec.GetTools() {
		if _, err := r.endpoint(tool); err != nil {
			return err
		}
	}
	return nil
}

// schema returns the schema an endpoint serves, resolved through its gRPC server reflection.
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

// invoker returns the method invoker of an endpoint.
func (r *toolRouter) invoker(ctx context.Context, endpoint string) (*pbreflection.MethodInvoker, error) {
	connection, err := r.connections.get(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return pbreflection.NewMethodInvoker(connection.Get()), nil
}

package agent_service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/reflection/grpc_reflection_v1"

	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

// toolSchemaTTL bounds how long a target's reflected schema is reused: a redeployed target may change its API.
const toolSchemaTTL = 5 * time.Minute

// toolRouter routes tool services to the endpoints serving them: their configured target, else this replica's own
// server, where co-hosted services live.
type toolRouter struct {
	serviceToEndpoint map[string]string
	defaultEndpoint   string
	connections       *connections
}

func newToolRouter(serviceToEndpoint map[string]string, defaultEndpoint string, connections *connections) (*toolRouter, error) {
	for service, endpoint := range serviceToEndpoint {
		if service == "" {
			return nil, fmt.Errorf("tool target %q names no service", endpoint)
		}
		if _, err := grpc.ParseClientOpts(endpoint); err != nil {
			return nil, fmt.Errorf("parsing the endpoint of tool service %s: %w", service, err)
		}
	}
	return &toolRouter{serviceToEndpoint: serviceToEndpoint, defaultEndpoint: defaultEndpoint, connections: connections}, nil
}

// endpoint returns the endpoint serving a service.
func (r *toolRouter) endpoint(service string) string {
	if endpoint, ok := r.serviceToEndpoint[service]; ok {
		return endpoint
	}
	return r.defaultEndpoint
}

// locate returns the endpoint serving a tool: a service, or one of its methods.
func (r *toolRouter) locate(tool string) string {
	if endpoint, ok := r.serviceToEndpoint[tool]; ok {
		return endpoint
	}
	if i := strings.LastIndex(tool, "."); i > 0 {
		return r.endpoint(tool[:i])
	}
	return r.defaultEndpoint
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

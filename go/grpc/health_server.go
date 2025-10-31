package grpc

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/malonaz/core/go/health"
)

// HealthServer implements the gRPC health checking protocol.
// It maintains per-service health checks and performs them on-demand.
type HealthServer struct {
	grpc_health_v1.UnimplementedHealthServer

	// serviceNameToHealthCheck maps service names to their health check functions
	serviceNameToHealthCheck map[string]health.Check
}

// newHealthServer creates a new HealthServer instance.
func newHealthServer() *HealthServer {
	return &HealthServer{
		serviceNameToHealthCheck: map[string]health.Check{},
	}
}

// RegisterService registers health checks for a specific service.
// serviceName should match the fully qualified gRPC service name (e.g., "myapp.v1.UserService").
// Use an empty string "" to register checks for the overall server health.
func (h *HealthServer) RegisterService(serviceName string, checks []health.Check) {
	h.serviceNameToHealthCheck[serviceName] = health.Checks(checks...)
	log.Debugf("Registered %d health check(s) for service: %s", len(checks), serviceName)
}

// Check performs health checks for the specified service.
func (h *HealthServer) check(ctx context.Context, service string) (grpc_health_v1.HealthCheckResponse_ServingStatus, error) {
	healthCheck, ok := h.serviceNameToHealthCheck[service]
	if !ok {
		return grpc_health_v1.HealthCheckResponse_UNKNOWN, status.Error(codes.NotFound, "service not found")
	}
	if err := healthCheck(ctx); err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			log.Debugf("Health check cancelled for service [%s]: %v", service, err)
			return grpc_health_v1.HealthCheckResponse_UNKNOWN, nil
		case errors.Is(err, context.DeadlineExceeded):
			log.Warnf("Health check timed out for service [%s]: %v", service, err)
			return grpc_health_v1.HealthCheckResponse_UNKNOWN, nil
		default:
			log.Warnf("Health check failed for service [%s]: %v", service, err)
			return grpc_health_v1.HealthCheckResponse_NOT_SERVING, nil
		}
	}
	return grpc_health_v1.HealthCheckResponse_SERVING, nil
}

func (h *HealthServer) Check(ctx context.Context, request *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if request.Service != "" {
		status, err := h.check(ctx, request.Service)
		return &grpc_health_v1.HealthCheckResponse{Status: status}, err
	}

	// Leverage the List endpoint.
	healthListRequest := &grpc_health_v1.HealthListRequest{}
	healthListResponse, err := h.List(ctx, healthListRequest)
	if err != nil {
		// List can never return an error.
		return nil, err
	}
	// Return the worse response (NOT_SERVING).
	status := grpc_health_v1.HealthCheckResponse_SERVING
	for _, healthCheckResponse := range healthListResponse.Statuses {
		if healthCheckResponse.Status == grpc_health_v1.HealthCheckResponse_UNKNOWN {
			status = healthCheckResponse.Status
		}
		if healthCheckResponse.Status == grpc_health_v1.HealthCheckResponse_NOT_SERVING {
			status = healthCheckResponse.Status
			break
		}
	}
	return &grpc_health_v1.HealthCheckResponse{Status: status}, err
}

// Watch implements streaming health checks (currently unimplemented).
func (h *HealthServer) Watch(req *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "method Watch not implemented")
}

// List returns all registered services with their current health status.
// Health checks are performed in parallel for better performance.
func (h *HealthServer) List(ctx context.Context, req *grpc_health_v1.HealthListRequest) (*grpc_health_v1.HealthListResponse, error) {
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(h.serviceNameToHealthCheck))

	statuses := make(map[string]*grpc_health_v1.HealthCheckResponse, len(h.serviceNameToHealthCheck))
	for serviceName := range h.serviceNameToHealthCheck {
		serviceName := serviceName
		go func() {
			defer wg.Done()
			request := &grpc_health_v1.HealthCheckRequest{Service: serviceName}
			response, err := h.Check(ctx, request)
			if err != nil {
				response = &grpc_health_v1.HealthCheckResponse{
					Status: grpc_health_v1.HealthCheckResponse_UNKNOWN,
				}
			}
			mutex.Lock()
			statuses[serviceName] = response
			mutex.Unlock()
		}()
	}

	wg.Wait()
	return &grpc_health_v1.HealthListResponse{
		Statuses: statuses,
	}, nil
}

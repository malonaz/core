package mockserver

import (
	"context"

	"google.golang.org/grpc/health/grpc_health_v1"
)

// healthServer reports every service as serving. The mock has no real dependencies to check,
// and its consumers only need the probe to succeed.
type healthServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (healthServer) Check(
	ctx context.Context,
	healthCheckRequest *grpc_health_v1.HealthCheckRequest,
) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (healthServer) Watch(
	healthCheckRequest *grpc_health_v1.HealthCheckRequest,
	stream grpc_health_v1.Health_WatchServer,
) error {
	healthCheckResponse := &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}
	if err := stream.Send(healthCheckResponse); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

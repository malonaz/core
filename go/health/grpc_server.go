package health

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Opts holds health opts.
type GRPCOpts struct {
	IntervalSeconds int `long:"interval-seconds" description:"Health check interval in seconds" default:"10"`
	TimeoutSeconds  int `long:"timeout-seconds" description:"Health check timeout in seconds" default:"30"`
}

// Server holds the health check server state and embeds gRPC health server.
type GRPCServer struct {
	*health.Server
	opts                     *GRPCOpts
	serviceNameToHealthCheck map[string]Check
	shutdownChan             chan struct{}
}

// NewGRPCServer creates a new health check server.
func NewGRPCServer(opts *GRPCOpts) *GRPCServer {
	return &GRPCServer{
		Server:                   health.NewServer(),
		opts:                     opts,
		serviceNameToHealthCheck: make(map[string]Check),
		shutdownChan:             make(chan struct{}),
	}
}

// RegisterService registers health checks for a specific service.
// serviceName should match the fully qualified gRPC service name (e.g., "myapp.v1.UserService").
// Use an empty string "" to register checks for the overall server health.
func (s *GRPCServer) RegisterService(serviceName string, checks ...Check) {
	s.serviceNameToHealthCheck[serviceName] = CombineChecks(checks...)
	s.SetServingStatus(serviceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	log.Debugf("Registered %d health check(s) for service: %s", len(checks), serviceName)
}

// Check performs health checks for the specified service, implementing the gRPC health interface.
func (s *GRPCServer) Start(ctx context.Context) {
	// the grpc/health instantiates this as 'serving' so we change it here.
	s.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	go s.updateHealthPeriodically(ctx)
}

func (s *GRPCServer) Shutdown() {
	close(s.shutdownChan)
	s.Server.Shutdown()
}

func (s *GRPCServer) updateHealthPeriodically(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.opts.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.shutdownChan:
			return
		case <-ticker.C:
			mutex := sync.Mutex{}
			wg := sync.WaitGroup{}
			wg.Add(len(s.serviceNameToHealthCheck))
			statuses := make([]grpc_health_v1.HealthCheckResponse_ServingStatus, 0, len(s.serviceNameToHealthCheck))
			for serviceName := range s.serviceNameToHealthCheck {
				request := &grpc_health_v1.HealthCheckRequest{Service: serviceName}
				go func() {
					defer wg.Done()
					var status grpc_health_v1.HealthCheckResponse_ServingStatus
					response, err := s.checkService(ctx, request)
					if err != nil {
						switch {
						case errors.Is(err, context.Canceled):
							log.Debugf("Health check cancelled for service [%s]: %v", request.Service, err)
							status = grpc_health_v1.HealthCheckResponse_UNKNOWN
						case errors.Is(err, context.DeadlineExceeded):
							log.Warnf("Health check timed out for service [%s]: %v", request.Service, err)
							status = grpc_health_v1.HealthCheckResponse_UNKNOWN
						default:
							log.Warnf("Health check failed for service [%s]: %v", request.Service, err)
							status = grpc_health_v1.HealthCheckResponse_NOT_SERVING
						}
					} else {
						status = response.Status
					}
					s.SetServingStatus(request.Service, status)
					mutex.Lock()
					statuses = append(statuses, status)
					mutex.Unlock()
				}()
			}

			// Update "" state.
			go func() {
				wg.Wait()
				worstStatus := grpc_health_v1.HealthCheckResponse_SERVING
				for _, status := range statuses {
					if status == grpc_health_v1.HealthCheckResponse_NOT_SERVING {
						worstStatus = grpc_health_v1.HealthCheckResponse_NOT_SERVING
						break
					} else if status == grpc_health_v1.HealthCheckResponse_UNKNOWN {
						worstStatus = grpc_health_v1.HealthCheckResponse_UNKNOWN
					}
				}
				s.SetServingStatus("", worstStatus)
			}()
		}
	}
}

func (s *GRPCServer) checkService(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	healthCheck, ok := s.serviceNameToHealthCheck[req.Service]
	if !ok {
		return nil, status.Error(codes.NotFound, "service not found")
	}

	checkCtx, cancel := context.WithTimeout(ctx, time.Duration(s.opts.TimeoutSeconds)*time.Second)
	defer cancel()

	status := grpc_health_v1.HealthCheckResponse_SERVING
	if err := healthCheck(checkCtx); err != nil {
		status = grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}
	return &grpc_health_v1.HealthCheckResponse{Status: status}, nil
}

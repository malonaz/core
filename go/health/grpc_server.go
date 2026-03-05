package health

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/malonaz/core/go/grpc/status"
)

const (
	minBackoff       = 1 * time.Second
	backoffIncrement = 100 * time.Millisecond
)

// GRPCOpts holds configuration for the gRPC health check server.
type GRPCOpts struct {
	Interval time.Duration `long:"interval" description:"Health check interval in seconds" default:"5s"`
	Timeout  time.Duration `long:"timeout" description:"Health check timeout in seconds" default:"30s"`
	Startup  time.Duration `long:"startup" description:"During this interval, we do not log failed health checks" default:"30s"`
}

// GRPCServer manages health checking for registered gRPC services.
// It embeds the standard gRPC health server and runs a dedicated monitoring
// goroutine per service. When a service becomes unhealthy, its check interval
// backs off exponentially (starting at minBackoff, capped at IntervalSeconds)
// to avoid overwhelming failing dependencies. On recovery, the interval resets
// to IntervalSeconds. An overall "" (empty) service status is maintained as the
// worst status across all services.
type GRPCServer struct {
	*health.Server
	name                     string
	opts                     *GRPCOpts
	log                      *slog.Logger
	serviceNameToHealthCheck map[string]Check
	shutdownChan             chan struct{}
	startupCompleted         bool
}

// NewGRPCServer creates a new health check server with the given options.
func NewGRPCServer(opts *GRPCOpts, name string) *GRPCServer {
	return &GRPCServer{
		Server:                   health.NewServer(),
		name:                     name,
		opts:                     opts,
		log:                      slog.Default(),
		serviceNameToHealthCheck: make(map[string]Check),
		shutdownChan:             make(chan struct{}),
	}
}

// WithLogger sets a custom logger and returns the server for chaining.
func (s *GRPCServer) WithLogger(logger *slog.Logger) *GRPCServer {
	s.log = logger
	return s
}

// RegisterService registers health checks for a specific service.
// serviceName should match the fully qualified gRPC service name (e.g., "myapp.v1.UserService").
// The service is initially marked as NOT_SERVING until the first successful check.
func (s *GRPCServer) RegisterService(serviceName string, checks ...Check) {
	s.serviceNameToHealthCheck[serviceName] = CombineChecks(checks...)
	s.SetServingStatus(serviceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	s.log.Debug("registered health check", "service", serviceName, "checks", len(checks))
}

// Start initializes the overall server status to NOT_SERVING and launches
// background health monitoring goroutines — one per registered service.
func (s *GRPCServer) Start(ctx context.Context) {
	s.log = s.log.WithGroup("health_grpc_server").With("name", s.name, "interval", s.opts.Interval, "timeout", s.opts.Timeout)
	s.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// Start a go-routine that cancels the context when Shutdown() is called.
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		select {
		case <-ctx.Done():
			return
		case <-s.shutdownChan:
			return
		}
	}()

	s.updateHealthPeriodically(ctx)
}

// Shutdown signals all monitoring goroutines to stop and shuts down the
// underlying gRPC health server, setting all services to NOT_SERVING.
func (s *GRPCServer) Shutdown() {
	close(s.shutdownChan)
	s.Server.Shutdown()
}

// updateHealthPeriodically spawns one goroutine per registered service, each
// with a random initial delay to stagger checks and avoid thundering herd.
// Each goroutine independently polls its service's health check. On success,
// it waits the full IntervalSeconds before the next check. On failure, it
// uses exponential backoff (minBackoff -> 2s -> 4s -> ... capped at IntervalSeconds)
// to retry more aggressively. After each check, the overall "" status is
// recomputed as the worst across all services, and a log line is emitted
// when all services transition to healthy.
func (s *GRPCServer) updateHealthPeriodically(ctx context.Context) {
	mutex := sync.Mutex{}
	serviceNameToStatus := make(map[string]grpc_health_v1.HealthCheckResponse_ServingStatus, len(s.serviceNameToHealthCheck))
	var allHealthy bool

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.opts.Startup):
			s.startupCompleted = true
		}
	}()

	// updateOverallStatus recomputes the aggregate "" status from individual
	// service statuses. NOT_SERVING takes priority over UNKNOWN, which takes
	// priority over SERVING. Logs once when transitioning to all-healthy.
	updateOverallStatus := func() {
		mutex.Lock()
		defer mutex.Unlock()

		worstStatus := grpc_health_v1.HealthCheckResponse_SERVING
		for _, servingStatus := range serviceNameToStatus {
			if servingStatus == grpc_health_v1.HealthCheckResponse_NOT_SERVING {
				worstStatus = grpc_health_v1.HealthCheckResponse_NOT_SERVING
				break
			} else if servingStatus == grpc_health_v1.HealthCheckResponse_UNKNOWN {
				worstStatus = grpc_health_v1.HealthCheckResponse_UNKNOWN
			}
		}
		s.SetServingStatus("", worstStatus)
		nowHealthy := worstStatus == grpc_health_v1.HealthCheckResponse_SERVING && len(serviceNameToStatus) == len(s.serviceNameToHealthCheck)
		if nowHealthy && !allHealthy {
			s.log.Info("all services are healthy")
		}
		allHealthy = nowHealthy
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(minBackoff):
			}
			updateOverallStatus()
		}
	}()

	for serviceName := range s.serviceNameToHealthCheck {
		// Random initial delay in [0, interval) to stagger checks across services.
		initialDelay := time.Duration(rand.Int64N(int64(s.opts.Interval)))

		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(initialDelay):
			}

			backoff := s.opts.Interval

			for {
				// Run the health check for this service.
				request := &grpc_health_v1.HealthCheckRequest{Service: serviceName}
				var servingStatus grpc_health_v1.HealthCheckResponse_ServingStatus
				response, err := s.checkService(ctx, request)
				if err != nil {
					servingStatus = grpc_health_v1.HealthCheckResponse_UNKNOWN
					if s.startupCompleted {
						log := s.log.With("service", serviceName, "error", err)
						switch {
						case errors.Is(err, context.Canceled):
							log.WarnContext(ctx, "health check cancelled")
						case errors.Is(err, context.DeadlineExceeded):
							log.WarnContext(ctx, "health check timed out")
						default:
							log.WarnContext(ctx, "health check failed")
						}
					}
				} else {
					servingStatus = response.Status
				}

				// Publish this service's status and recompute overall health.
				s.SetServingStatus(serviceName, servingStatus)
				s.recordMetrics(serviceName, servingStatus)
				mutex.Lock()
				serviceNameToStatus[serviceName] = servingStatus
				mutex.Unlock()

				// Healthy: wait full interval. Unhealthy: exponential backoff
				// starting at minBackoff, doubling each iteration, capped at interval.
				if servingStatus == grpc_health_v1.HealthCheckResponse_SERVING {
					backoff = s.opts.Interval
				} else {
					backoff = min(backoff+backoffIncrement, s.opts.Interval)
				}

				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
			}
		}()
	}
}

// recordMetrics records the health check status gauge and execution counter.
func (s *GRPCServer) recordMetrics(serviceName string, servingStatus grpc_health_v1.HealthCheckResponse_ServingStatus) {
	var statusValue float64
	var statusLabel string
	switch servingStatus {
	case grpc_health_v1.HealthCheckResponse_SERVING:
		statusValue = 1
		statusLabel = "serving"
	case grpc_health_v1.HealthCheckResponse_NOT_SERVING:
		statusValue = 0
		statusLabel = "not_serving"
	default:
		statusValue = -1
		statusLabel = "unknown"
	}
	metrics.status.WithLabelValues(s.name, serviceName).Set(statusValue)
	metrics.executionsTotal.WithLabelValues(s.name, serviceName, statusLabel).Inc()
}

// checkService executes the registered health check for the requested service
// within a timeout derived from TimeoutSeconds. Returns NOT_SERVING if the
// check fails, SERVING otherwise. Returns a NotFound error if the service
// is not registered.
func (s *GRPCServer) checkService(ctx context.Context, request *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	healthCheck, ok := s.serviceNameToHealthCheck[request.Service]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "service not found").Err()
	}
	ctxCancel, cancel := context.WithTimeout(ctx, s.opts.Timeout)
	defer cancel()
	start := time.Now()
	defer metrics.durationSeconds.WithLabelValues(s.name, request.Service).Observe(time.Since(start).Seconds())

	servingStatus := grpc_health_v1.HealthCheckResponse_SERVING
	if err := healthCheck(ctxCancel); err != nil {
		servingStatus = grpc_health_v1.HealthCheckResponse_UNKNOWN
		logAttrs := []any{"service", request.Service}

		// Look for a health check response.
		var healthCheckResponseFound bool
		status.RangeErrorDetails[*grpc_health_v1.HealthCheckResponse](err, func(healthCheckResponse *grpc_health_v1.HealthCheckResponse) bool {
			servingStatus = healthCheckResponse.Status
			logAttrs = append(logAttrs, "status", healthCheckResponse.Status.String())
			healthCheckResponseFound = true
			return false
		})
		if !healthCheckResponseFound {
			logAttrs = append(logAttrs, "error", err)
		}

		// Some providers will provide a breakdown.
		status.RangeErrorDetails[*grpc_health_v1.HealthListResponse](err, func(healthListResponse *grpc_health_v1.HealthListResponse) bool {
			for serviceName, serviceStatus := range healthListResponse.Statuses {
				if serviceName != "" && serviceStatus.Status != grpc_health_v1.HealthCheckResponse_SERVING {
					logAttrs = append(logAttrs, slog.Group(serviceName, "status", serviceStatus.Status.String()))
				}
			}
			return false
		})

		// If startup has not completed, we do not log.
		if s.startupCompleted {
			s.log.WarnContext(ctx, "health check failed", logAttrs...)
		}
	}
	return &grpc_health_v1.HealthCheckResponse{Status: servingStatus}, nil
}

// CheckFn returns a Check that queries the overall "" health status of this
// server, suitable for registering as a dependency health check in another
// GRPCServer instance.
func (s *GRPCServer) CheckFn() Check {
	return func(ctx context.Context) error {
		healthListRequest := &grpc_health_v1.HealthListRequest{}
		healthListResponse, err := s.List(ctx, healthListRequest)
		if err != nil {
			return err
		}
		healthCheckResponse := healthListResponse.Statuses[""]
		if healthCheckResponse.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			return status.Errorf(codes.Unavailable, "health check returned: %s", healthCheckResponse.Status).
				WithDetails(healthCheckResponse).
				WithDetails(healthListResponse).
				Err()
		}
		return nil
	}
}

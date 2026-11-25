package ai_processor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/routine"
)

type Opts struct {
	Targets  []string      `long:"target" env:"TARGETS" env-delim:"," description:"A gRPC service to mirror into platform tools, as pkg.Service=endpoint (unix: path, http(s):// or host:port). Repeatable"`
	Interval time.Duration `long:"interval" env:"INTERVAL" default:"5m" description:"Interval between mirroring passes"`
}

// target is a gRPC service the processor mirrors, and the endpoint serving it.
type target struct {
	service  string
	endpoint string
}

type runtime struct {
	targets []target
	// endpointToConnection holds one connection per endpoint, for the processor's life.
	endpointToConnection map[string]*grpc.Connection
}

func newRuntime(opts *Opts) (*runtime, error) {
	if opts.Interval <= 0 {
		return nil, fmt.Errorf("interval must be positive")
	}
	runtime := &runtime{endpointToConnection: map[string]*grpc.Connection{}}
	for _, value := range opts.Targets {
		service, endpoint, ok := strings.Cut(value, "=")
		if !ok || service == "" || endpoint == "" {
			return nil, fmt.Errorf("target %q is not pkg.Service=endpoint", value)
		}
		if _, err := toolSetID(service); err != nil {
			return nil, err
		}
		if _, err := grpc.ParseClientOpts(endpoint); err != nil {
			return nil, fmt.Errorf("parsing the endpoint of %s: %w", service, err)
		}
		runtime.targets = append(runtime.targets, target{service: service, endpoint: endpoint})
	}
	return runtime, nil
}

// start mirrors the targets now and every interval. A target that cannot be read leaves its tools as last mirrored.
func (s *Service) start(ctx context.Context) (func(), error) {
	mirror := routine.New("ai-processor-mirror", s.mirror).
		WithTicker(s.opts.Interval).WithConstantBackOff(10).WithMetrics().WithLogger(s.log).Start(ctx)
	return func() {
		mirror.Close()
		for _, connection := range s.endpointToConnection {
			connection.Close()
		}
	}, nil
}

// connection returns the connection to an endpoint, dialing it once.
func (s *Service) connection(ctx context.Context, endpoint string) (*grpc.Connection, error) {
	if connection, ok := s.endpointToConnection[endpoint]; ok {
		return connection, nil
	}
	clientOpts, err := grpc.ParseClientOpts(endpoint)
	if err != nil {
		return nil, err
	}
	connection, err := grpc.NewConnection(clientOpts, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("creating connection to %s: %w", endpoint, err)
	}
	if err := connection.Connect(context.WithoutCancel(ctx)); err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", endpoint, err)
	}
	s.endpointToConnection[endpoint] = connection
	return connection, nil
}

// HealthCheck reports healthy: a target failing is the target's problem, logged and counted.
func (s *Service) HealthCheck(ctx context.Context) error {
	return nil
}

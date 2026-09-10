package scheduler_dispatcher

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/malonaz/core/go/routine"
	"github.com/malonaz/core/go/scheduler/endpoints"
)

type Opts struct {
	MaxParallelJobs int           `long:"max-parallel-jobs" env:"MAX_PARALLEL_JOBS" default:"50" description:"Jobs this instance processes concurrently"`
	PollInterval    time.Duration `long:"poll-interval" env:"POLL_INTERVAL" default:"1s" description:"Interval between claim scans while idle"`
	LeaseDuration   time.Duration `long:"lease-duration" env:"LEASE_DURATION" default:"60s" description:"Lease held on a running job, renewed while its handler call is in flight. Must match the scheduler-service's"`
	Retention       time.Duration `long:"retention" env:"RETENTION" default:"720h" description:"How long terminal jobs are kept; 0 keeps them forever. Must match the scheduler-service's"`
	WorkerID        string        `long:"worker-id" env:"WORKER_ID" description:"Identifies this instance on the jobs it runs; defaults to hostname:pid"`
	Endpoints       []string      `long:"endpoint" env:"ENDPOINT" env-delim:"," description:"A gRPC endpoint to deliver to (unix: path, http(s):// or host:port); its scheduler-run methods are discovered over reflection at startup. Repeatable"`
}

type runtime struct {
	endpoints *endpoints.Pool

	// Bounds concurrency; the claim loop only asks for as many jobs as there are free slots.
	slots chan struct{}
	// Woken by the claim loop after a full batch, so work starts without waiting for the ticker.
	claimSignal chan struct{}
	workers     sync.WaitGroup
}

func newRuntime(opts *Opts) (*runtime, error) {
	if opts.MaxParallelJobs < 1 {
		return nil, fmt.Errorf("max-parallel-jobs must be at least 1")
	}
	if opts.LeaseDuration <= 0 || opts.PollInterval <= 0 {
		return nil, fmt.Errorf("lease-duration and poll-interval must be positive")
	}
	if opts.WorkerID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("resolving hostname: %w", err)
		}
		opts.WorkerID = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}
	return &runtime{
		endpoints:   endpoints.NewPool(),
		slots:       make(chan struct{}, opts.MaxParallelJobs),
		claimSignal: make(chan struct{}, 1),
	}, nil
}

// start discovers the endpoints' methods and converges their queues, then
// claims and runs jobs until stopped. Every endpoint must be serving: the
// dispatcher is started after the services it dispatches to.
func (s *Service) start(ctx context.Context) (func(), error) {
	if err := s.discover(ctx); err != nil {
		return nil, err
	}
	// Workers outlive the claim routine: they are cancelled and drained last.
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	claim := routine.New("scheduler-claim", func(ctx context.Context) error { return s.claim(ctx, workerCtx) }).
		WithTicker(s.opts.PollInterval).WithSignal(s.claimSignal).WithConstantBackOff(1).WithMetrics().WithLogger(s.log).Start(ctx)
	return func() {
		claim.Close()
		cancelWorkers()
		s.workers.Wait()
		s.endpoints.Close()
	}, nil
}

// HealthCheck reports the claim loop's health.
func (s *Service) HealthCheck(ctx context.Context) error {
	return nil
}

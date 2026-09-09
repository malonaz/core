package scheduler_service

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/malonaz/core/go/routine"
)

type Opts struct {
	MaxParallelJobs int           `long:"max-parallel-jobs" env:"MAX_PARALLEL_JOBS" default:"50" description:"Jobs this instance processes concurrently"`
	PollInterval    time.Duration `long:"poll-interval" env:"POLL_INTERVAL" default:"1s" description:"Interval between claim scans while idle"`
	LeaseDuration   time.Duration `long:"lease-duration" env:"LEASE_DURATION" default:"60s" description:"Lease held on a running job, renewed while its handler call is in flight; a lapsed lease returns the job to PENDING"`
	Retention       time.Duration `long:"retention" env:"RETENTION" default:"720h" description:"How long terminal jobs are kept; 0 keeps them forever"`
	SweepInterval   time.Duration `long:"sweep-interval" env:"SWEEP_INTERVAL" default:"1h" description:"Interval between retention sweeps"`
	WorkerID        string        `long:"worker-id" env:"WORKER_ID" description:"Identifies this instance on the jobs it runs; defaults to hostname:pid"`
}

type runtime struct {
	targets *targetConnections
	// What each target serves, the only source of method and message type information.
	schemas *targetSchemas

	// Workers in flight on this instance, so CancelJob can cut a local call short.
	inflight *inflight
	// Bounds concurrency; the claim loop only asks for as many jobs as there are free slots.
	slots chan struct{}
	// Woken by the claim loop after a full batch, and by CreateJob, so work starts without waiting for the ticker.
	claimSignal chan struct{}
	workers     sync.WaitGroup
}

func newRuntime(opts *Opts) (*runtime, error) {
	if opts.MaxParallelJobs < 1 {
		return nil, fmt.Errorf("max-parallel-jobs must be at least 1")
	}
	if opts.LeaseDuration <= 0 || opts.PollInterval <= 0 || opts.SweepInterval <= 0 {
		return nil, fmt.Errorf("lease-duration, poll-interval and sweep-interval must be positive")
	}
	if opts.WorkerID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("resolving hostname: %w", err)
		}
		opts.WorkerID = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}
	return &runtime{
		targets:     newTargetConnections(),
		schemas:     newTargetSchemas(),
		inflight:    newInflight(),
		slots:       make(chan struct{}, opts.MaxParallelJobs),
		claimSignal: make(chan struct{}, 1),
	}, nil
}

func (s *Service) start(ctx context.Context) (func(), error) {
	// Workers outlive the routines that spawn them: they are cancelled and drained last.
	workerCtx, cancelWorkers := context.WithCancel(ctx)

	routines := []*routine.Routine{
		routine.New("scheduler-claim", func(ctx context.Context) error { return s.claim(ctx, workerCtx) }).
			WithTicker(s.opts.PollInterval).WithSignal(s.claimSignal).WithConstantBackOff(1).WithMetrics().WithLogger(s.log).Start(ctx),
		routine.New("scheduler-reap", s.reap).
			WithTicker(s.opts.LeaseDuration / 2).WithConstantBackOff(1).WithMetrics().WithLogger(s.log).Start(ctx),
	}
	if s.opts.Retention > 0 {
		routines = append(routines, routine.New("scheduler-sweep", s.sweep).
			WithTicker(s.opts.SweepInterval).WithConstantBackOff(1).WithMetrics().WithLogger(s.log).Start(ctx))
	}

	return func() {
		for _, backgroundRoutine := range routines {
			backgroundRoutine.Close()
		}
		cancelWorkers()
		s.workers.Wait()
		s.targets.close()
	}, nil
}

// inflight tracks the cancel function of every job this instance is processing.
type inflight struct {
	mutex         sync.Mutex
	jobIDToCancel map[string]context.CancelFunc
}

func newInflight() *inflight {
	return &inflight{jobIDToCancel: map[string]context.CancelFunc{}}
}

func (i *inflight) add(jobID string, cancel context.CancelFunc) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	i.jobIDToCancel[jobID] = cancel
}

func (i *inflight) remove(jobID string) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	delete(i.jobIDToCancel, jobID)
}

// cancel cuts the job's handler call short if it runs on this instance.
func (i *inflight) cancel(jobID string) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	if cancel, ok := i.jobIDToCancel[jobID]; ok {
		cancel()
	}
}

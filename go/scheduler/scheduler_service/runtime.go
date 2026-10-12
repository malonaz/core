package scheduler_service

import (
	"context"
	"fmt"
	"time"

	policypb "github.com/malonaz/core/genproto/scheduler/policy/v1"
	"github.com/malonaz/core/go/routine"
	"github.com/malonaz/core/go/scheduler"
)

type Opts struct {
	LeaseDuration time.Duration `long:"lease-duration" env:"LEASE_DURATION" default:"60s" description:"Lease a dispatcher holds on a running job; a lapsed lease returns the job to PENDING. Must match the dispatchers'"`
	Retention     time.Duration `long:"retention" env:"RETENTION" default:"720h" description:"How long terminal jobs are kept; 0 keeps them forever"`
	SweepInterval time.Duration `long:"sweep-interval" env:"SWEEP_INTERVAL" default:"1h" description:"Interval between retention sweeps"`
	TickInterval  time.Duration `long:"tick-interval" env:"TICK_INTERVAL" default:"1s" description:"Interval between passes materializing due schedules' jobs"`
	// WaitJob's timeout is client-supplied, so it is capped here rather than trusted.
	WaitJobMaxTimeout time.Duration `long:"wait-job-max-timeout" env:"WAIT_JOB_MAX_TIMEOUT" default:"5m" description:"Longest a WaitJob call blocks, whatever timeout the request asks for"`
	// Workers on any instance write terminal transitions, so WaitJob can only re-read the row.
	WaitJobPollInterval time.Duration `long:"wait-job-poll-interval" env:"WAIT_JOB_POLL_INTERVAL" default:"250ms" description:"How often a WaitJob call re-reads the job"`
	// What a policy created or patched without a backoff gets. Must match the dispatchers'.
	DefaultRetryBackoff *scheduler.RetryBackoffOpts `group:"Default retry backoff" namespace:"default-retry-backoff" env-namespace:"DEFAULT_RETRY_BACKOFF"`
}

type runtime struct {
	defaultRetryBackoff *policypb.RetryBackoff
}

func newRuntime(opts *Opts) (*runtime, error) {
	if opts.LeaseDuration <= 0 || opts.SweepInterval <= 0 || opts.TickInterval <= 0 || opts.WaitJobMaxTimeout <= 0 || opts.WaitJobPollInterval <= 0 {
		return nil, fmt.Errorf("lease-duration, sweep-interval, tick-interval, wait-job-max-timeout and wait-job-poll-interval must be positive")
	}
	defaultRetryBackoff, err := opts.DefaultRetryBackoff.Proto()
	if err != nil {
		return nil, err
	}
	return &runtime{defaultRetryBackoff: defaultRetryBackoff}, nil
}

// start runs the store's upkeep: materializing due schedules' jobs, reaping
// jobs no dispatcher will complete and sweeping those past retention.
// Delivery is the dispatcher's.
func (s *Service) start(ctx context.Context) (func(), error) {
	routines := []*routine.Routine{
		routine.New("scheduler-tick", s.tick).
			WithTicker(s.opts.TickInterval).WithConstantBackOff(1).WithMetrics().WithLogger(s.log).Start(ctx),
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
	}, nil
}

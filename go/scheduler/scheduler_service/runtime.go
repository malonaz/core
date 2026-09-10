package scheduler_service

import (
	"context"
	"fmt"
	"time"

	"github.com/malonaz/core/go/routine"
)

type Opts struct {
	LeaseDuration time.Duration `long:"lease-duration" env:"LEASE_DURATION" default:"60s" description:"Lease a dispatcher holds on a running job; a lapsed lease returns the job to PENDING. Must match the dispatchers'"`
	Retention     time.Duration `long:"retention" env:"RETENTION" default:"720h" description:"How long terminal jobs are kept; 0 keeps them forever"`
	SweepInterval time.Duration `long:"sweep-interval" env:"SWEEP_INTERVAL" default:"1h" description:"Interval between retention sweeps"`
	// WaitJob's timeout is client-supplied, so it is capped here rather than trusted.
	WaitJobMaxTimeout time.Duration `long:"wait-job-max-timeout" env:"WAIT_JOB_MAX_TIMEOUT" default:"5m" description:"Longest a WaitJob call blocks, whatever timeout the request asks for"`
}

type runtime struct{}

func newRuntime(opts *Opts) (*runtime, error) {
	if opts.LeaseDuration <= 0 || opts.SweepInterval <= 0 || opts.WaitJobMaxTimeout <= 0 {
		return nil, fmt.Errorf("lease-duration, sweep-interval and wait-job-max-timeout must be positive")
	}
	return &runtime{}, nil
}

// start runs the store's upkeep: reaping jobs no dispatcher will complete and
// sweeping those past retention. Delivery is the dispatcher's.
func (s *Service) start(ctx context.Context) (func(), error) {
	routines := []*routine.Routine{
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

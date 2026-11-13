package agent_service

import (
	"context"
	"time"
)

// Opts holds the agent-service configuration.
type Opts struct {
	Workers                  int           `long:"workers" env:"WORKERS" default:"4" description:"Number of concurrent runner workers"`
	PollInterval             time.Duration `long:"poll-interval" env:"POLL_INTERVAL" default:"5s" description:"Queue poll interval; polling is the source of truth"`
	LeaseTimeout             time.Duration `long:"lease-timeout" env:"LEASE_TIMEOUT" default:"5m" description:"A claimed row whose heartbeat (update_time) is staler than this is re-queued by the reaper"`
	MaxTurnsPerClaim         int           `long:"max-turns-per-claim" env:"MAX_TURNS_PER_CLAIM" default:"32" description:"Maximum model turns driven on a single claim before re-queueing"`
	MaxTaskDepth             int           `long:"max-task-depth" env:"MAX_TASK_DEPTH" default:"3" description:"Maximum depth of a task tree"`
	CompactionCharThreshold  int           `long:"compaction-char-threshold" env:"COMPACTION_CHAR_THRESHOLD" default:"200000" description:"Approximate character size of an agent chat that triggers a rollover"`
	CompactionTailMessages   int           `long:"compaction-tail-messages" env:"COMPACTION_TAIL_MESSAGES" default:"6" description:"Number of trailing messages copied verbatim into the fresh chat"`
	CompactionSeedMemories   int           `long:"compaction-seed-memories" env:"COMPACTION_SEED_MEMORIES" default:"20" description:"Number of memories seeded into a fresh chat"`
	DisableRunner            bool          `long:"disable-runner" env:"DISABLE_RUNNER" description:"If set, this replica serves RPCs only and never claims work"`
}

// runtime holds long-lived service state.
type runtime struct{}

func newRuntime(opts *Opts) (*runtime, error) {
	return &runtime{}, nil
}

// start launches the runner: task workers, agent workers and the reaper.
// Polling is the wake mechanism (source of truth); every loop degrades to it.
func (s *Service) start(ctx context.Context) (func(), error) {
	if s.opts.DisableRunner {
		return func() {}, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLoops(ctx)
	}()
	return func() { cancel(); <-done }, nil
}

func (s *Service) runLoops(ctx context.Context) {
	for i := 0; i < s.opts.Workers; i++ {
		go s.pollLoop(ctx, s.processNextTask)
		go s.pollLoop(ctx, s.processNextAgent)
	}
	s.pollLoop(ctx, s.reap)
}

// pollLoop invokes fn every PollInterval; fn returns true when it did work,
// in which case it is re-invoked immediately (drain the queue).
func (s *Service) pollLoop(ctx context.Context, fn func(context.Context) bool) {
	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()
	for {
		for fn(ctx) {
			if ctx.Err() != nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

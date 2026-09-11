package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	"github.com/malonaz/core/go/routine"
	"github.com/malonaz/core/go/scheduler"
)

const (
	// defaultBatchSize is how many entries one pass drains.
	defaultBatchSize = 100
	// defaultInterval is how long a pass that drained nothing waits, and so the
	// latency an idle journal adds to an event. A pass that filled its batch
	// runs again at once, so a backlog is not paced by this.
	defaultInterval = 250 * time.Millisecond
)

// Opts configures a Relay. Everything but the optional fields is generated.
type Opts struct {
	// Name of the routine, for logs and metrics.
	Name string
	// List returns the oldest undelivered entries, up to the limit.
	List func(ctx context.Context, limit int) ([]*Entry, error)
	// Delete removes the entries the relay handed to the scheduler.
	Delete func(ctx context.Context, ids []string) error
	// Payload wraps an event in the request of the service's outbox method,
	// whose type is what selects the scheduler queue delivering it.
	Payload func(event *aippb.ResourceEvent) proto.Message
	// SchedulerServiceClient is where jobs are created.
	SchedulerServiceClient schedulerpb.SchedulerServiceClient

	// BatchSize is how many entries one pass drains. Defaults to 100.
	BatchSize int
	// Interval is how long a pass that drained nothing waits. Defaults to 1s.
	Interval time.Duration
}

// Relay hands journaled events to the scheduler, oldest first, and clears them
// once it has. Delivery is at-least-once: an entry whose job was created but
// not cleared is relayed again, and lands on the job it already created since
// the entry's id is the create's request id.
type Relay struct {
	opts    Opts
	log     *slog.Logger
	routine *routine.Routine
}

// New returns a relay over the journal opts describe.
func New(opts Opts) *Relay {
	if opts.BatchSize == 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.Interval == 0 {
		opts.Interval = defaultInterval
	}
	return &Relay{opts: opts, log: slog.Default()}
}

// WithLogger sets the logger of the relay's routine.
func (r *Relay) WithLogger(logger *slog.Logger) *Relay {
	r.log = logger
	return r
}

// Start runs the relay until Close, or until ctx is done.
func (r *Relay) Start(ctx context.Context) *Relay {
	r.routine = routine.New(r.opts.Name, r.drain).
		WithTicker(r.opts.Interval).
		WithConstantBackOff(1).
		WithMetrics().
		WithLogger(r.log).
		Start(ctx)
	return r
}

// Close stops the relay, blocking until its routine has exited.
func (r *Relay) Close() {
	if r.routine != nil {
		r.routine.Close()
	}
}

// HealthCheck reports whether the relay's routine is still running.
func (r *Relay) HealthCheck(ctx context.Context) error {
	if r.routine == nil {
		return fmt.Errorf("relay %q not started", r.opts.Name)
	}
	return r.routine.HealthCheck(ctx)
}

// drain hands batches to the scheduler until one comes up short, so a backlog
// is not paced by the relay's interval.
func (r *Relay) drain(ctx context.Context) error {
	for {
		listed, err := r.pass(ctx)
		if err != nil {
			return err
		}
		if listed < r.opts.BatchSize {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// pass hands one batch to the scheduler and returns how many entries it read.
// Entries are relayed in order and the pass stops at the first failure, so a
// poisoned entry holds the journal rather than letting the events behind it
// overtake it.
func (r *Relay) pass(ctx context.Context) (int, error) {
	entries, err := r.opts.List(ctx, r.opts.BatchSize)
	if err != nil {
		return 0, err
	}
	delivered := make([]string, 0, len(entries))
	var relayErr error
	for _, entry := range entries {
		if err := r.relay(ctx, entry); err != nil {
			relayErr = fmt.Errorf("relaying journal entry %s (%s): %w", entry.ID, entry.ResourceName, err)
			break
		}
		delivered = append(delivered, entry.ID)
	}
	if err := r.opts.Delete(ctx, delivered); err != nil {
		return 0, err
	}
	return len(entries), relayErr
}

// relay creates the job delivering one entry's event.
func (r *Relay) relay(ctx context.Context, entry *Entry) error {
	event, err := entry.ParseEvent()
	if err != nil {
		return err
	}
	_, err = scheduler.CreateJob(ctx, r.opts.SchedulerServiceClient, "", r.opts.Payload(event), scheduler.WithRequestID(entry.ID))
	return err
}

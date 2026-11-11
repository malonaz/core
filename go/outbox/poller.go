package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/malonaz/core/go/nats"
	"github.com/malonaz/core/go/routine"
)

var log = slog.Default().With("package", "outbox")

const (
	// defaultBatchSize is how many entries one claim takes.
	defaultBatchSize = 100
	// defaultInterval is how long a poll that drained the journal waits, and so
	// the latency an idle journal adds to an event.
	defaultInterval = 250 * time.Millisecond
)

// PollerOpts configures a Poller.
type PollerOpts struct {
	// Name of the routine, for logs and metrics.
	Name string
	// Claim claims up to limit entries of the journal and hands them to publish.
	Claim func(ctx context.Context, limit int, publish PublishFn) (int, error)

	// BatchSize is how many entries one claim takes. Defaults to 100.
	BatchSize int
	// Interval is how long a poll that drained the journal waits. Defaults to 250ms.
	Interval time.Duration
}

// Poller publishes a journal's entries to NATS, oldest first, and deletes them
// once published. Delivery is at-least-once: an entry published but not
// deleted is published again, and JetStream drops the duplicate within its
// deduplication window since the entry's id is the Nats-Msg-Id.
type Poller struct {
	opts       PollerOpts
	natsClient *nats.Client
	routine    *routine.Routine
}

// NewPoller returns a poller publishing through the client.
func NewPoller(opts PollerOpts, natsClient *nats.Client) *Poller {
	if opts.BatchSize == 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.Interval == 0 {
		opts.Interval = defaultInterval
	}
	return &Poller{opts: opts, natsClient: natsClient}
}

// Start runs the poller until Close, or until ctx is done.
func (p *Poller) Start(ctx context.Context) *Poller {
	if p.natsClient.OutboxPaused() {
		log.Warn("outbox paused: journal will buffer until unpaused", "poller", p.opts.Name)
	}
	p.routine = routine.New(p.opts.Name, p.drain).
		WithTicker(p.opts.Interval).
		WithConstantBackOff(1).
		WithMetrics().
		WithLogger(log).
		Start(ctx)
	return p
}

// Close stops the poller, blocking until its routine has exited.
func (p *Poller) Close() {
	if p.routine != nil {
		p.routine.Close()
	}
}

// HealthCheck reports whether the poller's routine is still running.
func (p *Poller) HealthCheck(ctx context.Context) error {
	if p.routine == nil {
		return fmt.Errorf("poller %q not started", p.opts.Name)
	}
	return p.routine.HealthCheck(ctx)
}

// drain claims batches until one comes up short, so a backlog is not paced by
// the interval.
func (p *Poller) drain(ctx context.Context) error {
	if p.natsClient.OutboxPaused() {
		return nil
	}
	for {
		claimed, err := p.opts.Claim(ctx, p.opts.BatchSize, p.publish)
		if err != nil {
			return err
		}
		if claimed < p.opts.BatchSize {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// publish publishes entries in order and stops at the first failure, so a
// failing entry holds the journal rather than letting later events overtake it.
func (p *Poller) publish(ctx context.Context, entries []*Entry) (int, error) {
	for i, entry := range entries {
		if _, err := p.natsClient.JetStream.Publish(ctx, entry.Subject, entry.Payload,
			jetstream.WithExpectStream(entry.Stream), jetstream.WithMsgID(entry.ID)); err != nil {
			return i, fmt.Errorf("publishing journal entry %s on %s: %w", entry.ID, entry.Subject, err)
		}
	}
	return len(entries), nil
}

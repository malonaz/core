package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/malonaz/core/go/nats"
	"github.com/malonaz/core/go/routine"
)

var log = slog.Default().With("package", "outbox")

// journalLagSeconds is what to alert on: it grows for as long as an entry
// cannot be published.
var journalLagSeconds = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "outbox_journal_lag_seconds",
		Help: "Age of the oldest entry a poller last found in its journal, 0 when it found none",
	},
	[]string{"poller"},
)

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
	lagSeconds prometheus.Gauge
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
	return &Poller{opts: opts, natsClient: natsClient, lagSeconds: journalLagSeconds.WithLabelValues(opts.Name)}
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
		if claimed == 0 {
			p.lagSeconds.Set(0)
		}
		if claimed < p.opts.BatchSize {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// publish publishes every entry it can, so a failing one holds back no other.
// Order is the consumer's to restore, by each entry's SequenceHeader.
func (p *Poller) publish(ctx context.Context, entries []*Entry) ([]*Entry, error) {
	p.lagSeconds.Set(time.Since(entries[0].CreateTime).Seconds())
	published := make([]*Entry, 0, len(entries))
	var errs []error
	for _, entry := range entries {
		headers := map[string][]string{SequenceHeader: {strconv.FormatInt(entry.Sequence, 10)}}
		if err := p.natsClient.PublishEncoded(ctx, entry.Stream, entry.Subject, entry.Payload, headers, entry.ID); err != nil {
			errs = append(errs, fmt.Errorf("publishing journal entry %s on %s: %w", entry.ID, entry.Subject, err))
			continue
		}
		published = append(published, entry)
	}
	return published, errors.Join(errs...)
}

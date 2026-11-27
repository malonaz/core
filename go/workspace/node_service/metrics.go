package node_service

import (
	"context"
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// The operations timed, each a log line and a series of phases.
const (
	operationSnapshot = "snapshot"
	operationPrepare  = "prepare"
	operationPurge    = "purge"
)

var (
	operationDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "workspace",
		Subsystem: "agent",
		Name:      "operation_duration_seconds",
		Help:      "Duration of the agent's operations on a filesystem by operation and resulting gRPC code.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 18),
	}, []string{"operation", "code"})

	phaseDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "workspace",
		Subsystem: "agent",
		Name:      "phase_duration_seconds",
		Help:      "Duration of each phase of the agent's operations on a filesystem.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 18),
	}, []string{"operation", "phase"})

	copyFilesHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "workspace",
		Subsystem: "agent",
		Name:      "copy_files",
		Help:      "Paths in a copy when walked, by operation.",
		Buckets:   prometheus.ExponentialBuckets(10, 4, 10),
	}, []string{"operation"})

	snapshotCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "workspace",
		Subsystem: "agent",
		Name:      "snapshots_total",
		Help:      "Snapshots taken by type: full, incremental, empty (nothing changed, nothing stored) or retry (answered from a recorded request).",
	}, []string{"type"})

	snapshotBytesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "workspace",
		Subsystem: "agent",
		Name:      "snapshot_bytes_total",
		Help:      "Bytes of snapshots by type and stage: raw (tar before compression) or stored (uploaded).",
	}, []string{"type", "stage"})

	snapshotPathsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "workspace",
		Subsystem: "agent",
		Name:      "snapshot_paths_total",
		Help:      "Paths in snapshots by change: added, changed or deleted.",
	}, []string{"change"})

	prepareCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "workspace",
		Subsystem: "agent",
		Name:      "prepares_total",
		Help:      "LOCAL copies prepared for a mount by outcome: kept (already at the latest snapshot) or restored.",
	}, []string{"outcome"})

	restoreBytesCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "workspace",
		Subsystem: "agent",
		Name:      "restore_bytes_total",
		Help:      "Bytes of snapshots downloaded to restore copies.",
	})
)

// operationTimer times an operation's phases into the metrics and its log line.
type operationTimer struct {
	operation  string
	start      time.Time
	last       time.Time
	attributes []any
}

func newOperationTimer(operation string, attributes ...any) *operationTimer {
	now := time.Now()
	return &operationTimer{operation: operation, start: now, last: now, attributes: attributes}
}

// phase ends the phase running since the previous one.
func (t *operationTimer) phase(name string) {
	now := time.Now()
	duration := now.Sub(t.last)
	t.last = now
	phaseDurationHistogram.WithLabelValues(t.operation, name).Observe(duration.Seconds())
	t.attributes = append(t.attributes, name+"_ms", milliseconds(duration))
}

// with adds attributes to the operation's log line.
func (t *operationTimer) with(attributes ...any) {
	t.attributes = append(t.attributes, attributes...)
}

// endOperation records an operation's duration and logs it with its phases: at error level only
// for a failure of the agent's own, so Sentry sees those and not refused requests.
func (s *Service) endOperation(ctx context.Context, timer *operationTimer, err error) {
	duration := time.Since(timer.start)
	code := grpcstatus.Code(err)
	operationDurationHistogram.WithLabelValues(timer.operation, code.String()).Observe(duration.Seconds())
	attributes := append(timer.attributes, "total_ms", milliseconds(duration))
	switch code {
	case codes.OK:
		s.log.InfoContext(ctx, timer.operation, attributes...)
	case codes.InvalidArgument, codes.FailedPrecondition, codes.Canceled:
		s.log.WarnContext(ctx, timer.operation+" refused", append(attributes, "error", err)...)
	default:
		s.log.ErrorContext(ctx, timer.operation+" failed", append(attributes, "error", err)...)
	}
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration.Microseconds()) / 1000
}

// countingWriter counts the bytes written through it.
type countingWriter struct {
	writer io.Writer
	count  int64
}

func (w *countingWriter) Write(bytes []byte) (int, error) {
	n, err := w.writer.Write(bytes)
	w.count += int64(n)
	return n, err
}

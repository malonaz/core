package scheduler_service

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/malonaz/core/gengo/scheduler/model"
	"github.com/malonaz/core/gengo/scheduler/store"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
)

var (
	attemptCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "attempts_total",
		Help:      "Handler calls by queue, method and resulting gRPC code.",
	}, []string{"queue", "method", "code"})

	attemptDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "attempt_duration_seconds",
		Help:      "Duration of handler calls by queue and method.",
		Buckets:   prometheus.ExponentialBuckets(0.05, 2, 16),
	}, []string{"queue", "method"})

	transitionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "transitions_total",
		Help:      "Outcomes recorded by workers, by queue, method and resulting state.",
	}, []string{"queue", "method", "state"})

	reapedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "reaped_total",
		Help:      "Running jobs returned to PENDING after their lease lapsed, by queue and method.",
	}, []string{"queue", "method"})

	inflightGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "inflight",
		Help:      "Jobs currently being processed by this instance.",
	})

	pendingGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scheduler",
		Subsystem: "jobs",
		Name:      "pending",
		Help:      "PENDING jobs by queue, refreshed on the reaper tick.",
	}, []string{"queue"})

	runningGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scheduler",
		Subsystem: "jobs",
		Name:      "running",
		Help:      "RUNNING jobs by queue, refreshed on the reaper tick.",
	}, []string{"queue"})

	oldestPendingAgeGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "oldest_pending_age_seconds",
		Help:      "Seconds since the earliest due time among a queue's PENDING jobs; 0 when nothing is pending.",
	}, []string{"queue"})
)

func observeAttempt(job *model.Job, duration time.Duration, err error) {
	attemptCounter.WithLabelValues(job.Queue, job.Method, grpcstatus.Code(err).String()).Inc()
	attemptDurationHistogram.WithLabelValues(job.Queue, job.Method).Observe(duration.Seconds())
}

func observeTransition(job *schedulerpb.Job, state schedulerpb.JobState) {
	transitionCounter.WithLabelValues(job.GetQueue(), job.GetMethod(), state.String()).Inc()
}

// observeQueueStats sets the backlog gauges from a full stats listing. Series
// of deleted queues are dropped rather than left at their last value.
func observeQueueStats(stats []*store.QueueStats, now time.Time) {
	pendingGauge.Reset()
	runningGauge.Reset()
	oldestPendingAgeGauge.Reset()
	for _, queueStats := range stats {
		queue := (&schedulerpb.QueueResourceName{Queue: queueStats.QueueID}).String()
		pendingGauge.WithLabelValues(queue).Set(float64(queueStats.PendingCount))
		runningGauge.WithLabelValues(queue).Set(float64(queueStats.RunningCount))
		var oldestPendingAge float64
		if queueStats.OldestPendingScheduleTime != nil {
			oldestPendingAge = max(now.Sub(*queueStats.OldestPendingScheduleTime).Seconds(), 0)
		}
		oldestPendingAgeGauge.WithLabelValues(queue).Set(oldestPendingAge)
	}
}

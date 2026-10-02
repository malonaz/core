package scheduler_service

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/malonaz/core/gengo/scheduler/store"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
)

var (
	reapedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "reaped_total",
		Help:      "Running jobs returned to PENDING after their lease lapsed, by queue and method.",
	}, []string{"queue", "method"})

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

	failedGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scheduler",
		Subsystem: "jobs",
		Name:      "failed",
		Help:      "FAILED jobs by queue, held until retried or purged, refreshed on the reaper tick.",
	}, []string{"queue"})

	scheduleTicksCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scheduler",
		Subsystem: "schedule",
		Name:      "ticks_total",
		Help:      "Schedule ticks reached, by outcome: created (a job), missed (run window already closed), failed (job creation failed; retried next pass).",
	}, []string{"outcome"})

	schedulesOverdueGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "scheduler",
		Subsystem: "schedules",
		Name:      "overdue",
		Help:      "ENABLED schedules whose tick was due more than a tick interval ago, refreshed on the tick pass.",
	})

	oldestPendingAgeGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "oldest_pending_age_seconds",
		Help:      "Seconds since the earliest due time among a queue's due PENDING jobs; 0 when nothing is due.",
	}, []string{"queue"})
)

// observeQueueStats sets the backlog gauges from a full stats listing. Series
// of deleted queues are dropped rather than left at their last value.
func observeQueueStats(stats []*store.QueueStats, now time.Time) {
	pendingGauge.Reset()
	runningGauge.Reset()
	failedGauge.Reset()
	oldestPendingAgeGauge.Reset()
	for _, queueStats := range stats {
		queue := (&schedulerpb.QueueRn{Queue: queueStats.QueueID}).String()
		pendingGauge.WithLabelValues(queue).Set(float64(queueStats.DueCount + queueStats.ScheduledCount))
		runningGauge.WithLabelValues(queue).Set(float64(queueStats.RunningCount))
		failedGauge.WithLabelValues(queue).Set(float64(queueStats.FailedCount))
		var oldestPendingAge float64
		if queueStats.OldestDueTime != nil {
			oldestPendingAge = max(now.Sub(*queueStats.OldestDueTime).Seconds(), 0)
		}
		oldestPendingAgeGauge.WithLabelValues(queue).Set(oldestPendingAge)
	}
}

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

	oldestPendingAgeGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "oldest_pending_age_seconds",
		Help:      "Seconds since the earliest due time among a queue's PENDING jobs; 0 when nothing is pending.",
	}, []string{"queue"})
)

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

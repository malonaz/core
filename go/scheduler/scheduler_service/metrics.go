package scheduler_service

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	grpcstatus "google.golang.org/grpc/status"

	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
)

var (
	attemptCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "attempts_total",
		Help:      "Processor calls by job type and resulting gRPC code.",
	}, []string{"job_type", "code"})

	attemptDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "attempt_duration_seconds",
		Help:      "Duration of processor calls by job type.",
		Buckets:   prometheus.ExponentialBuckets(0.05, 2, 16),
	}, []string{"job_type"})

	transitionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "transitions_total",
		Help:      "Outcomes recorded by workers, by job type and resulting state.",
	}, []string{"job_type", "state"})

	reapedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "reaped_total",
		Help:      "Running jobs returned to PENDING after their lease lapsed, by job type.",
	}, []string{"job_type"})

	inflightGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "inflight",
		Help:      "Jobs currently being processed by this instance.",
	})
)

func observeAttempt(jobType string, duration time.Duration, err error) {
	attemptCounter.WithLabelValues(jobType, grpcstatus.Code(err).String()).Inc()
	attemptDurationHistogram.WithLabelValues(jobType).Observe(duration.Seconds())
}

func observeTransition(jobType string, state schedulerpb.JobState) {
	transitionCounter.WithLabelValues(jobType, state.String()).Inc()
}

package scheduler_dispatcher

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/malonaz/core/gengo/scheduler/model"
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

	inflightGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "scheduler",
		Subsystem: "job",
		Name:      "inflight",
		Help:      "Jobs currently being processed by this instance.",
	})
)

func observeAttempt(job *model.Job, duration time.Duration, err error) {
	attemptCounter.WithLabelValues(job.Queue, job.Method, grpcstatus.Code(err).String()).Inc()
	attemptDurationHistogram.WithLabelValues(job.Queue, job.Method).Observe(duration.Seconds())
}

package routine

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricsOnce sync.Once
	metrics     *routineMetrics
)

type routineMetrics struct {
	executionsTotal *prometheus.CounterVec
	durationSeconds *prometheus.HistogramVec
	running         *prometheus.GaugeVec
}

func getMetrics() *routineMetrics {
	metricsOnce.Do(func() {
		metrics = &routineMetrics{
			executionsTotal: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "routine_executions_total",
					Help: "Total number of routine executions",
				},
				[]string{"routine", "status"},
			),
			durationSeconds: promauto.NewHistogramVec(
				prometheus.HistogramOpts{
					Name: "routine_execution_duration_seconds",
					Help: "Duration of routine executions",
				},
				[]string{"routine"},
			),
			running: promauto.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "routine_running",
					Help: "Whether the routine is currently running",
				},
				[]string{"routine"},
			),
		}
	})
	return metrics
}

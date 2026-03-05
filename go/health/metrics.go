package health

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var metrics = &healthMetrics{
	status: promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "health_check_status",
			Help: "Current serving status of the service (1=SERVING, 0=NOT_SERVING, -1=UNKNOWN)",
		},
		[]string{"name", "service"},
	),
	executionsTotal: promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "health_check_executions_total",
			Help: "Total number of health check executions",
		},
		[]string{"name", "service", "status"},
	),
	durationSeconds: promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "health_check_duration_seconds",
			Help: "Duration of health check executions",
		},
		[]string{"name", "service"},
	),
}

type healthMetrics struct {
	status          *prometheus.GaugeVec
	executionsTotal *prometheus.CounterVec
	durationSeconds *prometheus.HistogramVec
}

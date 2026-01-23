package grpc

import (
	"sync"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	prometheusServerMetricsOnce sync.Once
	prometheusServerMetrics     *grpc_prometheus.ServerMetrics

	prometheusDefaultHistogramBuckets = []float64{
		0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120,
	}
)

func getPrometheusServerMetrics() *grpc_prometheus.ServerMetrics {
	prometheusServerMetricsOnce.Do(func() {
		prometheusServerMetrics = grpc_prometheus.NewServerMetrics(
			grpc_prometheus.WithServerHandlingTimeHistogram(
				grpc_prometheus.WithHistogramBuckets(prometheusDefaultHistogramBuckets),
			),
		)
		prometheus.DefaultRegisterer.MustRegister(prometheusServerMetrics)
	})
	return prometheusServerMetrics
}

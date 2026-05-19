package ai_service

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	aipb "github.com/malonaz/core/genproto/ai/v1"
)

var (
	tokenCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ai",
		Subsystem: "model",
		Name:      "tokens_total",
		Help:      "Total tokens consumed by provider, model, and token type.",
	}, []string{"provider", "model", "token_type"})

	tokenCostCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ai",
		Subsystem: "model",
		Name:      "token_cost_dollars_total",
		Help:      "Total cost in dollars by provider, model, and token type.",
	}, []string{"provider", "model", "token_type"})

	requestCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ai",
		Subsystem: "model",
		Name:      "requests_total",
		Help:      "Total requests by provider and model.",
	}, []string{"provider", "model"})

	ttfbHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ai",
		Subsystem: "model",
		Name:      "ttfb_seconds",
		Help:      "Time to first byte in seconds.",
		Buckets:   prometheus.ExponentialBuckets(0.05, 2, 12),
	}, []string{"provider", "model"})

	ttlbHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ai",
		Subsystem: "model",
		Name:      "ttlb_seconds",
		Help:      "Time to last byte in seconds.",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12),
	}, []string{"provider", "model"})
)

func recordModelUsage(modelUsage *aipb.ModelUsage) {
	modelResourceName := &aipb.ModelResourceName{}
	if err := modelResourceName.UnmarshalString(modelUsage.GetModel()); err != nil {
		return
	}

	requestCounter.WithLabelValues(modelResourceName.Provider, modelResourceName.Model).Inc()

	tokenTypeToConsumption := map[string]*aipb.ResourceConsumption{
		"input":                   modelUsage.GetInputToken(),
		"output":                  modelUsage.GetOutputToken(),
		"output_reasoning":        modelUsage.GetOutputReasoningToken(),
		"input_cache_read":        modelUsage.GetInputTokenCacheRead(),
		"input_cache_write":       modelUsage.GetInputTokenCacheWrite(),
		"input_image":             modelUsage.GetInputImageToken(),
		"output_image":            modelUsage.GetOutputImageToken(),
		"input_image_cache_read":  modelUsage.GetInputImageTokenCacheRead(),
		"input_image_cache_write": modelUsage.GetInputImageTokenCacheWrite(),
	}

	for tokenType, consumption := range tokenTypeToConsumption {
		if consumption.GetQuantity() == 0 {
			continue
		}
		tokenCounter.WithLabelValues(modelResourceName.Provider, modelResourceName.Model, tokenType).Add(float64(consumption.Quantity))
		if consumption.Price > 0 {
			tokenCostCounter.WithLabelValues(modelResourceName.Provider, modelResourceName.Model, tokenType).Add(consumption.Price)
		}
	}
}

func recordGenerationMetrics(model string, generationMetrics *aipb.GenerationMetrics) {
	modelResourceName := &aipb.ModelResourceName{}
	if err := modelResourceName.UnmarshalString(model); err != nil {
		return
	}
	if generationMetrics.GetTtfb() != nil {
		ttfbHistogram.WithLabelValues(modelResourceName.Provider, modelResourceName.Model).Observe(generationMetrics.Ttfb.AsDuration().Seconds())
	}
	if generationMetrics.GetTtlb() != nil {
		ttlbHistogram.WithLabelValues(modelResourceName.Provider, modelResourceName.Model).Observe(generationMetrics.Ttlb.AsDuration().Seconds())
	}
}

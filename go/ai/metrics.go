package ai

import (
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

func NewResourceConsumption(quantity int32) *aipb.ResourceConsumption {
	if quantity == 0 {
		return nil
	}
	return &aipb.ResourceConsumption{
		Quantity: quantity,
	}
}

func ModelUsageCost(modelUsage *aipb.ModelUsage) float64 {
	return modelUsage.GetInputToken().GetPrice() +
		modelUsage.GetOutputToken().GetPrice() +
		modelUsage.GetOutputReasoningToken().GetPrice() +
		modelUsage.GetInputTokenCacheRead().GetPrice() +
		modelUsage.GetInputTokenCacheWrite().GetPrice() +
		modelUsage.GetInputSecond().GetPrice() +
		modelUsage.GetOutputSecond().GetPrice() +
		modelUsage.GetInputCharacter().GetPrice() +
		modelUsage.GetInputImageToken().GetPrice() +
		modelUsage.GetOutputImageToken().GetPrice() +
		modelUsage.GetInputImageTokenCacheRead().GetPrice() +
		modelUsage.GetInputImageTokenCacheWrite().GetPrice()
}

func SetModelUsagePrices(usage *aipb.ModelUsage, pricing *aipb.TttModelPricing) {
	setPrice := func(rc *aipb.ResourceConsumption, pricePerMillion float64) {
		if rc != nil {
			rc.Price = (float64(rc.Quantity) * pricePerMillion) / 1_000_000
		}
	}

	setPrice(usage.GetInputToken(), pricing.GetInputTokenPricePerMillion())
	setPrice(usage.GetOutputToken(), pricing.GetOutputTokenPricePerMillion())

	reasoningPrice := pricing.GetOutputReasoningTokenPricePerMillion()
	if reasoningPrice == 0 {
		reasoningPrice = pricing.GetOutputTokenPricePerMillion()
	}
	setPrice(usage.GetOutputReasoningToken(), reasoningPrice)

	setPrice(usage.GetInputTokenCacheRead(), pricing.GetInputTokenCacheReadPricePerMillion())
	setPrice(usage.GetInputTokenCacheWrite(), pricing.GetInputTokenCacheWritePricePerMillion())
	setPrice(usage.GetInputImageToken(), pricing.GetInputImageTokenPricePerMillion())
	setPrice(usage.GetOutputImageToken(), pricing.GetOutputImageTokenPricePerMillion())
	setPrice(usage.GetInputImageTokenCacheRead(), pricing.GetInputImageTokenCacheReadPricePerMillion())
	setPrice(usage.GetInputImageTokenCacheWrite(), pricing.GetInputImageTokenCacheWritePricePerMillion())
}

func IsModelUsageEmpty(modelUsage *aipb.ModelUsage) bool {
	return modelUsage.GetInputToken() == nil &&
		modelUsage.GetInputTokenCacheRead() == nil &&
		modelUsage.GetInputTokenCacheWrite() == nil &&
		modelUsage.GetOutputToken() == nil &&
		modelUsage.GetOutputReasoningToken() == nil &&
		modelUsage.GetInputImageToken() == nil &&
		modelUsage.GetInputImageTokenCacheRead() == nil &&
		modelUsage.GetInputImageTokenCacheWrite() == nil &&
		modelUsage.GetOutputImageToken() == nil &&
		modelUsage.GetInputSecond() == nil &&
		modelUsage.GetOutputSecond() == nil &&
		modelUsage.GetInputCharacter() == nil
}

func AggregateModelUsage(aggregateModelUsage *aipb.ModelUsage, modelUsages ...*aipb.ModelUsage) {
	addResourceConsumption := func(target **aipb.ResourceConsumption, source *aipb.ResourceConsumption) {
		if source.GetQuantity() == 0 && source.GetPrice() == 0 {
			return
		}
		if *target == nil {
			*target = &aipb.ResourceConsumption{}
		}
		(*target).Quantity += source.GetQuantity()
		(*target).Price += source.GetPrice()
	}

	for _, modelUsage := range modelUsages {
		addResourceConsumption(&aggregateModelUsage.InputToken, modelUsage.GetInputToken())
		addResourceConsumption(&aggregateModelUsage.OutputToken, modelUsage.GetOutputToken())
		addResourceConsumption(&aggregateModelUsage.OutputReasoningToken, modelUsage.GetOutputReasoningToken())
		addResourceConsumption(&aggregateModelUsage.InputTokenCacheRead, modelUsage.GetInputTokenCacheRead())
		addResourceConsumption(&aggregateModelUsage.InputTokenCacheWrite, modelUsage.GetInputTokenCacheWrite())
		addResourceConsumption(&aggregateModelUsage.InputSecond, modelUsage.GetInputSecond())
		addResourceConsumption(&aggregateModelUsage.OutputSecond, modelUsage.GetOutputSecond())
		addResourceConsumption(&aggregateModelUsage.InputCharacter, modelUsage.GetInputCharacter())
		addResourceConsumption(&aggregateModelUsage.InputImageToken, modelUsage.GetInputImageToken())
		addResourceConsumption(&aggregateModelUsage.OutputImageToken, modelUsage.GetOutputImageToken())
		addResourceConsumption(&aggregateModelUsage.InputImageTokenCacheRead, modelUsage.GetInputImageTokenCacheRead())
		addResourceConsumption(&aggregateModelUsage.InputImageTokenCacheWrite, modelUsage.GetInputImageTokenCacheWrite())
	}
}

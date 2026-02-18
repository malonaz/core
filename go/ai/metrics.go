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

package ai

import (
	"fmt"

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

func MergeModelUsage(base, incoming *aipb.ModelUsage) error {
	// Input tokens.
	if inputToken := incoming.GetInputToken(); inputToken != nil {
		if existingInputToken := base.GetInputToken(); existingInputToken != nil {
			if inputTokenCacheRead := incoming.GetInputTokenCacheRead(); inputTokenCacheRead != nil &&
				inputToken.Quantity+inputTokenCacheRead.Quantity == existingInputToken.Quantity {
				base.InputToken = inputToken
			} else if inputToken.Quantity < existingInputToken.Quantity {
				base.InputToken = inputToken
			} else if inputToken.Quantity > existingInputToken.Quantity {
				base.InputToken = inputToken
			}
		} else {
			base.InputToken = inputToken
		}
	}

	// Input cache read tokens.
	if inputTokenCacheRead := incoming.GetInputTokenCacheRead(); inputTokenCacheRead != nil {
		if existingInputTokenCacheRead := base.GetInputTokenCacheRead(); existingInputTokenCacheRead != nil {
			if existingInputTokenCacheRead.Quantity != inputTokenCacheRead.Quantity {
				return fmt.Errorf("received input cache read tokens twice with different quantities: previous %d, current %d",
					existingInputTokenCacheRead.Quantity, inputTokenCacheRead.Quantity)
			}
		} else {
			base.InputTokenCacheRead = inputTokenCacheRead
		}
	}

	// Input cache write tokens.
	if inputTokenCacheWrite := incoming.GetInputTokenCacheWrite(); inputTokenCacheWrite != nil {
		if existingInputTokenCacheWrite := base.GetInputTokenCacheWrite(); existingInputTokenCacheWrite != nil {
			if existingInputTokenCacheWrite.Quantity != inputTokenCacheWrite.Quantity {
				return fmt.Errorf("received input cache write tokens twice with different quantities: previous %d, current %d",
					existingInputTokenCacheWrite.Quantity, inputTokenCacheWrite.Quantity)
			}
		} else {
			base.InputTokenCacheWrite = inputTokenCacheWrite
		}
	}

	// Output tokens.
	if outputToken := incoming.GetOutputToken(); outputToken != nil {
		if existingOutputToken := base.GetOutputToken(); existingOutputToken != nil {
			if outputToken.Quantity < existingOutputToken.Quantity {
				return fmt.Errorf("received output tokens with smaller quantity: previous %d, current %d",
					existingOutputToken.Quantity, outputToken.Quantity)
			} else if outputToken.Quantity > existingOutputToken.Quantity {
				base.OutputToken = outputToken
			}
		} else {
			base.OutputToken = outputToken
		}
	}

	// Input image tokens.
	if inputImageToken := incoming.GetInputImageToken(); inputImageToken != nil {
		if existingInputImageToken := base.GetInputImageToken(); existingInputImageToken != nil {
			if inputImageToken.Quantity < existingInputImageToken.Quantity {
				base.InputImageToken = inputImageToken
			} else if inputImageToken.Quantity > existingInputImageToken.Quantity {
				base.InputImageToken = inputImageToken
			}
		} else {
			base.InputImageToken = inputImageToken
		}
	}

	// Input image cache read tokens.
	if inputImageTokenCacheRead := incoming.GetInputImageTokenCacheRead(); inputImageTokenCacheRead != nil {
		if existingInputImageTokenCacheRead := base.GetInputImageTokenCacheRead(); existingInputImageTokenCacheRead != nil {
			if existingInputImageTokenCacheRead.Quantity != inputImageTokenCacheRead.Quantity {
				return fmt.Errorf("received input image cache read tokens twice with different quantities: previous %d, current %d",
					existingInputImageTokenCacheRead.Quantity, inputImageTokenCacheRead.Quantity)
			}
		} else {
			base.InputImageTokenCacheRead = inputImageTokenCacheRead
		}
	}

	// Input image cache write tokens.
	if inputImageTokenCacheWrite := incoming.GetInputImageTokenCacheWrite(); inputImageTokenCacheWrite != nil {
		if existingInputImageTokenCacheWrite := base.GetInputImageTokenCacheWrite(); existingInputImageTokenCacheWrite != nil {
			if existingInputImageTokenCacheWrite.Quantity != inputImageTokenCacheWrite.Quantity {
				return fmt.Errorf("received input image cache write tokens twice with different quantities: previous %d, current %d",
					existingInputImageTokenCacheWrite.Quantity, inputImageTokenCacheWrite.Quantity)
			}
		} else {
			base.InputImageTokenCacheWrite = inputImageTokenCacheWrite
		}
	}

	// Output image tokens.
	if outputImageToken := incoming.GetOutputImageToken(); outputImageToken != nil {
		if existingOutputImageToken := base.GetOutputImageToken(); existingOutputImageToken != nil {
			if outputImageToken.Quantity < existingOutputImageToken.Quantity {
				return fmt.Errorf("received output image tokens with smaller quantity: previous %d, current %d",
					existingOutputImageToken.Quantity, outputImageToken.Quantity)
			} else if outputImageToken.Quantity > existingOutputImageToken.Quantity {
				base.OutputImageToken = outputImageToken
			}
		} else {
			base.OutputImageToken = outputImageToken
		}
	}

	// Output reasoning tokens.
	if outputReasoningToken := incoming.GetOutputReasoningToken(); outputReasoningToken != nil {
		if existingOutputReasoningToken := base.GetOutputReasoningToken(); existingOutputReasoningToken != nil {
			if outputReasoningToken.Quantity != existingOutputReasoningToken.Quantity {
				base.OutputReasoningToken = outputReasoningToken
			}
		} else {
			base.OutputReasoningToken = outputReasoningToken
		}
	}

	return nil
}

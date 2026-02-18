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

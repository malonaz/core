package ai

import (
	"testing"

	"github.com/stretchr/testify/require"

	aipb "github.com/malonaz/core/genproto/ai/v1"
)

func TestAddModelUsage(t *testing.T) {
	usage := func(model string, input, output int64) *aipb.ModelUsage {
		return &aipb.ModelUsage{
			Model:       model,
			InputToken:  &aipb.ResourceConsumption{Quantity: input, Price: float64(input) / 1e6},
			OutputToken: &aipb.ResourceConsumption{Quantity: output, Price: float64(output) / 1e6},
		}
	}

	var modelUsages []*aipb.ModelUsage
	modelUsages = AddModelUsage(modelUsages, usage("providers/p/models/b", 10, 1))
	modelUsages = AddModelUsage(modelUsages, usage("providers/p/models/c", 20, 2))
	modelUsages = AddModelUsage(modelUsages, usage("providers/p/models/a", 30, 3))
	modelUsages = AddModelUsage(modelUsages, usage("providers/p/models/b", 5, 5))

	require.Len(t, modelUsages, 3)
	// Sorted by model, one entry per model.
	require.Equal(t, "providers/p/models/a", modelUsages[0].GetModel())
	require.Equal(t, "providers/p/models/b", modelUsages[1].GetModel())
	require.Equal(t, "providers/p/models/c", modelUsages[2].GetModel())
	// Repeated model folds into the existing entry.
	require.Equal(t, int64(15), modelUsages[1].GetInputToken().GetQuantity())
	require.Equal(t, int64(6), modelUsages[1].GetOutputToken().GetQuantity())
	require.InDelta(t, 15e-6, modelUsages[1].GetInputToken().GetPrice(), 1e-12)
	require.Nil(t, modelUsages[1].GetOutputReasoningToken())
}

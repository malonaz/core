package openai

import (
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

var configXai = &config{
	ID:      providerIdXai,
	BaseUrl: "https://api.x.ai/v1",
	DefaultModels: []*aipb.Model{
		// TTT Models
		{
			Name:            provider.NewModelName(providerIdXai, "grok-4.1"),
			Description:     "A frontier multimodal model optimized specifically for high-performance agentic tool calling.",
			ProviderModelId: "grok-4-1-fast-non-reasoning",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 2_000_000,
				OutputTokenLimit:  30_000,
				Reasoning:         false,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(providerIdXai, "grok-4.1-reasoning"),
			Description:     "A frontier multimodal model optimized specifically for high-performance agentic tool calling, with reasoning capabilities.",
			ProviderModelId: "grok-4-1-fast-reasoning",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 2_000_000,
				OutputTokenLimit:  30_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(providerIdXai, "grok-code-fast-1"),
			Description:     "A speedy and economical reasoning model that excels at agentic coding.",
			ProviderModelId: "grok-code-fast-1",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 256_000,
				OutputTokenLimit:  30_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
	},
}

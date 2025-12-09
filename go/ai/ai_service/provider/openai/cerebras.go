package openai

import (
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

var configCerebras = &config{
	ID:      providerIdCerebras,
	BaseUrl: "https://api.cerebras.ai/v1",
	DefaultModels: []*aipb.Model{
		// TTT Models
		{
			Name:            provider.NewModelName(providerIdCerebras, "qwen3-235b"),
			ProviderModelId: "qwen-3-235b-a22b-instruct-2507",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 131_072,
				OutputTokenLimit:  40_960,
				Reasoning:         true,
				ToolCall:          true,
			},
		},

		{
			Name:            provider.NewModelName(providerIdCerebras, "glm-4.6"),
			ProviderModelId: "zai-glm-4.6",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 131_072,
				OutputTokenLimit:  40_960,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
	},
}

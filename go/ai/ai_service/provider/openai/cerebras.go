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
			Description:     "This non-thinking version offers powerful multilingual capabilities with significant improvements in instruction following, logical reasoning, mathematics, coding, and tool usage.",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 131_072,
				OutputTokenLimit:  40_960,
				Reasoning:         false,
				ToolCall:          true,
			},
		},

		{
			Name:            provider.NewModelName(providerIdCerebras, "glm-4.6"),
			ProviderModelId: "zai-glm-4.6",
			Description:     "This model delivers strong coding performance with advanced reasoning capabilities, superior tool use, and enhanced real-world performance in agentic coding applications.",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 131_072,
				OutputTokenLimit:  40_960,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
	},
}

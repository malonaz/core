package openai

import (
	"google.golang.org/protobuf/types/known/structpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

const (
	providerGoogleReasoningEffortType               = "reasoning_effort_type"
	providerGoogleReasoningEffortTypeThinkingBudget = "reasoning_effort_type_thinking_budget"
	providerGoogleReasoningEffortTypeThinkingLevel  = "reasoning_effort_type_thinking_level"

	providerGoogleThinkingLevelLow  = "low"
	providerGoogleThinkingLevelHigh = "high"

	providerGoogleThinkingBudgetMinimal = 1024
	providerGoogleThinkingBudgetLow     = 1024
	providerGoogleThinkingBudgetMedium  = 8192
	providerGoogleThinkingBudgetHigh    = 24576
)

var configGoogle = &config{
	ID:      providerIdGoogle,
	BaseUrl: "https://generativelanguage.googleapis.com/v1beta/openai",
	DefaultModels: []*aipb.Model{
		// TTT Models
		{
			Name:            provider.NewModelName(providerIdGoogle, "gemini-3-pro-preview"),
			ProviderModelId: "gemini-3-pro-preview",
			Description:     "The best model in the world for multimodal understanding, and our most powerful agentic and vibe-coding model yet, delivering richer visuals and deeper interactivity, all built on a foundation of state-of-the-art reasoning.",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 1_048_576,
				OutputTokenLimit:  65_536,
				Reasoning:         true,
				ToolCall:          true,
			},
			ProviderSettings: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					providerGoogleReasoningEffortType: structpb.NewStringValue(providerGoogleReasoningEffortTypeThinkingLevel),
				},
			},
		},

		{
			Name:            provider.NewModelName(providerIdGoogle, "gemini-2.5-pro"),
			ProviderModelId: "gemini-2.5-pro",
			Description:     "Our state-of-the-art thinking model, capable of reasoning over complex problems in code, math, and STEM, as well as analyzing large datasets, codebases, and documents using long context.",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 1_048_576,
				OutputTokenLimit:  65_536,
				Reasoning:         true,
				ToolCall:          true,
			},
			ProviderSettings: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					providerGoogleReasoningEffortType: structpb.NewStringValue(providerGoogleReasoningEffortTypeThinkingBudget),
				},
			},
		},

		{
			Name:            provider.NewModelName(providerIdGoogle, "gemini-2.5-flash"),
			ProviderModelId: "gemini-2.5-flash",
			Description:     "Our best model in terms of price-performance, offering well-rounded capabilities. 2.5 Flash is best for large scale processing, low-latency, high volume tasks that require thinking, and agentic use cases.",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 1_048_576,
				OutputTokenLimit:  65_536,
				Reasoning:         true,
				ToolCall:          true,
			},
			ProviderSettings: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					providerGoogleReasoningEffortType: structpb.NewStringValue(providerGoogleReasoningEffortTypeThinkingBudget),
				},
			},
		},

		{
			Name:            provider.NewModelName(providerIdGoogle, "gemini-2.5-flash-lite"),
			ProviderModelId: "gemini-2.5-flash-lite",
			Description:     "Our fastest flash model optimized for cost-efficiency and high throughput.",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 1_048_576,
				OutputTokenLimit:  65_536,
				Reasoning:         true,
				ToolCall:          true,
			},
			ProviderSettings: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					providerGoogleReasoningEffortType: structpb.NewStringValue(providerGoogleReasoningEffortTypeThinkingBudget),
				},
			},
		},
	},
}

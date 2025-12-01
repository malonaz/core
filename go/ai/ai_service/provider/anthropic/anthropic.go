package anthropic

import (
	"context"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"google.golang.org/protobuf/types/known/timestamppb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

// Client implements AI interfaces using Anthropic's API.
type Client struct {
	client       anthropic.Client
	modelService *provider.ModelService
}

// NewClient creates a new Anthropic client.
func NewClient(apiKey string, modelService *provider.ModelService) *Client {
	return &Client{
		client:       anthropic.NewClient(option.WithAPIKey(apiKey)),
		modelService: modelService,
	}
}

// Implements the provider.Provider interface.
func (c *Client) ProviderId() string { return "anthropic" }

// Implements the provider.Provider interface.
func (c *Client) Start(context.Context) error { return nil }

// Implements the provider.Provider interface.
func (c *Client) Stop() {}

// Implements the provider.Provider interface.
func (c *Client) DefaultModels() []*aipb.Model {
	return []*aipb.Model{
		// Latest Models - Auto-updating (non-pinned)
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-sonnet-4.5"),
			ProviderModelId: "claude-sonnet-4.5",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  64_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-haiku-4.5"),
			ProviderModelId: "claude-haiku-4.5",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  64_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-opus-4.5"),
			ProviderModelId: "claude-opus-4.5",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  64_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-opus-4.1"),
			ProviderModelId: "claude-opus-4.1",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  32_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},

		// Latest Models - Pinned versions
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-sonnet-4.5-20250929"),
			ProviderModelId: "claude-sonnet-4-5-20250929",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  64_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-haiku-4.5-20251001"),
			ProviderModelId: "claude-haiku-4-5-20251001",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  64_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-opus-4.5-20251101"),
			ProviderModelId: "claude-opus-4-5-20251101",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  64_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-opus-4.1-20250805"),
			ProviderModelId: "claude-opus-4-1-20250805",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  32_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},

		// Legacy Models - Pinned versions
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-sonnet-4-20250514"),
			ProviderModelId: "claude-sonnet-4-20250514",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  64_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-opus-4-20250514"),
			ProviderModelId: "claude-opus-4-20250514",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  32_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-3-5-haiku-20241022"),
			ProviderModelId: "claude-3-5-haiku-20241022",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  8_000,
				Reasoning:         false,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-3-haiku-20240307"),
			ProviderModelId: "claude-3-haiku-20240307",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  4_000,
				Reasoning:         false,
				ToolCall:          true,
			},
		},

		// Legacy Models - Auto-updating (non-pinned)
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-sonnet-4"),
			ProviderModelId: "claude-sonnet-4",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  64_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-opus-4"),
			ProviderModelId: "claude-opus-4",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  32_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-3-5-haiku"),
			ProviderModelId: "claude-3-5-haiku",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  8_000,
				Reasoning:         false,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-3-haiku"),
			ProviderModelId: "claude-3-haiku",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  4_000,
				Reasoning:         false,
				ToolCall:          true,
			},
		},

		// Deprecated. See [list](https://platform.claude.com/docs/en/about-claude/model-deprecations).
		{
			Name:            provider.NewModelName(c.ProviderId(), "claude-3-7-sonnet-20250219"),
			ProviderModelId: "claude-3-7-sonnet-20250219",
			DeprecateTime:   timestamppb.New(time.Date(2025, 10, 28, 0, 0, 0, 0, time.UTC)),
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 200_000,
				OutputTokenLimit:  64_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
	}
}

// Verify interface compliance at compile time.
var (
	_ provider.TextToTextClient = (*Client)(nil)
)

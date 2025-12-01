package anthropic

import (
	"context"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

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
		{
			Name:            (&aipb.ModelResourceName{Provider: c.ProviderId(), Model: "claude-3-7-sonnet-20250219"}).String(),
			ProviderModelId: "claude-3-7-sonnet-20250219",
			Ttt: &aipb.TttModelConfig{
				Reasoning: true,
				ToolCall:  true,
			},
		},
		{
			Name:            (&aipb.ModelResourceName{Provider: c.ProviderId(), Model: "claude-sonnet-4"}).String(),
			ProviderModelId: "claude-sonnet-4",
			Ttt: &aipb.TttModelConfig{
				Reasoning: true,
				ToolCall:  true,
			},
		},
	}
}

// Verify interface compliance at compile time.
var (
	_ provider.TextToTextClient = (*Client)(nil)
)

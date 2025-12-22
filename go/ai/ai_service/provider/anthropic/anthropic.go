package anthropic

import (
	"context"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

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
func (c *Client) ProviderId() string { return provider.Anthropic }

// Implements the provider.Provider interface.
func (c *Client) Start(context.Context) error { return nil }

// Implements the provider.Provider interface.
func (c *Client) Stop() {}

// Verify interface compliance at compile time.
var (
	_ provider.TextToTextClient = (*Client)(nil)
)

package google

import (
	"context"
	"fmt"

	"google.golang.org/genai"

	"github.com/malonaz/core/go/ai/ai_service/provider"
)

// Client is a Google Gemini API client.
type Client struct {
	apiKey       string
	client       *genai.Client
	modelService *provider.ModelService
}

// NewClient creates a new Google Gemini client.
func NewClient(apiKey string, modelService *provider.ModelService) *Client {
	return &Client{
		apiKey:       apiKey,
		modelService: modelService,
	}
}

// Implements the provider.Provider interface.
func (c *Client) Start(ctx context.Context) error {
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey: c.apiKey,
	})
	if err != nil {
		return fmt.Errorf("creating genai client: %w", err)
	}
	c.client = client
	return nil
}

// ProviderId returns the provider ID.
func (c *Client) ProviderId() string { return provider.Google }

// Close closes the client.
func (c *Client) Stop() {}

// Verify interface compliance at compile time.
var (
	_ provider.TextToTextClient = (*Client)(nil)
)

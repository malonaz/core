package anthropic

import (
	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

// Client implements AI interfaces using Anthropic's API.
type Client struct {
	client   anthropic.Client
	provider aipb.Provider
}

// Implements the provider.Provider interface.
func (c *Client) Provider() aipb.Provider { return c.provider }

// NewClient creates a new Anthropic client.
func NewClient(apiKey string) *Client {
	return &Client{
		client:   anthropic.NewClient(option.WithAPIKey(apiKey)),
		provider: aipb.Provider_PROVIDER_ANTHROPIC,
	}
}

// Verify interface compliance at compile time.
var (
	_ provider.TextToTextClient = (*Client)(nil)
)

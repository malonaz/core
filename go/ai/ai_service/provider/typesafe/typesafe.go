// Package typesafe implements a ClassificationClient against TypeSafe's Jev
// model: https://docs.typesafe.ai.
package typesafe

import (
	"context"
	"net/http"
	"time"

	"github.com/malonaz/core/go/ai/ai_service/provider"
)

const baseURL = "https://api.typesafe.ai/v1/systemone"

// Client implements provider.ClassificationClient against the TypeSafe API.
type Client struct {
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a new Jev client.
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// ProviderId implements the provider.Provider interface.
func (c *Client) ProviderId() string { return provider.TypeSafe }

// Start implements the provider.Provider interface.
func (c *Client) Start(context.Context) error { return nil }

// Stop implements the provider.Provider interface.
func (c *Client) Stop() {}

// Verify interface compliance at compile time.
var _ provider.ClassificationClient = (*Client)(nil)

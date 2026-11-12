// Package typesafe implements the decision provider backed by TypeSafe's Jev
// models: https://docs.typesafe.ai.
package typesafe

import (
	"context"

	"github.com/malonaz/core/go/ai/ai_service/provider"
)

// Client asks TypeSafe's System One API to answer decisions.
type Client struct {
	apiKey string
}

func NewClient(apiKey string) *Client {
	return &Client{apiKey: apiKey}
}

// Implements the provider.Provider interface.
func (c *Client) ProviderId() string { return provider.Typesafe }

// Implements the provider.Provider interface.
func (c *Client) Start(context.Context) error { return nil }

// Implements the provider.Provider interface.
func (c *Client) Stop() {}

// Verify interface compliance at compile time.
var _ provider.CreateDecisionClient = (*Client)(nil)

package elevenlabs

import (
	"context"
	"net/http"

	"github.com/malonaz/core/go/ai/ai_service/provider"
)

const (
	defaultBaseURL = "https://api.elevenlabs.io/v1"
)

type Client struct {
	apiKey       string
	baseURL      string
	client       *http.Client
	modelService *provider.ModelService
}

func NewClient(apiKey string, modelService *provider.ModelService) *Client {
	return &Client{
		apiKey:       apiKey,
		baseURL:      defaultBaseURL,
		client:       &http.Client{},
		modelService: modelService,
	}
}

// Implements the provider.Provider interface.
func (c *Client) ProviderId() string { return provider.Elevenlabs }

// Implements the provider.Provider interface.
func (c *Client) Start(context.Context) error { return nil }

// Implements the provider.Provider interface.
func (c *Client) Stop() {}

// Verify interface implementation
var (
	_ provider.TextToSpeechClient       = (*Client)(nil)
	_ provider.SpeechToTextStreamClient = (*Client)(nil)
)

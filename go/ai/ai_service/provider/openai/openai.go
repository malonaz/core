package openai

import (
	openai "github.com/sashabaranov/go-openai"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

const (
	openAIBaseUrl = "https://api.openai.com/v1"
	groqBaseUrl   = "https://api.groq.com/openai/v1"
)

// Client implements all AI interfaces using OpenAI's API.
type Client struct {
	client     *openai.Client
	provider   aipb.Provider
	pcmSupport bool
}

// Implements the provider.Provider interface.
func (c *Client) Provider() aipb.Provider { return c.provider }

// NewClient creates a new OpenAI client.
func NewClient(apiKey string) *Client {
	config := openai.DefaultConfig(apiKey)
	config.BaseURL = openAIBaseUrl
	return &Client{
		client:     openai.NewClientWithConfig(config),
		provider:   aipb.Provider_PROVIDER_OPENAI,
		pcmSupport: true,
	}
}

// NewClient creates a new OpenAI client.
func NewGroqClient(apiKey string) *Client {
	config := openai.DefaultConfig(apiKey)
	config.BaseURL = groqBaseUrl
	return &Client{
		client:   openai.NewClientWithConfig(config),
		provider: aipb.Provider_PROVIDER_GROQ,
	}
}

// Verify interface compliance at compile time.
var (
	_ provider.SpeechToTextClient = (*Client)(nil)
	_ provider.TextToSpeechClient = (*Client)(nil)
	_ provider.TextToTextClient   = (*Client)(nil)
)

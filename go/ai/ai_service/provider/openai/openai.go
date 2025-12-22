package openai

import (
	"context"

	openai2 "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	openai "github.com/sashabaranov/go-openai"

	"github.com/malonaz/core/go/ai/ai_service/provider"
)

type config struct {
	ID         string
	BaseUrl    string
	PcmSupport bool
}

var (
	configOpenAI = &config{
		ID:         provider.Openai,
		BaseUrl:    "https://api.openai.com/v1",
		PcmSupport: true,
	}

	configCerebras = &config{
		ID:      provider.Cerebras,
		BaseUrl: "https://api.cerebras.ai/v1",
	}

	configGoogle = &config{
		ID:      provider.Google,
		BaseUrl: "https://generativelanguage.googleapis.com/v1beta/openai",
	}

	configGroq = &config{
		ID:      provider.Groq,
		BaseUrl: "https://api.groq.com/openai/v1",
	}

	configXai = &config{
		ID:      provider.Xai,
		BaseUrl: "https://api.x.ai/v1",
	}
)

func (c *config) clientConfig(apiKey string) openai.ClientConfig {
	clientConfig := openai.DefaultConfig(apiKey)
	clientConfig.BaseURL = c.BaseUrl
	return clientConfig
}

// Client implements all AI interfaces using OpenAI's API.
type Client struct {
	config       *config
	client       *openai.Client
	client2      openai2.Client
	modelService *provider.ModelService
}

func newClient(apiKey string, modelService *provider.ModelService, config *config) *Client {
	return &Client{
		config:       config,
		client:       openai.NewClientWithConfig(config.clientConfig(apiKey)),
		client2:      openai2.NewClient(option.WithAPIKey(apiKey), option.WithBaseURL(config.BaseUrl)),
		modelService: modelService,
	}
}

func NewClient(apiKey string, modelService *provider.ModelService) *Client {
	return newClient(apiKey, modelService, configOpenAI)
}

func NewGroqClient(apiKey string, modelService *provider.ModelService) *Client {
	return newClient(apiKey, modelService, configGroq)
}

func NewCerebrasClient(apiKey string, modelService *provider.ModelService) *Client {
	return newClient(apiKey, modelService, configCerebras)
}

func NewGoogleClient(apiKey string, modelService *provider.ModelService) *Client {
	return newClient(apiKey, modelService, configGoogle)
}

func NewXaiClient(apiKey string, modelService *provider.ModelService) *Client {
	return newClient(apiKey, modelService, configXai)
}

// Implements the provider.Provider interface.
func (c *Client) ProviderId() string { return c.config.ID }

// Implements the provider.Provider interface.
func (c *Client) Start(context.Context) error { return nil }

// Implements the provider.Provider interface.
func (c *Client) Stop() {}

// Verify interface compliance at compile time.
var (
	_ provider.SpeechToTextClient = (*Client)(nil)
	_ provider.TextToSpeechClient = (*Client)(nil)
	_ provider.TextToTextClient   = (*Client)(nil)
)

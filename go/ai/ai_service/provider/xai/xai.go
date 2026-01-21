package xai

import (
	"context"

	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/ai/ai_service/provider/openai"
)

type Client struct {
	*openai.Client
	apiKey       string
	modelService *provider.ModelService
}

func NewClient(apiKey string, modelService *provider.ModelService) *Client {
	openAIClient := openai.NewXaiClient(apiKey, modelService)
	return &Client{
		apiKey:       apiKey,
		modelService: modelService,
		Client:       openAIClient,
	}
}

func (c *Client) ProviderId() string { return provider.Xai }

func (c *Client) Start(context.Context) error { return nil }

func (c *Client) Stop() {}

var (
	_ provider.SpeechToTextStreamClient = (*Client)(nil)
	_ provider.TextToTextClient         = (*Client)(nil)
)

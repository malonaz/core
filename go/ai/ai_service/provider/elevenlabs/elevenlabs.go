package elevenlabs

import (
	"context"
	"net/http"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
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
func (c *Client) ProviderId() string { return "elevenlabs" }

// Implements the provider.Provider interface.
func (c *Client) Start(context.Context) error { return nil }

// Implements the provider.Provider interface.
func (c *Client) Stop() {}

// Implements the provider.Provider interface.
func (c *Client) DefaultModels() []*aipb.Model {
	return []*aipb.Model{
		{
			Name:            (&aipb.ModelResourceName{Provider: c.ProviderId(), Model: "flash-v2-5"}).String(),
			ProviderModelId: "eleven_flash_v2_5",
			Tts: &aipb.TtsModelConfig{
				AudioFormat: &audiopb.Format{
					SampleRate:    16000,
					Channels:      1,
					BitsPerSample: 16,
				},
			},
		},
	}
}

// Verify interface implementation
var _ provider.TextToSpeechClient = (*Client)(nil)

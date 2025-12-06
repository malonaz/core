package cartesia

import (
	"context"
	"fmt"
	"net/url"

	"github.com/malonaz/core/go/websocket"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

const (
	defaultBaseURL = "https://api.cartesia.ai"
	apiVersion     = "2025-04-16"
	wsBaseURL      = "wss://api.cartesia.ai/tts/websocket"
)

type Client struct {
	apiKey         string
	baseURL        string
	ttsMultiplexer *Multiplexer[*TextToSpeechRequest, *TextToSpeechResponse]
	modelService   *provider.ModelService
}

func NewClient(apiKey string, modelService *provider.ModelService) *Client {
	return &Client{
		apiKey:       apiKey,
		baseURL:      defaultBaseURL,
		modelService: modelService,
	}
}

// Start initializes the WebSocket connection and starts the message handler
func (c *Client) Start(ctx context.Context) error {
	// Build the WebSocket URL with query parameters
	wsURL, err := url.Parse(wsBaseURL)
	if err != nil {
		return fmt.Errorf("failed to parse WebSocket URL: %w", err)
	}

	query := wsURL.Query()
	query.Set("api_key", c.apiKey)
	query.Set("cartesia_version", apiVersion)
	wsURL.RawQuery = query.Encode()

	// Create WebSocket client with properly typed multiplexed messages
	websocketClient, err := websocket.NewClient[
		MultiplexedRequest[*TextToSpeechRequest],
		MultiplexedResponse[*TextToSpeechResponse],
	](wsURL.String())
	if err != nil {
		return fmt.Errorf("failed to create WebSocket client: %w", err)
	}

	// Start the client
	if err := websocketClient.Start(ctx); err != nil {
		return fmt.Errorf("failed to start WebSocket client: %w", err)
	}

	c.ttsMultiplexer = NewMultiplexer[*TextToSpeechRequest, *TextToSpeechResponse](websocketClient)
	go c.ttsMultiplexer.Multiplex(ctx)

	return nil
}

// Stop gracefully shuts down the WebSocket connection
func (c *Client) Stop() {
	if c.ttsMultiplexer != nil {
		c.ttsMultiplexer.Close()
	}
}

func (c *Client) NewTextToSpeechStream() *Stream[*TextToSpeechRequest, *TextToSpeechResponse] {
	return c.ttsMultiplexer.NewStream()
}

// Implements the provider.Provider interface.
func (c *Client) ProviderId() string { return "cartesia" }

// Implements the provider.Provider interface.
func (c *Client) DefaultModels() []*aipb.Model {
	return []*aipb.Model{
		{
			Name:            (&aipb.ModelResourceName{Provider: c.ProviderId(), Model: "sonic-3"}).String(),
			ProviderModelId: "sonic-3",
			Tts: &aipb.TtsModelConfig{
				AudioFormat: &audiopb.Format{
					SampleRate:    8000,
					Channels:      1,
					BitsPerSample: 16,
				},
				SupportedSampleRates: []int32{
					8_000,
					16_000,
					22_050,
					24_000,
					44_100,
					48_000,
				},
			},
		},
	}
}

// Verify interface implementation
var _ provider.TextToSpeechClient = (*Client)(nil)

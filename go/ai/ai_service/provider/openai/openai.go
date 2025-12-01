package openai

import (
	"context"

	openai "github.com/sashabaranov/go-openai"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

const (
	providerIdGroq   = "groq"
	providerIdOpenai = "openai"

	openAIBaseUrl = "https://api.openai.com/v1"
	groqBaseUrl   = "https://api.groq.com/openai/v1"
)

// Client implements all AI interfaces using OpenAI's API.
type Client struct {
	client       *openai.Client
	providerId   string
	pcmSupport   bool
	modelService *provider.ModelService
}

// NewClient creates a new OpenAI client.
func NewClient(apiKey string, modelService *provider.ModelService) *Client {
	config := openai.DefaultConfig(apiKey)
	config.BaseURL = openAIBaseUrl
	return &Client{
		client:       openai.NewClientWithConfig(config),
		providerId:   providerIdOpenai,
		pcmSupport:   true,
		modelService: modelService,
	}
}

// NewClient creates a new OpenAI client.
func NewGroqClient(apiKey string, modelService *provider.ModelService) *Client {
	config := openai.DefaultConfig(apiKey)
	config.BaseURL = groqBaseUrl
	return &Client{
		client:       openai.NewClientWithConfig(config),
		providerId:   providerIdGroq,
		modelService: modelService,
	}
}

// Implements the provider.Provider interface.
func (c *Client) ProviderId() string { return c.providerId }

// Implements the provider.Provider interface.
func (c *Client) Start(context.Context) error { return nil }

// Implements the provider.Provider interface.
func (c *Client) Stop() {}

// Implements the provider.Provider interface.
func (c *Client) DefaultModels() []*aipb.Model {
	return providerIdToDefaultModels[c.ProviderId()]
}

var providerIdToDefaultModels = map[string][]*aipb.Model{
	providerIdOpenai: {
		// STT Models
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdOpenai, Model: "whisper-1"}).String(),
			ProviderModelId: "whisper-1",
			Stt:             &aipb.SttModelConfig{},
		},
		// TTT Models
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdOpenai, Model: "gpt-4o"}).String(),
			ProviderModelId: "gpt-4o",
			Ttt: &aipb.TttModelConfig{
				Reasoning: false,
				ToolCall:  true,
			},
		},
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdOpenai, Model: "gpt-4-turbo"}).String(),
			ProviderModelId: "gpt-4-turbo",
			Ttt: &aipb.TttModelConfig{
				Reasoning: false,
				ToolCall:  true,
			},
		},
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdOpenai, Model: "gpt-4-turbo-2024-04-09"}).String(),
			ProviderModelId: "gpt-4-turbo-2024-04-09",
			Ttt: &aipb.TttModelConfig{
				Reasoning: false,
				ToolCall:  true,
			},
		},
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdOpenai, Model: "gpt-4.1"}).String(),
			ProviderModelId: "gpt-4.1",
			Ttt: &aipb.TttModelConfig{
				Reasoning: false,
				ToolCall:  true,
			},
		},
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdOpenai, Model: "gpt-5"}).String(),
			ProviderModelId: "gpt-5",
			Ttt: &aipb.TttModelConfig{
				Reasoning: true,
				ToolCall:  true,
			},
		},
		// TTS Models
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdOpenai, Model: "tts-1"}).String(),
			ProviderModelId: "tts-1",
			Tts: &aipb.TtsModelConfig{
				AudioFormat: &audiopb.Format{
					SampleRate:    24000,
					Channels:      1,
					BitsPerSample: 16,
				},
			},
		},
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdOpenai, Model: "gpt-4o-mini-tts"}).String(),
			ProviderModelId: "gpt-4o-mini-tts",
			Tts: &aipb.TtsModelConfig{
				AudioFormat: &audiopb.Format{
					SampleRate:    24000,
					Channels:      1,
					BitsPerSample: 16,
				},
			},
		},
	},
	providerIdGroq: {
		// STT Models
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdGroq, Model: "whisper-large-v3-turbo"}).String(),
			ProviderModelId: "whisper-large-v3-turbo",
			Stt:             &aipb.SttModelConfig{},
		},
		// TTT Models
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdGroq, Model: "kimi-k2-instruct-0905"}).String(),
			ProviderModelId: "moonshotai/kimi-k2-instruct-0905",
			Ttt: &aipb.TttModelConfig{
				Reasoning: false,
				ToolCall:  true,
			},
		},
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdGroq, Model: "qwen3-32b"}).String(),
			ProviderModelId: "qwen/qwen3-32b",
			Ttt: &aipb.TttModelConfig{
				Reasoning: true,
				ToolCall:  true,
			},
		},
		// TTS Models
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdGroq, Model: "playai-tts"}).String(),
			ProviderModelId: "playai-tts",
			Tts: &aipb.TtsModelConfig{
				AudioFormat: &audiopb.Format{
					SampleRate:    48000,
					Channels:      1,
					BitsPerSample: 16,
				},
			},
		},
	},
}

// Verify interface compliance at compile time.
var (
	_ provider.SpeechToTextClient = (*Client)(nil)
	_ provider.TextToSpeechClient = (*Client)(nil)
	_ provider.TextToTextClient   = (*Client)(nil)
)

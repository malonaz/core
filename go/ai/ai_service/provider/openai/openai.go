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
	//
	// ───────────────────────────────────────────────────────────────
	// OPENAI
	// ───────────────────────────────────────────────────────────────
	//
	providerIdOpenai: {
		// ------------------------------
		// Text-to-Text (Core Models)
		// ------------------------------

		// Latest models.
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-5.1"),
			ProviderModelId: "gpt-5.1",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 400_000,
				OutputTokenLimit:  128_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-5-pro"),
			ProviderModelId: "gpt-5-pro",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 400_000,
				OutputTokenLimit:  272_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-5-mini"),
			ProviderModelId: "gpt-5-mini",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 400_000,
				OutputTokenLimit:  128_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-5-nano"),
			ProviderModelId: "gpt-5-nano",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 400_000,
				OutputTokenLimit:  128_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-4.1"),
			ProviderModelId: "gpt-4.1",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 1_047_576,
				OutputTokenLimit:  32_768,
				Reasoning:         false,
				ToolCall:          true,
			},
		},

		// Latest models (pinned).
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-5.1-2025-11-13"),
			ProviderModelId: "gpt-5.1-2025-11-13",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 400_000,
				OutputTokenLimit:  128_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-5-mini-2025-08-07"),
			ProviderModelId: "gpt-5-mini-2025-08-07",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 400_000,
				OutputTokenLimit:  128_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-5-nano-2025-08-07"),
			ProviderModelId: "gpt-5-nano-2025-08-07",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 400_000,
				OutputTokenLimit:  128_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-4.1-2025-04-14"),
			ProviderModelId: "gpt-4.1-2025-04-14",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 1_047_576,
				OutputTokenLimit:  32_768,
				Reasoning:         false,
				ToolCall:          true,
			},
		},
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-5-pro-2025-10-06"),
			ProviderModelId: "gpt-5-pro-2025-10-06",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 400_000,
				OutputTokenLimit:  272_000,
				Reasoning:         true,
				ToolCall:          true,
			},
		},

		// ------------------------------
		// Speech-to-Text
		// ------------------------------
		{
			Name:            provider.NewModelName(providerIdOpenai, "whisper-1"),
			ProviderModelId: "whisper-1",
			Stt:             &aipb.SttModelConfig{},
		},

		// ------------------------------
		// Text-to-Speech
		// ------------------------------
		{
			Name:            provider.NewModelName(providerIdOpenai, "tts-1"),
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
			Name:            provider.NewModelName(providerIdOpenai, "gpt-4o-mini-tts"),
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
				ContextTokenLimit: 262_144,
				OutputTokenLimit:  16_384,
				Reasoning:         false,
				ToolCall:          true,
			},
		},
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdGroq, Model: "llama-3.1-8b-instant"}).String(),
			ProviderModelId: "llama-3.1-8b-instant",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 131_072,
				OutputTokenLimit:  131_072,
				Reasoning:         false,
				ToolCall:          true,
			},
		},
		{
			Name:            (&aipb.ModelResourceName{Provider: providerIdGroq, Model: "qwen3-32b"}).String(),
			ProviderModelId: "qwen/qwen3-32b",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 131_072,
				OutputTokenLimit:  40_960,
				Reasoning:         true,
				ToolCall:          true,
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

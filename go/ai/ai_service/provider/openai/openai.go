package openai

import (
	"context"

	openai2 "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	openai "github.com/sashabaranov/go-openai"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

const (
	providerIdOpenai   = "openai"
	providerIdGroq     = "groq"
	providerIdCerebras = "cerebras"
	providerIdGoogle   = "google"
	providerIdXai      = "xai"
)

type config struct {
	ID            string
	BaseUrl       string
	DefaultModels []*aipb.Model
	PcmSupport    bool
}

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

// Implements the provider.Provider interface.
func (c *Client) DefaultModels() []*aipb.Model {
	return c.config.DefaultModels
}

// Verify interface compliance at compile time.
var (
	_ provider.SpeechToTextClient = (*Client)(nil)
	_ provider.TextToSpeechClient = (*Client)(nil)
	_ provider.TextToTextClient   = (*Client)(nil)
)

// /////////////////////////// OPEN AI CONFIG ////////////////
var configOpenAI = &config{
	ID:         providerIdOpenai,
	BaseUrl:    "https://api.openai.com/v1",
	PcmSupport: true,
	DefaultModels: []*aipb.Model{
		// ------------------------------
		// Text-to-Text (Core Models)
		// ------------------------------

		// Latest models.
		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-5.1"),
			ProviderModelId: "gpt-5.1",
			Description:     "GPT-5.1 is our flagship model for coding and agentic tasks with configurable reasoning and non-reasoning effort.",
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
			Description:     "GPT-5 pro uses more compute to think harder and provide consistently better answers. GPT-5 pro is available in the Responses API only to enable support for multi-turn model interactions before responding to API requests, and other advanced API features in the future. Since GPT-5 pro is designed to tackle tough problems, some requests may take several minutes to finish. To avoid timeouts, try using background mode. As our most advanced reasoning model, GPT-5 pro defaults to (and only supports) reasoning.effort: high. GPT-5 pro does not support code interpreter.",
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
			Description:     "GPT-5 mini is a faster, more cost-efficient version of GPT-5. It's great for well-defined tasks and precise prompts.",
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
			Description:     "GPT-5 Nano is our fastest, cheapest version of GPT-5. It's great for summarization and classification tasks.",
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
			Description:     "GPT-4.1 excels at instruction following and tool calling, with broad knowledge across domains. It features a 1M token context window, and low latency without a reasoning step. Note that we recommend starting with GPT-5 for complex tasks.",
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
			Description:     "GPT-5.1 is our flagship model for coding and agentic tasks with configurable reasoning and non-reasoning effort.",
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
			Description:     "GPT-5 mini is a faster, more cost-efficient version of GPT-5. It's great for well-defined tasks and precise prompts.",
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
			Description:     "GPT-5 Nano is our fastest, cheapest version of GPT-5. It's great for summarization and classification tasks.",
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
			Description:     "GPT-4.1 excels at instruction following and tool calling, with broad knowledge across domains. It features a 1M token context window, and low latency without a reasoning step. Note that we recommend starting with GPT-5 for complex tasks.",
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
			Description:     "GPT-5 pro uses more compute to think harder and provide consistently better answers. GPT-5 pro is available in the Responses API only to enable support for multi-turn model interactions before responding to API requests, and other advanced API features in the future. Since GPT-5 pro is designed to tackle tough problems, some requests may take several minutes to finish. To avoid timeouts, try using background mode. As our most advanced reasoning model, GPT-5 pro defaults to (and only supports) reasoning.effort: high. GPT-5 pro does not support code interpreter.",
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
			Description:     "Whisper is a general-purpose speech recognition model, trained on a large dataset of diverse audio. You can also use it as a multitask model to perform multilingual speech recognition as well as speech translation and language identification.",
			Stt:             &aipb.SttModelConfig{},
		},

		// ------------------------------
		// Text-to-Speech
		// ------------------------------
		{
			Name:            provider.NewModelName(providerIdOpenai, "tts-1"),
			ProviderModelId: "tts-1",
			Description:     "TTS is a model that converts text to natural sounding spoken text. The tts-1 model is optimized for realtime text-to-speech use cases. Use it with the Speech endpoint in the Audio API.",
			Tts: &aipb.TtsModelConfig{
				SupportedSampleRates: []int32{24_000},
				AudioFormat: &audiopb.Format{
					SampleRate:    24_000,
					Channels:      1,
					BitsPerSample: 16,
				},
			},
		},

		{
			Name:            provider.NewModelName(providerIdOpenai, "gpt-4o-mini-tts"),
			ProviderModelId: "gpt-4o-mini-tts",
			Description:     "GPT-4o mini TTS is a text-to-speech model built on GPT-4o mini, a fast and powerful language model. Use it to convert text to natural sounding spoken text. The maximum number of input tokens is 2000.",
			Tts: &aipb.TtsModelConfig{
				SupportedSampleRates: []int32{24_000},
				AudioFormat: &audiopb.Format{
					SampleRate:    24_000,
					Channels:      1,
					BitsPerSample: 16,
				},
			},
		},
	},
}

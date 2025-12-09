package openai

import (
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

var configGroq = &config{
	ID:      providerIdGroq,
	BaseUrl: "https://api.groq.com/openai/v1",
	DefaultModels: []*aipb.Model{
		// STT Models
		{
			Name:            provider.NewModelName(providerIdGroq, "whisper-large-v3-turbo"),
			ProviderModelId: "whisper-large-v3-turbo",
			Description:     "Whisper Large v3 is OpenAI's most advanced and capable speech recognition model, delivering state-of-the-art accuracy across a wide range of audio conditions and languages. This flagship model excels at handling challenging audio scenarios including background noise, accents, and technical terminology. With its robust architecture and extensive training, it represents the gold standard for automatic speech recognition tasks requiring the highest possible accuracy.",
			Stt:             &aipb.SttModelConfig{},
		},

		// TTT Models
		{
			Name:            provider.NewModelName(providerIdGroq, "kimi-k2-instruct-0905"),
			Description:     "Kimi K2 0905 is Moonshot AI's improved version of the Kimi K2 model, featuring enhanced coding capabilities with superior frontend development and tool calling performance. This Mixture-of-Experts (MoE) model with 1 trillion total parameters and 32 billion activated parameters offers improved integration with various agent scaffolds, making it ideal for building sophisticated AI agents and autonomous systems.",
			ProviderModelId: "moonshotai/kimi-k2-instruct-0905",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 262_144,
				OutputTokenLimit:  16_384,
				Reasoning:         false,
				ToolCall:          true,
			},
		},

		{
			Name:            provider.NewModelName(providerIdGroq, "llama-4-maverick-17b-128e-instruct"),
			ProviderModelId: "meta-llama/llama-4-maverick-17b-128e-instruct",
			Description:     "Llama 4 Maverick is Meta's natively multimodal model that enables text and image understanding. With a 17 billion parameter mixture-of-experts architecture (128 experts), this model offers industry-leading performance for multimodal tasks like natural assistant-like chat, image recognition, and coding tasks. With a 128K token context window and support for 12 languages (Arabic, English, French, German, Hindi, Indonesian, Italian, Portuguese, Spanish, Tagalog, Thai, and Vietnamese), the model delivers exceptional capabilities when paired with Groq for fast inference.",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 131_072,
				OutputTokenLimit:  8_192,
				Reasoning:         false,
				ToolCall:          true,
			},
		},

		{
			Name:            provider.NewModelName(providerIdGroq, "llama-3.3-70b-versatile"),
			ProviderModelId: "llama-3.3-70b-versatile",
			Description:     "Llama 3.3 70B Versatile is Meta's advanced multilingual large language model, optimized for a wide range of natural language processing tasks. With 70 billion parameters, it offers high performance across various benchmarks while maintaining efficiency suitable for diverse applications. The model supports tool use and JSON object mode, making it ideal for complex reasoning tasks and structured output generation.",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 131_072,
				OutputTokenLimit:  32_768,
				Reasoning:         false,
				ToolCall:          true,
			},
		},

		{
			Name:            provider.NewModelName(providerIdGroq, "llama-3.1-8b-instant"),
			ProviderModelId: "llama-3.1-8b-instant",
			Description:     "Llama 3.1 8B on Groq provides low-latency, high-quality responses suitable for real-time conversational interfaces, content filtering systems, and data analysis applications. This model offers a balance of speed and performance with significant cost savings compared to larger models. Technical capabilities include native function calling support, JSON mode for structured output generation, and a 128K token context window for handling large documents.",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 131_072,
				OutputTokenLimit:  131_072,
				Reasoning:         false,
				ToolCall:          true,
			},
		},

		{
			Name:            provider.NewModelName(providerIdGroq, "qwen3-32b"),
			ProviderModelId: "qwen/qwen3-32b",
			Description:     "Qwen 3 32B is the latest generation of large language models in the Qwen series, offering groundbreaking advancements in reasoning, instruction-following, agent capabilities, and multilingual support. It uniquely supports seamless switching between thinking mode (for complex logical reasoning, math, and coding) and non-thinking mode (for efficient, general-purpose dialogue) within a single model. The model excels in human preference alignment, creative writing, role-playing, and multi-turn dialogues, while supporting 100+ languages and dialects.",
			Ttt: &aipb.TttModelConfig{
				ContextTokenLimit: 131_072,
				OutputTokenLimit:  40_960,
				Reasoning:         true,
				ToolCall:          true,
			},
		},

		// TTS Models
		{
			Name:            provider.NewModelName(providerIdGroq, "playai-tts"),
			ProviderModelId: "playai-tts",
			Description:     "PlayAI Dialog v1.0 is a generative AI model designed to assist with creative content generation, interactive storytelling, and narrative development. Built on a transformer-based architecture, the model generates human-like audio to support writers, game developers, and content creators in vocalizing text to speech, crafting voice agentic experiences, or exploring interactive dialogue options.",
			Tts: &aipb.TtsModelConfig{
				SupportedSampleRates: []int32{48_000},
				AudioFormat: &audiopb.Format{
					SampleRate:    48_000,
					Channels:      1,
					BitsPerSample: 16,
				},
			},
		},
	},
}

package provider

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/malonaz/core/go/pbutil"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

type Provider interface {
	Provider() aipb.Provider
}

type SpeechToTextClient interface {
	Provider
	SpeechToText(context.Context, *aiservicepb.SpeechToTextRequest) (*aiservicepb.SpeechToTextResponse, error)
}

type TextToTextClient interface {
	Provider
	TextToText(context.Context, *aiservicepb.TextToTextRequest) (*aiservicepb.TextToTextResponse, error)
	TextToTextStream(*aiservicepb.TextToTextStreamRequest, aiservicepb.Ai_TextToTextStreamServer) error
}

// TextToSpeechClient uses the exact gRPC server streaming interface
type TextToSpeechClient interface {
	Provider
	TextToSpeechStream(*aiservicepb.TextToSpeechStreamRequest, aiservicepb.Ai_TextToSpeechStreamServer) error
}

var (
	modelToModelConfig = map[aipb.Model]*aipb.ModelConfig{}
	modelToTTTClient   = map[aipb.Model]TextToTextClient{}
	modelToSTTClient   = map[aipb.Model]SpeechToTextClient{}
	modelToTTSClient   = map[aipb.Model]TextToSpeechClient{}
)

func init() {
	for valueInteger := range aipb.Model_name {
		model := aipb.Model(valueInteger)
		if model == aipb.Model_MODEL_UNSPECIFIED {
			continue
		}
		modelConfig := pbutil.MustGetEnumValueOption(model, aipb.E_Config).(*aipb.ModelConfig)
		modelToModelConfig[model] = modelConfig
	}
}

func GetModelConfig(model aipb.Model) (*aipb.ModelConfig, error) {
	modelConfig, ok := modelToModelConfig[model]
	if !ok {
		return nil, fmt.Errorf("unknown model %s", model)
	}
	return modelConfig, nil
}

// GetTextToTextClient returns the registered TextToTextClient for the given provider and model.
func GetTextToTextClient(model aipb.Model) (TextToTextClient, bool) {
	client, ok := modelToTTTClient[model]
	return client, ok
}

// GetSpeechToTextClient returns the registered SpeechToTextClient for the given provider and model.
func GetSpeechToTextClient(model aipb.Model) (SpeechToTextClient, bool) {
	client, ok := modelToSTTClient[model]
	return client, ok
}

// GetTextToSpeechClient returns the registered TextToSpeechClient for the given provider and model.
func GetTextToSpeechClient(model aipb.Model) (TextToSpeechClient, bool) {
	client, ok := modelToTTSClient[model]
	return client, ok
}

func RegisterProviders(providers ...Provider) error {
	for _, provider := range providers {
		providerEnum := provider.Provider()
		slog.Info("registering provider", "provider", providerEnum)
		for model, modelConfig := range modelToModelConfig {
			if modelConfig.Provider != providerEnum {
				continue
			}
			switch modelConfig.ModelType {
			case aipb.ModelType_MODEL_TYPE_TTT:
				tttClient, ok := provider.(TextToTextClient)
				if !ok {
					return fmt.Errorf("provider %v has TTT model %s but does not implement the TTT client interface", providerEnum, model)
				}
				modelToTTTClient[model] = tttClient
			case aipb.ModelType_MODEL_TYPE_STT:
				sttClient, ok := provider.(SpeechToTextClient)
				if !ok {
					return fmt.Errorf("provider %v has STT model %s but does not implement the STT client interface", providerEnum, model)
				}
				modelToSTTClient[model] = sttClient
			case aipb.ModelType_MODEL_TYPE_TTS:
				ttsClient, ok := provider.(TextToSpeechClient)
				if !ok {
					return fmt.Errorf("provider %v has TTS model %s but does not implement the TTS client interface", providerEnum, model)
				}
				modelToTTSClient[model] = ttsClient
			default:
				return fmt.Errorf("unrecognized model type %v for provider %v model %s", modelConfig.ModelType, providerEnum, model)
			}
		}
	}
	return nil
}

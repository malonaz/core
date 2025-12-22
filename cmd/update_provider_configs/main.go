package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/pbutil"
)

var (
	//go:embed models.json
	litellmModels []byte
)

type LitellmModel struct {
	MaxInputTokens              float64  `json:"max_input_tokens"`
	MaxOutputTokens             float64  `json:"max_output_tokens"`
	SupportsFunctionCalling     bool     `json:"supports_function_calling"`
	SupportsReasoning           bool     `json:"supports_reasoning"`
	SupportedModalities         []string `json:"supported_modalities"`
	Mode                        string   `json:"mode"`
	InputCostPerToken           float64  `json:"input_cost_per_token"`
	OutputCostPerToken          float64  `json:"output_cost_per_token"`
	OutputCostPerReasoningToken float64  `json:"output_cost_per_reasoning_token"`
	CacheReadInputTokenCost     float64  `json:"cache_read_input_token_cost"`
	CacheCreationInputTokenCost float64  `json:"cache_creation_input_token_cost"`
}

var providerKeyFuncs = map[string]func(*aipb.Model) string{
	provider.Google: func(m *aipb.Model) string {
		return m.ProviderModelId
	},
	provider.Groq: func(m *aipb.Model) string {
		return "groq/" + m.ProviderModelId
	},
	provider.Anthropic: func(m *aipb.Model) string {
		return m.ProviderModelId
	},
	provider.Openai: func(m *aipb.Model) string {
		return m.ProviderModelId
	},
	provider.Xai: func(m *aipb.Model) string {
		return "xai/" + m.ProviderModelId
	},
	provider.Cerebras: func(m *aipb.Model) string {
		return "cerebras/" + m.ProviderModelId
	},
}

func main() {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(litellmModels, &raw); err != nil {
		log.Fatalf("parsing litellm models: %v", err)
	}
	delete(raw, "sample_spec")

	litellm := make(map[string]LitellmModel)
	for key, val := range raw {
		var model LitellmModel
		if err := json.Unmarshal(val, &model); err != nil {
			log.Printf("WARN: skipping %s: %v", key, err)
			continue
		}
		litellm[key] = model
	}

	configDir := "go/ai/ai_service/provider/configs"
	hasErrors := false

	for providerID, keyFunc := range providerKeyFuncs {
		configPath := filepath.Join(configDir, providerID+".json")
		configBytes, err := os.ReadFile(configPath)
		if err != nil {
			log.Fatalf("reading %s: %v", configPath, err)
		}

		config := &aipb.ProviderModelConfig{}
		if err := pbutil.JSONUnmarshalStrict(configBytes, config); err != nil {
			log.Fatalf("parsing %s: %v", configPath, err)
		}

		modified := false
		for _, model := range config.Models {
			if model.Ttt == nil {
				continue
			}

			key := keyFunc(model)
			litellmModel, found := litellm[key]
			if !found {
				log.Printf("ERROR: model %s (key=%s) not found in litellm", model.Name, key)
				hasErrors = true
				continue
			}

			if model.Ttt.Pricing == nil {
				model.Ttt.Pricing = &aipb.TttModelPricing{}
			}

			newPricing := &aipb.TttModelPricing{
				InputTokenPricePerMillion:           litellmModel.InputCostPerToken * 1_000_000,
				OutputTokenPricePerMillion:          litellmModel.OutputCostPerToken * 1_000_000,
				OutputReasoningTokenPricePerMillion: litellmModel.OutputCostPerReasoningToken * 1_000_000,
				InputCacheReadTokenPricePerMillion:  litellmModel.CacheReadInputTokenCost * 1_000_000,
				InputCacheWriteTokenPricePerMillion: litellmModel.CacheCreationInputTokenCost * 1_000_000,
			}

			if !proto.Equal(model.Ttt.Pricing, newPricing) {
				model.Ttt.Pricing = newPricing
				modified = true
			}
		}

		if modified {
			outBytes, err := pbutil.JSONMarshalPretty(config)
			if err != nil {
				log.Fatalf("marshaling %s: %v", configPath, err)
			}
			if err := os.WriteFile(configPath, outBytes, 0644); err != nil {
				log.Fatalf("writing %s: %v", configPath, err)
			}
			fmt.Printf("Updated %s\n", configPath)
		}
	}

	if hasErrors {
		os.Exit(1)
	}
	fmt.Println("All models processed successfully")
}

package provider

import (
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil"
)

func NewModelName(provider, model string) string {
	return (&aipb.ModelResourceName{Provider: provider, Model: model}).String()
}

func parseModels(bytes []byte) (*aipb.ProviderModelConfig, error) {
	config := &aipb.ProviderModelConfig{}
	err := pbutil.JSONUnmarshalStrict(bytes, config)
	return config, err
}

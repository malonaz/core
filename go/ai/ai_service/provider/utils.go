package provider

import (
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

func NewModelName(provider, model string) string {
	return (&aipb.ModelResourceName{Provider: provider, Model: model}).String()
}

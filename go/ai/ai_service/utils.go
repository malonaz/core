package ai_service

import (
	"fmt"
	"time"

	aipb "github.com/malonaz/core/genproto/ai/v1"
)

// checkModelDeprecation validates that a model is not deprecated and returns an error if it is.
func checkModelDeprecation(model *aipb.Model) error {
	if model.DeprecateTime == nil {
		return nil
	}
	now := time.Now()
	deprecateTime := model.DeprecateTime.AsTime()
	if now.After(deprecateTime) {
		return fmt.Errorf("model %s has been deprecated as of %s", model.Name, deprecateTime.Format(time.RFC3339))
	}
	return nil
}

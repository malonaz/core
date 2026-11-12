package anthropic

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
)

func TestToolSchemaKeywords(t *testing.T) {
	tool := &aipb.Tool{
		Name: "CreateReport",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"title": {Type: "string", MaxLength: 256},
				"labels": {
					Type:                 "object",
					AdditionalProperties: &jsonpb.Schema{Type: "string", MaxLength: 63},
				},
			},
			Required: []string{"title"},
		},
	}
	anthropicTool, err := pbToolToAnthropic(tool, false)
	require.NoError(t, err)
	bytes, err := json.Marshal(anthropicTool)
	require.NoError(t, err)

	var wire struct {
		InputSchema map[string]any `json:"input_schema"`
	}
	require.NoError(t, json.Unmarshal(bytes, &wire))
	require.Equal(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"title": map[string]any{"type": "string", "maxLength": float64(256)},
			"labels": map[string]any{
				"type":                 "object",
				"additionalProperties": map[string]any{"type": "string", "maxLength": float64(63)},
			},
		},
		"required": []any{"title"},
	}, wire.InputSchema)
}

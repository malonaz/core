package openai

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
)

// toolWithSchemaKeywords declares a tool whose schema uses keywords that the proto and JSON Schema spell differently.
func toolWithSchemaKeywords() *aipb.Tool {
	return &aipb.Tool{
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
		},
	}
}

// The properties of toolWithSchemaKeywords as a provider must receive them.
var schemaKeywordsProperties = map[string]any{
	"title": map[string]any{"type": "string", "maxLength": float64(256)},
	"labels": map[string]any{
		"type":                 "object",
		"additionalProperties": map[string]any{"type": "string", "maxLength": float64(63)},
	},
}

func TestToolSchemaKeywords(t *testing.T) {
	openaiTool, err := pbToolToOpenAI(toolWithSchemaKeywords())
	require.NoError(t, err)
	bytes, err := json.Marshal(openaiTool)
	require.NoError(t, err)

	var wire struct {
		Function struct {
			Parameters map[string]any `json:"parameters"`
		} `json:"function"`
	}
	require.NoError(t, json.Unmarshal(bytes, &wire))
	require.Equal(t, schemaKeywordsProperties, wire.Function.Parameters["properties"])
}

package ai

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	jsonpb "github.com/malonaz/core/genproto/json/v1"
)

func TestJSONMarshal(t *testing.T) {
	value, err := structpb.NewStruct(map[string]any{
		"z": 1,
		"a": []any{"x, y", "z"},
		"m": map[string]any{"k": true},
	})
	require.NoError(t, err)

	// Holds whatever space protojson adds for this binary; string contents are untouched.
	bytes, err := JSONMarshal(value)
	require.NoError(t, err)
	require.Equal(t, `{"a":["x, y","z"],"m":{"k":true},"z":1}`, string(bytes))
}

func TestJSONSchemaMarshal(t *testing.T) {
	schema := &jsonpb.Schema{
		Type: "object",
		Properties: map[string]*jsonpb.Schema{
			"tags": {Type: "array", Items: &jsonpb.Schema{Type: "string", MinLength: 1}, MinItems: 1, MaxItems: 5},
		},
		AdditionalProperties: &jsonpb.Schema{Type: "number", ExclusiveMinimum: 1, MultipleOf: 2},
		AnyOf:                []*jsonpb.Schema{{Type: "null"}},
	}
	bytes, err := JSONSchemaMarshal(schema)
	require.NoError(t, err)
	require.Equal(
		t,
		`{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string","minLength":1},"minItems":1,"maxItems":5}},"additionalProperties":{"type":"number","exclusiveMinimum":1,"multipleOf":2},"anyOf":[{"type":"null"}]}`,
		string(bytes),
	)
}

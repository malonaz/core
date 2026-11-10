package ai

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
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

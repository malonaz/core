package tool

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
)

func TestUnwrapExecuteToolCall(t *testing.T) {
	t.Run("rewrites the call into the named tool", func(t *testing.T) {
		arguments, err := structpb.NewStruct(map[string]any{
			"name":      "get_weather",
			"arguments": map[string]any{"location": "Paris"},
		})
		require.NoError(t, err)
		toolCall := &aipb.ToolCall{Id: "1", Name: ExecuteToolName, Arguments: arguments}

		require.True(t, UnwrapExecuteToolCall(toolCall))
		require.Equal(t, "get_weather", toolCall.Name)
		require.Equal(t, map[string]any{"location": "Paris"}, toolCall.Arguments.AsMap())
	})

	t.Run("defaults missing arguments to an empty object", func(t *testing.T) {
		arguments, err := structpb.NewStruct(map[string]any{"name": "ping"})
		require.NoError(t, err)
		toolCall := &aipb.ToolCall{Name: ExecuteToolName, Arguments: arguments}

		require.True(t, UnwrapExecuteToolCall(toolCall))
		require.Equal(t, "ping", toolCall.Name)
		require.Empty(t, toolCall.Arguments.AsMap())
	})

	t.Run("reports a partial call whose name has not streamed yet", func(t *testing.T) {
		toolCall := &aipb.ToolCall{Name: ExecuteToolName, Arguments: &structpb.Struct{}, Partial: true}

		require.False(t, UnwrapExecuteToolCall(toolCall))
		require.Equal(t, ExecuteToolName, toolCall.Name)
	})
}

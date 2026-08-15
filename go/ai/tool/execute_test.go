package tool

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/status"
)

func newExecuteToolCall(t *testing.T, toolName string, arguments map[string]any) *aipb.ToolCall {
	t.Helper()
	argumentsStruct, err := structpb.NewStruct(map[string]any{
		"tool_name": toolName,
		"arguments": arguments,
	})
	require.NoError(t, err)
	return &aipb.ToolCall{
		Id:          "call-1",
		Name:        ExecuteToolName,
		Arguments:   argumentsStruct,
		Annotations: CreateExecuteTool().GetAnnotations(),
	}
}

func TestCreateExecuteTool(t *testing.T) {
	tool := CreateExecuteTool()
	require.Equal(t, ExecuteToolName, tool.GetName())
	require.Equal(t, AnnotationValueToolTypeExecute, tool.GetAnnotations()[AnnotationKeyToolType])
	require.ElementsMatch(t, []string{"tool_name", "arguments"}, tool.GetJsonSchema().GetRequired())
}

func TestParseExecuteToolCall(t *testing.T) {
	t.Run("parses tool name and arguments", func(t *testing.T) {
		toolCall := newExecuteToolCall(t, "MyTool", map[string]any{"key": "value"})
		toolCallExecute, err := ParseExecuteToolCall(toolCall)
		require.NoError(t, err)
		require.Equal(t, "MyTool", toolCallExecute.GetToolName())
		require.Equal(t, "value", toolCallExecute.GetArguments().AsMap()["key"])
	})

	t.Run("errors on missing tool name", func(t *testing.T) {
		toolCall := newExecuteToolCall(t, "", nil)
		_, err := ParseExecuteToolCall(toolCall)
		require.True(t, status.HasCode(err, codes.InvalidArgument))
	})
}

func TestUnwrapExecuteToolCall(t *testing.T) {
	innerTool := &aipb.Tool{
		Name: "MyTool",
		Annotations: map[string]string{
			AnnotationKeyToolType:     AnnotationValueToolTypeGenerateMessage,
			AnnotationKeyProtoMessage: "some.Message",
		},
	}
	toolNameToTool := map[string]*aipb.Tool{"MyTool": innerTool}

	t.Run("unwraps into the inner tool call", func(t *testing.T) {
		toolCall := newExecuteToolCall(t, "MyTool", map[string]any{"key": "value"})
		unwrappedToolCall, err := UnwrapExecuteToolCall(toolCall, toolNameToTool)
		require.NoError(t, err)
		require.Equal(t, "call-1", unwrappedToolCall.GetId())
		require.Equal(t, "MyTool", unwrappedToolCall.GetName())
		require.Equal(t, "value", unwrappedToolCall.GetArguments().AsMap()["key"])
		// Inherits the inner tool's annotations, not Execute's.
		require.Equal(t, innerTool.GetAnnotations(), unwrappedToolCall.GetAnnotations())
	})

	t.Run("errors on undiscovered tool", func(t *testing.T) {
		toolCall := newExecuteToolCall(t, "UnknownTool", nil)
		_, err := UnwrapExecuteToolCall(toolCall, toolNameToTool)
		require.True(t, status.HasCode(err, codes.NotFound))
	})
}

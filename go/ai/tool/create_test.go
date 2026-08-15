package tool

import (
	"testing"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

func TestNewMethod(t *testing.T) {
	t.Run("normalizes a generated gRPC full method name", func(t *testing.T) {
		// The form emitted by protoc-gen-go-grpc's *_FullMethodName constants.
		method := NewMethod("/malonaz.ai.ai_engine.v1.AiEngine/CreateTool")
		require.Equal(t, "malonaz.ai.ai_engine.v1.AiEngine.CreateTool", method.fullName)
	})

	t.Run("leaves an already dotted name untouched", func(t *testing.T) {
		method := NewMethod("malonaz.ai.ai_engine.v1.AiEngine.CreateTool")
		require.Equal(t, "malonaz.ai.ai_engine.v1.AiEngine.CreateTool", method.fullName)
	})
}

func TestMethodToolRequest(t *testing.T) {
	t.Run("sets the dotted method name expected by descriptor lookups", func(t *testing.T) {
		createToolRequest := MethodToolRequest(NewMethod("/malonaz.ai.ai_engine.v1.AiEngine/CreateTool"))
		require.Equal(t,
			"malonaz.ai.ai_engine.v1.AiEngine.CreateTool",
			createToolRequest.GetDescriptorReference().GetMethod(),
		)
	})

	t.Run("leaves max depth unset so the server applies its own default", func(t *testing.T) {
		createToolRequest := MethodToolRequest(NewMethod("/pkg.Service/Method"))
		require.Zero(t, createToolRequest.GetSchemaConfiguration().GetWithMaxDepth())
	})

	t.Run("applies options", func(t *testing.T) {
		fieldMask := pbfieldmask.FromPaths("contact.metadata")
		createToolRequest := MethodToolRequest(
			NewMethod("/pkg.Service/Method"),
			WithFieldMask(fieldMask),
			WithMaxDepth(8),
			WithResponseReadMask(),
			WithResponseSchema(5),
		)
		schemaConfiguration := createToolRequest.GetSchemaConfiguration()
		require.Equal(t, []string{"contact.metadata"}, schemaConfiguration.GetFieldMask().GetPaths())
		require.EqualValues(t, 8, schemaConfiguration.GetWithMaxDepth())
		require.True(t, schemaConfiguration.GetWithResponseReadMask())
		require.EqualValues(t, 5, schemaConfiguration.GetWithResponseSchemaMaxDepth())
	})
}

func TestMessageToolRequest(t *testing.T) {
	t.Run("derives the message full name from the descriptor", func(t *testing.T) {
		createToolRequest := MessageToolRequest(&fieldmaskpb.FieldMask{})
		require.Equal(t,
			"google.protobuf.FieldMask",
			createToolRequest.GetDescriptorReference().GetMessage(),
		)
	})

	t.Run("applies the title option", func(t *testing.T) {
		createToolRequest := MessageToolRequest(&fieldmaskpb.FieldMask{}, WithTitle("a short summary"))
		require.Equal(t, "a short summary", createToolRequest.GetSchemaConfiguration().GetWithTitle())
	})
}

package tool

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	_ "github.com/malonaz/core/genproto/ai/ai_service/v1"
)

func method(t *testing.T, name protoreflect.FullName) protoreflect.MethodDescriptor {
	t.Helper()
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
	require.NoError(t, err)
	return descriptor.(protoreflect.MethodDescriptor)
}

// A method's tool states which method to call and whether it changes anything.
func TestToolMethod(t *testing.T) {
	getDecision := ToolMethod(method(t, "malonaz.ai.ai_service.v1.AiService.GetDecision"))
	require.Equal(t, "malonaz.ai.ai_service.v1.AiService", getDecision.GetService())
	require.Equal(t, "malonaz.ai.ai_service.v1.AiService.GetDecision", getDecision.GetMethod())
	require.True(t, getDecision.GetReadOnly())

	createDecision := ToolMethod(method(t, "malonaz.ai.ai_service.v1.AiService.CreateDecision"))
	require.False(t, createDecision.GetReadOnly())
}

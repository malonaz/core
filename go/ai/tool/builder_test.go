package tool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	_ "github.com/malonaz/core/genproto/ai/ai_service/v1"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

// newTestBuilder returns a builder over the ai service, as a target reflects it.
func newTestBuilder(t *testing.T) *Builder {
	t.Helper()
	services := []string{"malonaz.ai.ai_service.v1.AiService"}
	serviceInfoProvider, err := pbreflection.NewServiceInfoProvider(protoregistry.GlobalFiles, services)
	require.NoError(t, err)
	reflectionClient := pbreflection.NewServerReflectionClientInProc(reflection.ServerOptions{Services: serviceInfoProvider})
	schema, err := pbreflection.ResolveSchema(context.Background(), reflectionClient)
	require.NoError(t, err)
	return NewBuilder(schema)
}

func method(t *testing.T, name protoreflect.FullName) protoreflect.MethodDescriptor {
	t.Helper()
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
	require.NoError(t, err)
	return descriptor.(protoreflect.MethodDescriptor)
}

// A method's tool states what its callers act on: the method to call, whether it changes anything, whether they
// derive its request id.
func TestBuilder_ToolMethod(t *testing.T) {
	builder := newTestBuilder(t)

	getDecision := builder.ToolMethod(method(t, "malonaz.ai.ai_service.v1.AiService.GetDecision"))
	require.Equal(t, "malonaz.ai.ai_service.v1.AiService", getDecision.GetService())
	require.Equal(t, "malonaz.ai.ai_service.v1.AiService.GetDecision", getDecision.GetMethod())
	require.True(t, getDecision.GetReadOnly())
	require.False(t, getDecision.GetRequestId())

	createDecision := builder.ToolMethod(method(t, "malonaz.ai.ai_service.v1.AiService.CreateDecision"))
	require.False(t, createDecision.GetReadOnly())
	require.True(t, createDecision.GetRequestId())
}

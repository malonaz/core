package tool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	_ "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	_ "github.com/malonaz/core/genproto/test/library/library_service/v1"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

// newTestBuilder returns a builder over the library and ai services, as a target reflects them.
func newTestBuilder(t *testing.T) *Builder {
	t.Helper()
	services := []string{"malonaz.test.library.library_service.v1.LibraryService", "malonaz.ai.ai_service.v1.AiService"}
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

// A method's tool states what its descriptors declare: its AIP kind, the resource it acts on, what it supports.
func TestBuilder_ToolMethod(t *testing.T) {
	builder := newTestBuilder(t)

	listBooks := builder.ToolMethod(method(t, "malonaz.test.library.library_service.v1.LibraryService.ListBooks"))
	require.Equal(t, aipb.ToolMethod_KIND_LIST, listBooks.GetKind())
	require.Equal(t, "library.test.malonaz.com/Book", listBooks.GetResourceType())
	require.NotEmpty(t, listBooks.GetResourcePatterns())
	require.True(t, listBooks.GetPagination())
	require.True(t, listBooks.GetFilter())

	exportShelves := builder.ToolMethod(method(t, "malonaz.test.library.library_service.v1.LibraryService.ExportShelves"))
	require.Equal(t, aipb.ToolMethod_KIND_EXPORT, exportShelves.GetKind())
	require.True(t, exportShelves.GetLongRunning())

	getDecision := builder.ToolMethod(method(t, "malonaz.ai.ai_service.v1.AiService.GetDecision"))
	require.Equal(t, aipb.ToolMethod_KIND_GET, getDecision.GetKind())
	require.True(t, getDecision.GetReadOnly())

	// A custom method acts on the resource its request names.
	generateMessage := builder.ToolMethod(method(t, "malonaz.ai.ai_service.v1.AiService.GenerateMessage"))
	require.Equal(t, aipb.ToolMethod_KIND_CUSTOM, generateMessage.GetKind())
	require.Equal(t, "ai.malonaz.com/Chat", generateMessage.GetResourceType())
	require.False(t, generateMessage.GetReadOnly())
	require.Equal(t, "malonaz.ai.ai_service.v1.AiService", generateMessage.GetService())
}

package pbreflection

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
)

// fileDescriptorProtos returns fd and its transitive imports, as a reflection
// server would serve them.
func fileDescriptorProtos(fd protoreflect.FileDescriptor) []*descriptorpb.FileDescriptorProto {
	seen := map[string]bool{}
	var fdProtos []*descriptorpb.FileDescriptorProto
	var visit func(protoreflect.FileDescriptor)
	visit = func(fd protoreflect.FileDescriptor) {
		if seen[fd.Path()] {
			return
		}
		seen[fd.Path()] = true
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			visit(imports.Get(i).FileDescriptor)
		}
		fdProtos = append(fdProtos, protodesc.ToFileDescriptorProto(fd))
	}
	visit(fd)
	return fdProtos
}

func TestNewSchema_StandardMethodTypes(t *testing.T) {
	service := libraryservicepb.File_malonaz_test_library_library_service_v1_library_service_proto.Services().Get(0)
	schema, err := newSchema(&schemaData{
		FileDescriptors: fileDescriptorProtos(service.ParentFile()),
		ServiceSet:      []string{string(service.FullName())},
	})
	require.NoError(t, err)

	// Annotated methods must resolve to a type their name starts with, e.g. ExportBooks is Export.
	mismatches := map[protoreflect.Name]StandardMethodType{}
	methods := service.Methods()
	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		if !proto.HasExtension(method.Options(), aippb.E_StandardMethod) {
			continue
		}
		methodType, err := schema.GetStandardMethodType(method.FullName())
		require.NoError(t, err)
		if methodType == StandardMethodTypeUnspecified || !strings.HasPrefix(string(method.Name()), string(methodType)) {
			mismatches[method.Name()] = methodType
		}
	}
	require.Empty(t, mismatches)
}

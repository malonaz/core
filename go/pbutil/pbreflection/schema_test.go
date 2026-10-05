package pbreflection

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

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

	for methodName, expected := range map[protoreflect.Name]StandardMethodType{
		"CreateShelf":   StandardMethodTypeCreate,
		"GetShelf":      StandardMethodTypeGet,
		"ListShelves":   StandardMethodTypeList,
		"SearchAuthors": StandardMethodTypeSearch,
		"ImportBooks":   StandardMethodTypeImport,
		"ExportShelves": StandardMethodTypeExport,
		"ExportBooks":   StandardMethodTypeExport,
	} {
		t.Run(string(methodName), func(t *testing.T) {
			methodType, err := schema.GetStandardMethodType(service.FullName().Append(methodName))
			require.NoError(t, err)
			require.Equal(t, expected, methodType)
		})
	}
}

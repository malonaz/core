package pbutils

import (
	"strings"

	"github.com/mennanov/fmutils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

// ApplyMask filters a proto message with the given paths.
// Note that the given paths are structured as follow: "a.b,a.c" etc.
func ApplyMask(message proto.Message, paths string) {
	mask := fmutils.NestedMaskFromPaths(strings.Split(paths, ","))
	mask.Filter(message)
}

// ApplyMaskInverse prunes a proto message with the given paths.
// Note that the given paths are structured as follow: "a.b,a.c" etc.
func ApplyMaskInverse(message proto.Message, paths string) {
	mask := fmutils.NestedMaskFromPaths(strings.Split(paths, ","))
	mask.Prune(message)
}

type enum interface {
	protoreflect.Enum
	String() string
}

// MustGetEnumValueOption returns the enum value option or panics.
func MustGetEnumValueOption(enum enum, extensionInfo *protoimpl.ExtensionInfo) interface{} {
	enumDescriptor := enum.Descriptor()
	valueEnumDescriptor := enumDescriptor.Values().ByName(protoreflect.Name(enum.String()))
	options := valueEnumDescriptor.Options().(*descriptorpb.EnumValueOptions)
	return proto.GetExtension(options, extensionInfo)
}

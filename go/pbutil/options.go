package pbutil

import (
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/descriptorpb"
)

type enum interface {
	protoreflect.Enum
	String() string
}

// MustGetServiceOption returns the service option or panics.
func MustGetServiceOption(
	serviceName string,
	extensionInfo *protoimpl.ExtensionInfo,
) any {
	serviceOption, ok := GetServiceOption(serviceName, extensionInfo)
	if !ok {
		panic("could not find service option")
	}
	return serviceOption
}

// MustGetServiceOption returns the service option or panics.
func GetServiceOption(
	serviceName string,
	extensionInfo *protoimpl.ExtensionInfo,
) (any, bool) {
	fd, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		panic("could not find service descriptor: " + err.Error())
	}
	serviceDescriptor, ok := fd.(protoreflect.ServiceDescriptor)
	if !ok {
		panic(fmt.Errorf("descriptor is not a service descriptor for service: %s", serviceName))
	}

	options, ok := serviceDescriptor.Options().(*descriptorpb.ServiceOptions)
	if !ok {
		return nil, false
	}
	extension := proto.GetExtension(options, extensionInfo)
	if extension == nil {
		return nil, false
	}
	return extension, true
}

// MustGetEnumValueOption returns the enum value option or panics.
func MustGetEnumValueOption(enum enum, extensionInfo *protoimpl.ExtensionInfo) any {
	enumDescriptor := enum.Descriptor()
	valueEnumDescriptor := enumDescriptor.Values().ByName(protoreflect.Name(enum.String()))
	options := valueEnumDescriptor.Options().(*descriptorpb.EnumValueOptions)
	return proto.GetExtension(options, extensionInfo)
}

// MustGetMessageOption returns an option for the given message.
func MustGetMessageOption(m proto.Message, extensionInfo *protoimpl.ExtensionInfo) any {
	options := m.ProtoReflect().Descriptor().Options()
	if options != nil {
		if err := protovalidate.Validate(options); err != nil {
			panic(fmt.Errorf("validating message option: %w", err))
		}
	}
	return proto.GetExtension(options, extensionInfo)
}

// GetMessageOption returns an option for the given message.
// Returns an error if validation fails or if the type assertion fails.
func GetMessageOption[T proto.Message](m proto.Message, extensionInfo *protoimpl.ExtensionInfo) (T, error) {
	var zero T
	options := m.ProtoReflect().Descriptor().Options()
	if options != nil {
		if err := protovalidate.Validate(options); err != nil {
			return zero, fmt.Errorf("validating message option: %w", err)
		}
	}

	ext := proto.GetExtension(options, extensionInfo)
	result, ok := ext.(T)
	if !ok {
		return zero, fmt.Errorf("failed to cast extension to type %T", zero)
	}

	return result, nil
}

// GetEnumValueOption returns the enum value option along with any error encountered.
func GetEnumValueOption(enum enum, extensionInfo *protoimpl.ExtensionInfo) (any, error) {
	enumDescriptor := enum.Descriptor()
	valueEnumDescriptor := enumDescriptor.Values().ByName(protoreflect.Name(enum.String()))
	if valueEnumDescriptor == nil {
		return nil, fmt.Errorf("enum value descriptor for %v not found", enum.String())
	}
	options, ok := valueEnumDescriptor.Options().(*descriptorpb.EnumValueOptions)
	if !ok || options == nil {
		return nil, fmt.Errorf("enum value options for %v not found or wrong type", enum.String())
	}
	extension := proto.GetExtension(options, extensionInfo)
	if extension == nil {
		return nil, fmt.Errorf("extension is undefined for %v", enum.String())
	}
	return extension, nil
}

func SanitizeEnumString(enum, prefix string) string {
	enum = strings.TrimPrefix(enum, prefix)
	enum = strings.ReplaceAll(enum, "_", " ")
	enum = strings.ToLower(enum)
	return enum
}

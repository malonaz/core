package pbutil

import (
	"fmt"

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

func Must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func GetServiceOption[T any](serviceName string, extensionInfo *protoimpl.ExtensionInfo) (T, error) {
	var zero T
	fd, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return zero, fmt.Errorf("could not find service descriptor: %w", err)
	}
	serviceDescriptor, ok := fd.(protoreflect.ServiceDescriptor)
	if !ok {
		return zero, fmt.Errorf("descriptor is not a service descriptor for service: %s", serviceName)
	}
	options, ok := serviceDescriptor.Options().(*descriptorpb.ServiceOptions)
	if !ok || options == nil {
		return zero, fmt.Errorf("service options for %s not found or wrong type", serviceName)
	}
	ext := proto.GetExtension(options, extensionInfo)
	result, ok := ext.(T)
	if !ok {
		return zero, fmt.Errorf("failed to cast extension to type %T", zero)
	}
	return result, nil
}

// GetMessageOption returns an option for the given message.
// Returns an error if validation fails or if the type assertion fails.
func GetMessageOption[T any](m proto.Message, extensionInfo *protoimpl.ExtensionInfo) (T, error) {
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

func GetEnumValueOption[T any](enum enum, extensionInfo *protoimpl.ExtensionInfo) (T, error) {
	var zero T
	enumDescriptor := enum.Descriptor()
	valueEnumDescriptor := enumDescriptor.Values().ByName(protoreflect.Name(enum.String()))
	if valueEnumDescriptor == nil {
		return zero, fmt.Errorf("enum value descriptor for %v not found", enum.String())
	}
	options, ok := valueEnumDescriptor.Options().(*descriptorpb.EnumValueOptions)
	if !ok || options == nil {
		return zero, fmt.Errorf("enum value options for %v not found or wrong type", enum.String())
	}
	ext := proto.GetExtension(options, extensionInfo)
	result, ok := ext.(T)
	if !ok {
		return zero, fmt.Errorf("failed to cast extension to type %T", zero)
	}
	return result, nil
}

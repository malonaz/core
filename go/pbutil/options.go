package pbutil

import (
	"fmt"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
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

func HasExtension(m proto.Message, extensionType protoreflect.ExtensionType) bool {
	return proto.HasExtension(m, extensionType)
}

func GetExtension[T any](options proto.Message, extensionType protoreflect.ExtensionType) (T, error) {
	var zero T
	ext := proto.GetExtension(options, extensionType)
	result, ok := ext.(T)
	if !ok {
		return zero, fmt.Errorf("failed to cast extension to type %T", zero)
	}
	if m, ok := any(result).(proto.Message); ok {
		if err := protovalidate.Validate(m); err != nil {
			return zero, fmt.Errorf("validating extension: %w", err)
		}
	}
	return result, nil
}

func GetServiceOption[T any](serviceName string, extensionType protoreflect.ExtensionType) (T, error) {
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
	return GetExtension[T](options, extensionType)
}

func GetMessageOption[T any](m proto.Message, extensionType protoreflect.ExtensionType) (T, error) {
	var zero T
	options := m.ProtoReflect().Descriptor().Options()
	if options == nil {
		return zero, fmt.Errorf("message options not found")
	}
	return GetExtension[T](options, extensionType)
}

func GetEnumValueOption[T any](enum enum, extensionType protoreflect.ExtensionType) (T, error) {
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
	return GetExtension[T](options, extensionType)
}

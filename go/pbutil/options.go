package pbutil

import (
	"errors"
	"fmt"
	"reflect"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

var ErrExtensionNotFound = errors.New("extension not found")

type enum interface {
	protoreflect.Enum
	String() string
}

type optionConfig struct {
	registry *protoregistry.Files
}

type OptionConfigOpt func(*optionConfig)

func WithRegistry(registry *protoregistry.Files) OptionConfigOpt {
	return func(configuration *optionConfig) {
		configuration.registry = registry
	}
}

func newOptionConfig(opts []OptionConfigOpt) *optionConfig {
	configuration := &optionConfig{
		registry: protoregistry.GlobalFiles,
	}
	for _, opt := range opts {
		opt(configuration)
	}
	return configuration
}

func Must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func GetExtension[T any](options proto.Message, extensionType protoreflect.ExtensionType) (T, error) {
	var zero T
	if !proto.HasExtension(options, extensionType) {
		return zero, ErrExtensionNotFound
	}
	ext := proto.GetExtension(options, extensionType)
	result, ok := ext.(T)
	if !ok {
		return zero, fmt.Errorf("failed to cast extension to type %T", zero)
	}
	if m, ok := any(result).(proto.Message); ok {
		if err := protovalidate.Validate(m); err != nil {
			return zero, fmt.Errorf("validating extension: %w", err)
		}
	} else {
		rv := reflect.ValueOf(result)
		if rv.Kind() == reflect.Slice {
			for i := 0; i < rv.Len(); i++ {
				if m, ok := rv.Index(i).Interface().(proto.Message); ok {
					if err := protovalidate.Validate(m); err != nil {
						return zero, fmt.Errorf("validating extension[%d]: %w", i, err)
					}
				}
			}
		}
	}
	return result, nil
}

func GetServiceOption[T any](serviceName string, extensionType protoreflect.ExtensionType, opts ...OptionConfigOpt) (T, error) {
	var zero T
	configuration := newOptionConfig(opts)
	fd, err := configuration.registry.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return zero, fmt.Errorf("could not find service descriptor: %w", err)
	}
	serviceDescriptor, ok := fd.(protoreflect.ServiceDescriptor)
	if !ok {
		return zero, fmt.Errorf("descriptor is not a service descriptor for service: %s", serviceName)
	}
	options, ok := serviceDescriptor.Options().(*descriptorpb.ServiceOptions)
	if !ok {
		return zero, fmt.Errorf("service options for %s has wrong type", serviceName)
	}
	return GetExtension[T](options, extensionType)
}

func GetMethodOption[T any](methodName string, extensionType protoreflect.ExtensionType, opts ...OptionConfigOpt) (T, error) {
	var zero T
	configuration := newOptionConfig(opts)
	fd, err := configuration.registry.FindDescriptorByName(protoreflect.FullName(methodName))
	if err != nil {
		return zero, fmt.Errorf("could not find method descriptor: %w", err)
	}
	methodDescriptor, ok := fd.(protoreflect.MethodDescriptor)
	if !ok {
		return zero, fmt.Errorf("descriptor is not a method descriptor for method: %s", methodName)
	}
	options, ok := methodDescriptor.Options().(*descriptorpb.MethodOptions)
	if !ok {
		return zero, fmt.Errorf("method options for %s has wrong type", methodName)
	}
	return GetExtension[T](options, extensionType)
}

func GetMessageOption[T any](m proto.Message, extensionType protoreflect.ExtensionType) (T, error) {
	options := m.ProtoReflect().Descriptor().Options()
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

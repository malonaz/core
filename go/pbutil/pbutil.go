package pbutil

import (
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	"github.com/mennanov/fmutils"
	"go.einride.tech/aip/fieldmask"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type enum interface {
	protoreflect.Enum
	String() string
}

// MustGetServiceOption returns the service option or panics.
func MustGetServiceOption(
	serviceName string,
	extensionInfo *protoimpl.ExtensionInfo,
) interface{} {
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
) (interface{}, bool) {
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
func MustGetEnumValueOption(enum enum, extensionInfo *protoimpl.ExtensionInfo) interface{} {
	enumDescriptor := enum.Descriptor()
	valueEnumDescriptor := enumDescriptor.Values().ByName(protoreflect.Name(enum.String()))
	options := valueEnumDescriptor.Options().(*descriptorpb.EnumValueOptions)
	return proto.GetExtension(options, extensionInfo)
}

// MustGetMessageOption returns an option for the given message.
func MustGetMessageOption(m proto.Message, extensionInfo *protoimpl.ExtensionInfo) interface{} {
	options := m.ProtoReflect().Descriptor().Options()
	if options != nil {
		if err := protovalidate.Validate(options); err != nil {
			panic(fmt.Errorf("validating message option: %w", err))
		}
	}
	return proto.GetExtension(options, extensionInfo)
}

// GetEnumValueOption returns the enum value option along with any error encountered.
func GetEnumValueOption(enum enum, extensionInfo *protoimpl.ExtensionInfo) (interface{}, error) {
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

func ValidateMask(message proto.Message, paths string) error {
	fieldMask := &fieldmaskpb.FieldMask{Paths: strings.Split(paths, ",")}
	return fieldmask.Validate(fieldMask, message)
}

func ExtractConcreteMessageFromAnyMessage(anyMessage *anypb.Any) (proto.Message, error) {
	// Get the message type
	mt, err := protoregistry.GlobalTypes.FindMessageByURL(anyMessage.TypeUrl)
	if err != nil {
		return nil, fmt.Errorf("unknown type %s: %v", anyMessage.TypeUrl, err)
	}
	// Create a new instance of the message
	message := mt.New().Interface()
	// Unmarshal the Any message
	if err := anyMessage.UnmarshalTo(message); err != nil {
		return nil, err
	}
	return message, nil
}

// ApplyMaskAny handles an any message elegantly.
func ApplyMaskAny(anyMessage *anypb.Any, paths string) error {
	// Get the message type
	mt, err := protoregistry.GlobalTypes.FindMessageByURL(anyMessage.TypeUrl)
	if err != nil {
		return fmt.Errorf("unknown type %s: %v", anyMessage.TypeUrl, err)
	}
	// Create a new instance of the message
	maskedMessage := mt.New().Interface()
	// Unmarshal the Any message
	if err := anyMessage.UnmarshalTo(maskedMessage); err != nil {
		return err
	}
	// Apply the mask.
	if err := ApplyMask(maskedMessage, paths); err != nil {
		return err
	}
	anyMessage.Reset()
	return anyMessage.MarshalFrom(maskedMessage)
}

// ApplyMask filters a proto message with the given paths.
// Note that the given paths are structured as follow: "a.b,a.c" etc.
func ApplyMask(message proto.Message, paths string) error {
	if err := ValidateMask(message, paths); err != nil {
		return fmt.Errorf("validating field mask: %w", err)
	}
	mask := fmutils.NestedMaskFromPaths(strings.Split(paths, ","))
	mask.Filter(message)
	return nil
}

// ApplyMaskInverse prunes a proto message with the given paths.
// Note that the given paths are structured as follow: "a.b,a.c" etc.
func ApplyMaskInverse(message proto.Message, paths string) error {
	if err := ValidateMask(message, paths); err != nil {
		return fmt.Errorf("validating field mask: %w", err)
	}
	mask := fmutils.NestedMaskFromPaths(strings.Split(paths, ","))
	mask.Prune(message)
	return nil
}

type NestedFieldMask struct {
	nm fmutils.NestedMask
}

func MustNewNestedFieldMask(message proto.Message, paths string) *NestedFieldMask {
	nestedFieldMask, err := NewNestedFieldMask(message, paths)
	if err != nil {
		panic(err)
	}
	return nestedFieldMask
}

func NewNestedFieldMask(message proto.Message, paths string) (*NestedFieldMask, error) {
	if err := ValidateMask(message, paths); err != nil {
		return nil, fmt.Errorf("validating field mask: %w", err)
	}
	nm := fmutils.NestedMaskFromPaths(strings.Split(paths, ","))
	return &NestedFieldMask{nm: nm}, nil
}

func (m *NestedFieldMask) ApplyInverse(message proto.Message) {
	m.nm.Prune(message)
}

func SanitizeEnumString(enum, prefix string) string {
	enum = strings.TrimPrefix(enum, prefix)
	enum = strings.ReplaceAll(enum, "_", " ")
	enum = strings.ToLower(enum)
	return enum
}

// ///////////////////////////////// MARSHALING ///////////////////////////////////
var marshalOptions = &proto.MarshalOptions{}

func Marshal(m proto.Message) ([]byte, error) {
	return marshalOptions.Marshal(m)
}

var marshalDeterministicOptions = &proto.MarshalOptions{
	Deterministic: true,
}

func MarshalDeterministic(m proto.Message) ([]byte, error) {
	return marshalDeterministicOptions.Marshal(m)
}

var unmarshalOptions = &proto.UnmarshalOptions{
	DiscardUnknown: true,
}

func Unmarshal(b []byte, m proto.Message) error {
	return unmarshalOptions.Unmarshal(b, m)
}

var ProtoJsonUnmarshalOptions = protojson.UnmarshalOptions{
	DiscardUnknown: true,
}

func JSONUnmarshal(b []byte, m proto.Message) error {
	return ProtoJsonUnmarshalOptions.Unmarshal(b, m)
}

var ProtoJsonUnmarshalStrictOptions = protojson.UnmarshalOptions{
	DiscardUnknown: false,
}

func JSONUnmarshalStrict(b []byte, m proto.Message) error {
	return ProtoJsonUnmarshalStrictOptions.Unmarshal(b, m)
}

var ProtoJsonMarshalOptions = protojson.MarshalOptions{
	UseProtoNames: true,
}

func JSONMarshal(m proto.Message) ([]byte, error) {
	return ProtoJsonMarshalOptions.Marshal(m)
}

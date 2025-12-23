package pbutil

import (
	"fmt"
	"strings"

	"github.com/mennanov/fmutils"
	"go.einride.tech/aip/fieldmask"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func ValidateMask(message proto.Message, paths string) error {
	fieldMask := &fieldmaskpb.FieldMask{Paths: strings.Split(paths, ",")}
	return fieldmask.Validate(fieldMask, message)
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

type FieldMaskOptions struct {
	OnlySet bool
	Parent  string
}

type FieldMaskOption func(*FieldMaskOptions)

func WithOnlySet() FieldMaskOption {
	return func(o *FieldMaskOptions) {
		o.OnlySet = true
	}
}

func WithParent(parent string) FieldMaskOption {
	return func(o *FieldMaskOptions) {
		o.Parent = parent
	}
}

// GenerateFieldMaskPaths returns all possible field mask paths for a proto message.
func GenerateFieldMaskPaths(message proto.Message, opts ...FieldMaskOption) []string {
	options := &FieldMaskOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var paths []string
	generatePaths(message.ProtoReflect(), "", options, &paths)
	return paths
}

func generatePaths(m protoreflect.Message, prefix string, opts *FieldMaskOptions, paths *[]string) {
	md := m.Descriptor()
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)

		if opts.OnlySet && !m.Has(field) {
			continue
		}

		path := string(field.Name())
		if prefix != "" {
			path = prefix + "." + path
		}
		if opts.Parent != "" {
			path = opts.Parent + "." + path
		}
		if field.Kind() == protoreflect.MessageKind && !field.IsMap() && !field.IsList() {
			initialLen := len(*paths)
			generatePaths(m.Get(field).Message(), path, opts, paths)
			// If no children were added, add the parent path.
			if len(*paths) == initialLen {
				*paths = append(*paths, path)
			}
		} else {
			*paths = append(*paths, path)
		}
	}
}

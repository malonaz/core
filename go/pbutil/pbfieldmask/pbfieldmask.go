package pbfieldmask

import (
	"fmt"
	"strings"
	"sync"

	"github.com/mennanov/fmutils"
	"go.einride.tech/aip/fieldmask"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// WildcardPath is the special "*" path that matches all fields.
const WildcardPath = fieldmask.WildcardPath

// FieldMask wraps a protobuf FieldMask with lazy-initialized nested mask support
// for efficient filtering and pruning operations.
type FieldMask struct {
	pb         *fieldmaskpb.FieldMask
	nested     fmutils.NestedMask
	nestedOnce sync.Once
}

// New creates a FieldMask from an existing protobuf FieldMask, normalizing paths
// to remove redundancies (e.g. "a.b" is removed if "a" is already present).
func New(fm *fieldmaskpb.FieldMask) *FieldMask {
	fm.Normalize()
	return &FieldMask{
		pb: fm,
	}
}

// FromPaths creates a FieldMask from a variadic list of dot-separated field paths.
func FromPaths(paths ...string) *FieldMask {
	return New(&fieldmaskpb.FieldMask{Paths: paths})
}

// FromString creates a FieldMask by splitting a comma-separated string of paths.
func FromString(s string) *FieldMask {
	return FromPaths(strings.Split(s, ",")...)
}

// getNestedMask lazily initializes and returns the nested mask representation,
// which enables efficient per-field filtering and pruning on proto messages.
func (m *FieldMask) getNestedMask() fmutils.NestedMask {
	m.nestedOnce.Do(func() {
		m.nested = fmutils.NestedMaskFromPaths(m.GetPaths())
	})
	return m.nested
}

// fieldMaskOptions holds configuration for field mask generation from a message.
type fieldMaskOptions struct {
	OnlySet bool
}

// FieldMaskOption is a functional option for configuring FromMessage behavior.
type FieldMaskOption func(*fieldMaskOptions)

// WithOnlySet restricts FromMessage to only include paths for fields that are
// explicitly populated (non-default) in the source message.
func WithOnlySet() FieldMaskOption {
	return func(o *fieldMaskOptions) {
		o.OnlySet = true
	}
}

// FromMessage derives a FieldMask by introspecting a proto message's fields.
// By default all fields are included; use WithOnlySet to include only populated fields.
func FromMessage(message proto.Message, opts ...FieldMaskOption) *FieldMask {
	options := &fieldMaskOptions{}
	for _, opt := range opts {
		opt(options)
	}
	var paths []string
	generateFieldMaskPaths(message.ProtoReflect(), "", options, &paths)
	return FromPaths(paths...)
}

// WithParent prefixes every path in the mask with the given parent path.
// Panics if the mask is a wildcard, since "*" cannot be scoped under a parent.
func (m *FieldMask) WithParent(parent string) *FieldMask {
	if m.IsWildcardPath() {
		panic("cannot call WithParent on wildcard path")
	}
	paths := m.pb.GetPaths()
	newPaths := make([]string, len(paths))
	for i, p := range paths {
		newPaths[i] = parent + "." + p
	}
	return FromPaths(newPaths...)
}

// IsWildcardPath returns true if the mask consists of exactly the "*" path,
// indicating all fields should be included.
func (m *FieldMask) IsWildcardPath() bool {
	return len(m.pb.GetPaths()) == 1 && m.pb.GetPaths()[0] == WildcardPath
}

// Proto returns the underlying protobuf FieldMask message.
func (m *FieldMask) Proto() *fieldmaskpb.FieldMask {
	return m.pb
}

// GetPaths returns the list of dot-separated field paths in the mask.
func (m *FieldMask) GetPaths() []string {
	return m.pb.GetPaths()
}

// String returns a comma-separated representation of all paths in the mask.
func (m *FieldMask) String() string {
	return strings.Join(m.pb.GetPaths(), ",")
}

// Contains returns true if the given path is targeted by the mask. A path is
// considered targeted if the mask is a wildcard, contains the exact path,
// contains a parent of the path, or contains a child of the path.
func (m *FieldMask) Contains(path string) bool {
	for _, p := range m.pb.GetPaths() {
		if p == WildcardPath || p == path || strings.HasPrefix(path, p+".") || strings.HasPrefix(p, path+".") {
			return true
		}
	}
	return false
}

// Validate checks that every path in the mask corresponds to a valid field
// on the given proto message type.
func (m *FieldMask) Validate(message proto.Message) error {
	return fieldmask.Validate(m.pb, message)
}

// MustValidate is like Validate but panics on error. Returns the mask for chaining.
func (m *FieldMask) MustValidate(message proto.Message) *FieldMask {
	if err := m.Validate(message); err != nil {
		panic(err)
	}
	return m
}

// Update copies fields from src into dest according to the mask.
// Nested messages are updated recursively. Repeated fields and maps are copied
// by reference. If the mask is empty, only non-zero values from src are copied.
// The wildcard "*" path triggers a full replacement of all fields in dest.
func (m *FieldMask) Update(dest, src proto.Message) {
	fieldmask.Update(m.Proto(), dest, src)
}

// Apply retains only the fields specified by the mask, clearing everything else.
// A wildcard mask is a no-op (all fields are retained).
func (m *FieldMask) Apply(message proto.Message) {
	if m.IsWildcardPath() {
		return
	}
	m.getNestedMask().Filter(message)
}

// ApplyInverse removes the fields specified by the mask, retaining everything else.
// A wildcard mask resets the entire message.
func (m *FieldMask) ApplyInverse(message proto.Message) {
	if m.IsWildcardPath() {
		proto.Reset(message)
		return
	}
	m.getNestedMask().Prune(message)
}

// ApplyAny applies the mask to a message wrapped in an anypb.Any. The Any is
// unmarshaled, filtered, and re-marshaled in place. Returns an error if the
// type URL is unregistered or unmarshaling fails.
func (m *FieldMask) ApplyAny(anyMessage *anypb.Any) error {
	mt, err := protoregistry.GlobalTypes.FindMessageByURL(anyMessage.TypeUrl)
	if err != nil {
		return fmt.Errorf("unknown type %s: %v", anyMessage.TypeUrl, err)
	}
	maskedMessage := mt.New().Interface()
	if err := anyMessage.UnmarshalTo(maskedMessage); err != nil {
		return err
	}
	m.Apply(maskedMessage)
	anyMessage.Reset()
	return anyMessage.MarshalFrom(maskedMessage)
}

// generateFieldMaskPaths recursively walks a proto message's descriptor to
// collect dot-separated field paths. Maps, lists, and well-known leaf types
// (e.g. Timestamp, Duration) are emitted as leaf paths without descending
// further. Empty nested messages are emitted as a single path to the message
// itself rather than being omitted entirely.
func generateFieldMaskPaths(m protoreflect.Message, prefix string, opts *fieldMaskOptions, paths *[]string) {
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

		switch {
		case field.IsMap(), field.IsList():
			*paths = append(*paths, path)

		case field.Kind() == protoreflect.MessageKind:
			if _, ok := wellKnownLeafTypes[field.Message().FullName()]; ok {
				*paths = append(*paths, path)
				continue
			}
			initialLen := len(*paths)
			generateFieldMaskPaths(m.Get(field).Message(), path, opts, paths)
			if len(*paths) == initialLen {
				*paths = append(*paths, path)
			}

		default:
			*paths = append(*paths, path)
		}
	}
}

// wellKnownLeafTypes lists proto message types that should be treated as atomic
// leaf values rather than descended into during field mask generation. These are
// standard wrapper types, well-known types, and utility types from the protobuf
// well-known type library.
var wellKnownLeafTypes = map[protoreflect.FullName]struct{}{
	"google.protobuf.Timestamp":   {},
	"google.protobuf.Duration":    {},
	"google.protobuf.Any":         {},
	"google.protobuf.Struct":      {},
	"google.protobuf.Value":       {},
	"google.protobuf.ListValue":   {},
	"google.protobuf.BoolValue":   {},
	"google.protobuf.BytesValue":  {},
	"google.protobuf.DoubleValue": {},
	"google.protobuf.FloatValue":  {},
	"google.protobuf.Int32Value":  {},
	"google.protobuf.Int64Value":  {},
	"google.protobuf.StringValue": {},
	"google.protobuf.UInt32Value": {},
	"google.protobuf.UInt64Value": {},
	"google.protobuf.FieldMask":   {},
	"google.protobuf.Empty":       {},
}

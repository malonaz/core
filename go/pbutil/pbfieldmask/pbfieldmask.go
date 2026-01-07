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

const WildcardPath = fieldmask.WildcardPath

type FieldMask struct {
	pb         *fieldmaskpb.FieldMask
	nested     fmutils.NestedMask
	nestedOnce sync.Once
}

func FromFieldMask(fm *fieldmaskpb.FieldMask) *FieldMask {
	fm.Normalize()
	return &FieldMask{
		pb: fm,
	}
}

func FromPaths(paths ...string) *FieldMask {
	return FromFieldMask(&fieldmaskpb.FieldMask{Paths: paths})
}

func FromString(s string) *FieldMask {
	return FromPaths(strings.Split(s, ",")...)
}

func (m *FieldMask) getNestedMask() fmutils.NestedMask {
	m.nestedOnce.Do(func() {
		m.nested = fmutils.NestedMaskFromPaths(m.GetPaths())
	})
	return m.nested
}

type fieldMaskOptions struct {
	OnlySet bool
}

type FieldMaskOption func(*fieldMaskOptions)

func WithOnlySet() FieldMaskOption {
	return func(o *fieldMaskOptions) {
		o.OnlySet = true
	}
}

func FromMessage(message proto.Message, opts ...FieldMaskOption) *FieldMask {
	options := &fieldMaskOptions{}
	for _, opt := range opts {
		opt(options)
	}
	var paths []string
	generateFieldMaskPaths(message.ProtoReflect(), "", options, &paths)
	return FromPaths(paths...)
}

func (m *FieldMask) WithParent(parent string) *FieldMask {
	if m.IsWildcardPath() {
		panic("cannot call WithParent on wildcard path")
	}
	paths := m.pb.GetPaths()
	newPaths := make([]string, len(paths))
	for i, p := range paths {
		newPaths[i] = parent + "." + p
	}
	*m = *FromPaths(newPaths...)
	return m
}

func (m *FieldMask) IsWildcardPath() bool {
	return len(m.pb.GetPaths()) == 1 && m.pb.GetPaths()[0] == WildcardPath
}

func (m *FieldMask) Proto() *fieldmaskpb.FieldMask {
	return m.pb
}

func (m *FieldMask) GetPaths() []string {
	return m.pb.GetPaths()
}

func (m *FieldMask) String() string {
	return strings.Join(m.pb.GetPaths(), ",")
}

func (m *FieldMask) Validate(message proto.Message) error {
	return fieldmask.Validate(m.pb, message)
}

func (m *FieldMask) MustValidate(message proto.Message) *FieldMask {
	if err := m.Validate(message); err != nil {
		panic(err)
	}
	return m
}

// Update updates fields in dst with values from src according to the provided field mask.
// Nested messages are recursively updated in the same manner.
// Repeated fields and maps are copied by reference from src to dst.
// Field mask paths referring to Individual entries in maps or repeated fields are ignored.
// If no update mask is provided, only non-zero values of src are copied to dst.
// If the special value "*" is provided as the field mask, a full replacement of all fields in dst is done.
func (m *FieldMask) Update(dest, src proto.Message) {
	fieldmask.Update(m.Proto(), dest, src)
}

func (m *FieldMask) Apply(message proto.Message) {
	if m.IsWildcardPath() {
		return
	}
	m.getNestedMask().Filter(message)
}

func (m *FieldMask) ApplyInverse(message proto.Message) {
	if m.IsWildcardPath() {
		proto.Reset(message)
		return
	}
	m.getNestedMask().Prune(message)
}

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

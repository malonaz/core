package pbjson

import (
	"fmt"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

const (
	defaultMaxDepth = 10
)

var (
	timestampFullName = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().FullName()
	durationFullName  = (&durationpb.Duration{}).ProtoReflect().Descriptor().FullName()
	fieldMaskFullName = (&fieldmaskpb.FieldMask{}).ProtoReflect().Descriptor().FullName()
	dateFullName      = (&date.Date{}).ProtoReflect().Descriptor().FullName()
	timeOfDayFullName = (&timeofday.TimeOfDay{}).ProtoReflect().Descriptor().FullName()
)

type SchemaBuilder struct {
	schema   *pbreflection.Schema
	maxDepth int
}

type Option func(*SchemaBuilder)

func WithMaxDepth(depth int) Option {
	return func(b *SchemaBuilder) {
		b.maxDepth = depth
	}
}

func NewSchemaBuilder(schema *pbreflection.Schema, opts ...Option) *SchemaBuilder {
	b := &SchemaBuilder{schema: schema, maxDepth: defaultMaxDepth}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

type schemaOptions struct {
	fieldMask *fieldmaskpb.FieldMask
}

type SchemaOption func(*schemaOptions)

func WithFieldMaskPaths(paths ...string) SchemaOption {
	return func(o *schemaOptions) {
		o.fieldMask = &fieldmaskpb.FieldMask{
			Paths: paths,
		}
	}
}

func (b *SchemaBuilder) BuildSchema(messageFullName protoreflect.FullName, methodType pbreflection.StandardMethodType, opts ...SchemaOption) (*jsonpb.Schema, error) {
	var so schemaOptions
	for _, opt := range opts {
		opt(&so)
	}

	desc, err := b.schema.FindDescriptorByName(messageFullName)
	if err != nil {
		return nil, fmt.Errorf("message not found: %s", messageFullName)
	}
	msg, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor is not a message: %s", messageFullName)
	}

	// Validate the field mask.
	allowedPaths := make(map[string]bool)
	if len(so.fieldMask.GetPaths()) > 0 {
		fieldMask := pbfieldmask.FromFieldMask(so.fieldMask)
		if err := fieldMask.Validate(dynamicpb.NewMessage(msg)); err != nil {
			return nil, fmt.Errorf("invalid field mask: %w", err)
		}
		for _, path := range fieldMask.GetPaths() {
			allowedPaths[path] = true
		}
	}

	return b.buildMessageSchema(msg, "", 0, methodType, allowedPaths), nil
}

func (b *SchemaBuilder) buildMessageSchema(
	msg protoreflect.MessageDescriptor, prefix string, depth int, methodType pbreflection.StandardMethodType, allowedPaths map[string]bool,
) *jsonpb.Schema {
	switch msg.FullName() {
	case timestampFullName:
		return &jsonpb.Schema{Type: "string", Description: "RFC3339, e.g. 2006-01-02T15:04:05Z"}
	case durationFullName:
		return &jsonpb.Schema{Type: "string", Description: "e.g. 1h30m"}
	case fieldMaskFullName:
		return &jsonpb.Schema{Type: "string", Description: "comma-separated paths"}
	case dateFullName:
		return &jsonpb.Schema{Type: "string", Description: "YYYY-MM-DD, e.g. 2006-01-02"}
	case timeOfDayFullName:
		return &jsonpb.Schema{Type: "string", Description: "HH:MM:SS, e.g. 15:04:05"}
	}

	properties := make(map[string]*jsonpb.Schema)
	var required []string

	fields := msg.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		schema, isRequired := b.buildFieldSchema(field, prefix, depth, methodType, allowedPaths)
		if schema != nil {
			properties[string(field.Name())] = schema
			if isRequired {
				required = append(required, string(field.Name()))
			}
		}
	}

	return &jsonpb.Schema{
		Type:        "object",
		Description: b.schema.GetComment(msg.FullName(), pbreflection.CommentStyleMultiline),
		Properties:  properties,
		Required:    required,
	}
}

func (b *SchemaBuilder) buildFieldSchema(field protoreflect.FieldDescriptor, prefix string, depth int, methodType pbreflection.StandardMethodType, allowedPaths map[string]bool) (*jsonpb.Schema, bool) {
	if depth > b.maxDepth {
		return nil, false
	}

	path := prefix + string(field.Name())

	if len(allowedPaths) > 0 && !isPathAllowed(path, allowedPaths) {
		return nil, false
	}

	behaviors := getFieldBehaviors(field)
	if behaviors.outputOnly {
		return nil, false
	}
	var isRequired bool
	switch methodType {
	case pbreflection.StandardMethodTypeCreate:
		if behaviors.identifier {
			return nil, false
		}
		isRequired = behaviors.required
	case pbreflection.StandardMethodTypeUpdate:
		if behaviors.immutable {
			return nil, false
		}
		isRequired = behaviors.identifier
	default:
		isRequired = behaviors.required
	}

	description := b.schema.GetComment(field.FullName(), pbreflection.CommentStyleMultiline)

	if field.IsList() {
		return &jsonpb.Schema{
			Type:        "array",
			Description: description,
			Items:       b.elementSchema(field, path, depth, methodType, allowedPaths),
		}, isRequired
	}

	if field.Kind() == protoreflect.MessageKind {
		schema := b.buildMessageSchema(field.Message(), path+".", depth+1, methodType, allowedPaths)
		if description != "" {
			schema.Description = description + " (" + schema.Description + ")"
		}
		return schema, isRequired
	}

	return b.scalarSchema(field, description), isRequired
}

func (b *SchemaBuilder) scalarSchema(field protoreflect.FieldDescriptor, description string) *jsonpb.Schema {
	schema := &jsonpb.Schema{Description: description}
	switch field.Kind() {
	case protoreflect.BoolKind:
		schema.Type = "boolean"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		schema.Type = "integer"
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		schema.Type = "number"
	case protoreflect.EnumKind:
		schema.Type = "string"
		values := field.Enum().Values()
		for i := 0; i < values.Len(); i++ {
			schema.Enum = append(schema.Enum, string(values.Get(i).Name()))
		}
	default:
		schema.Type = "string"
	}
	return schema
}

func (b *SchemaBuilder) elementSchema(field protoreflect.FieldDescriptor, prefix string, depth int, methodType pbreflection.StandardMethodType, allowedPaths map[string]bool) *jsonpb.Schema {
	if field.Kind() == protoreflect.MessageKind {
		return b.buildMessageSchema(field.Message(), prefix+".", depth+1, methodType, allowedPaths)
	}
	return b.scalarSchema(field, "")
}

type fieldBehaviors struct {
	required   bool
	outputOnly bool
	immutable  bool
	identifier bool
}

func getFieldBehaviors(field protoreflect.FieldDescriptor) fieldBehaviors {
	var fb fieldBehaviors
	opts := field.Options()
	if opts == nil {
		return fb
	}
	if !proto.HasExtension(opts, annotations.E_FieldBehavior) {
		return fb
	}
	behaviors := proto.GetExtension(opts, annotations.E_FieldBehavior).([]annotations.FieldBehavior)
	for _, behavior := range behaviors {
		switch behavior {
		case annotations.FieldBehavior_REQUIRED:
			fb.required = true
		case annotations.FieldBehavior_OUTPUT_ONLY:
			fb.outputOnly = true
		case annotations.FieldBehavior_IMMUTABLE:
			fb.immutable = true
		case annotations.FieldBehavior_IDENTIFIER:
			fb.identifier = true
		}
	}
	return fb
}

func isPathAllowed(path string, allowedPaths map[string]bool) bool {
	if allowedPaths[path] {
		return true
	}
	for allowed := range allowedPaths {
		// Check if this path is a parent of an allowed path
		if strings.HasPrefix(allowed, path+".") {
			return true
		}
		// Check if path is a descendant of an allowed path (allows all children)
		if strings.HasPrefix(path, allowed+".") {
			return true
		}
	}
	return false
}

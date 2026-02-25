package pbjson

import (
	"fmt"
	"strings"

	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

const (
	defaultMaxDepth = 5
)

var (
	timestampFullName = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().FullName()
	durationFullName  = (&durationpb.Duration{}).ProtoReflect().Descriptor().FullName()
	fieldMaskFullName = (&fieldmaskpb.FieldMask{}).ProtoReflect().Descriptor().FullName()
	dateFullName      = (&date.Date{}).ProtoReflect().Descriptor().FullName()
	timeOfDayFullName = (&timeofday.TimeOfDay{}).ProtoReflect().Descriptor().FullName()
)

type SchemaBuilder struct {
	schema *pbreflection.Schema
}

func NewSchemaBuilder(schema *pbreflection.Schema) *SchemaBuilder {
	return &SchemaBuilder{schema: schema}
}

type schemaOptions struct {
	maxDepth                   int
	fieldMask                  *fieldmaskpb.FieldMask
	withResponseReadMask       bool
	withResponseSchemaMaxDepth int
}

type SchemaOption func(*schemaOptions)

// Include a response schema in the tool description.
func WithResponseSchemaMaxDepth(maxDepth int) SchemaOption {
	return func(o *schemaOptions) {
		o.withResponseSchemaMaxDepth = maxDepth
	}
}

func WithResponseReadMask() SchemaOption {
	return func(o *schemaOptions) {
		o.withResponseReadMask = true
	}
}

func WithMaxDepth(maxDepth int) SchemaOption {
	return func(o *schemaOptions) {
		o.maxDepth = maxDepth
	}
}

func WithFieldMaskPaths(paths ...string) SchemaOption {
	return func(o *schemaOptions) {
		o.fieldMask = &fieldmaskpb.FieldMask{
			Paths: paths,
		}
	}
}

func (b *SchemaBuilder) BuildSchema(descriptorFullName protoreflect.FullName, opts ...SchemaOption) (*jsonpb.Schema, error) {
	so := &schemaOptions{maxDepth: defaultMaxDepth}
	for _, opt := range opts {
		opt(so)
	}

	desc, err := b.schema.FindDescriptorByName(descriptorFullName)
	if err != nil {
		return nil, fmt.Errorf("descriptor not found: %s", descriptorFullName)
	}

	var msg protoreflect.MessageDescriptor
	var standardMethodType pbreflection.StandardMethodType
	var responseDesc string

	switch d := desc.(type) {
	case protoreflect.MessageDescriptor:
		msg = d
		standardMethodType = pbreflection.StandardMethodTypeUnspecified
	case protoreflect.MethodDescriptor:
		msg = d.Input()
		var err error
		standardMethodType, err = b.schema.GetStandardMethodType(d.FullName())
		if err != nil {
			return nil, fmt.Errorf("getting standard method type %q: %v", d.FullName(), err)
		}
		if so.withResponseSchemaMaxDepth > 0 {
			responseSchema, err := b.BuildSchema(d.Output().FullName(), WithMaxDepth(so.withResponseSchemaMaxDepth))
			if err != nil {
				return nil, fmt.Errorf("building response schema: %v", err)
			}
			bytes, err := pbutil.JSONMarshal(responseSchema)
			if err != nil {
				return nil, fmt.Errorf("marshaling response schema: %v", err)
			}
			if d.Input().FullName() == d.Output().FullName() {
				responseDesc = fmt.Sprintf("Returns the same object type as the input. JSON schema (max_depth=%d):\n%s", so.withResponseSchemaMaxDepth, bytes)
			} else {
				responseDesc = fmt.Sprintf("Response JSON schema (max_depth=%d):\n%s", so.withResponseSchemaMaxDepth, bytes)
			}
		}
	default:
		return nil, fmt.Errorf("descriptor is not a message or method: %s", descriptorFullName)
	}

	allowedPaths := make(map[string]bool)
	if len(so.fieldMask.GetPaths()) > 0 {
		fieldMask := pbfieldmask.New(so.fieldMask)
		if err := fieldMask.Validate(dynamicpb.NewMessage(msg)); err != nil {
			return nil, fmt.Errorf("invalid field mask: %w", err)
		}
		for _, path := range fieldMask.GetPaths() {
			allowedPaths[path] = true
		}
	}

	schema, err := b.buildMessageSchema(so, msg, "", 0, standardMethodType, allowedPaths)
	if err != nil {
		return nil, fmt.Errorf("building message schema %q: %w", msg.FullName(), err)
	}
	if so.withResponseReadMask {
		schema.Properties[responseReadMaskKey] = &jsonpb.Schema{
			Type:        "string",
			Description: "Google AIP field mask: comma-separated snake_case paths to include in the response (e.g. 'field_one,field_two.nested_field'). Only specified fields are returned. Nested fields use dot notation. Omit to return all fields.",
		}
		schema.Required = append(schema.Required, responseReadMaskKey)
	}

	if responseDesc != "" {
		if schema.Description != "" {
			schema.Description += "\n\n"
		}
		schema.Description += responseDesc
	}
	return schema, nil
}

func (b *SchemaBuilder) buildMessageSchema(
	so *schemaOptions, msg protoreflect.MessageDescriptor, prefix string, depth int, methodType pbreflection.StandardMethodType, allowedPaths map[string]bool,
) (*jsonpb.Schema, error) {
	switch msg.FullName() {
	case timestampFullName:
		return &jsonpb.Schema{Type: "string", Description: "RFC3339, e.g. 2006-01-02T15:04:05Z"}, nil
	case durationFullName:
		return &jsonpb.Schema{Type: "string", Description: "e.g. 1h30m"}, nil
	case fieldMaskFullName:
		return &jsonpb.Schema{Type: "string", Description: "comma-separated paths"}, nil
	case dateFullName:
		return &jsonpb.Schema{Type: "string", Description: "YYYY-MM-DD, e.g. 2006-01-02"}, nil
	case timeOfDayFullName:
		return &jsonpb.Schema{Type: "string", Description: "HH:MM:SS, e.g. 15:04:05"}, nil
	}

	properties := make(map[string]*jsonpb.Schema)
	var required []string

	fields := msg.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		schema, isRequired, err := b.buildFieldSchema(so, field, prefix, depth+1, methodType, allowedPaths)
		if err != nil {
			return nil, fmt.Errorf("building field %q schema: %w", field.Name(), err)
		}
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
	}, nil
}

func (b *SchemaBuilder) buildFieldSchema(so *schemaOptions, fieldDescriptor protoreflect.FieldDescriptor, prefix string, depth int, methodType pbreflection.StandardMethodType, allowedPaths map[string]bool) (*jsonpb.Schema, bool, error) {
	// Depth chech.
	if depth > so.maxDepth {
		return nil, false, nil
	}

	// Construct the field path and check that it is allowed.
	path := prefix + string(fieldDescriptor.Name())
	if len(allowedPaths) > 0 && !isPathAllowed(path, allowedPaths) {
		return nil, false, nil
	}

	fieldBehavior, err := pbutil.GetFieldBehavior(fieldDescriptor)
	if err != nil {
		return nil, false, fmt.Errorf("getting field behavior: %w", err)
	}
	if fieldBehavior.OutputOnly {
		return nil, false, nil
	}

	var isRequired bool
	switch methodType {
	case pbreflection.StandardMethodTypeCreate:
		if fieldBehavior.Identifier {
			return nil, false, nil
		}
		isRequired = fieldBehavior.Required
	case pbreflection.StandardMethodTypeUpdate:
		if fieldBehavior.Immutable {
			return nil, false, nil
		}
		isRequired = fieldBehavior.Identifier
	default:
		isRequired = fieldBehavior.Required
	}

	description := b.schema.GetComment(fieldDescriptor.FullName(), pbreflection.CommentStyleMultiline)

	if fieldDescriptor.IsMap() {
		additionalProperties, err := b.elementSchema(so, fieldDescriptor.MapValue(), path, depth, methodType, allowedPaths)
		if err != nil {
			return nil, false, fmt.Errorf("building element schema %q: %q", fieldDescriptor.FullName(), err)
		}

		return &jsonpb.Schema{
			Type:                 "object",
			Description:          description,
			AdditionalProperties: additionalProperties,
		}, isRequired, nil
	}

	if fieldDescriptor.IsList() {
		items, err := b.elementSchema(so, fieldDescriptor, path, depth, methodType, allowedPaths)
		if err != nil {
			return nil, false, fmt.Errorf("building element schema %q: %q", fieldDescriptor.FullName(), err)
		}
		return &jsonpb.Schema{
			Type:        "array",
			Description: description,
			Items:       items,
		}, isRequired, nil
	}

	if fieldDescriptor.Kind() == protoreflect.MessageKind {
		schema, err := b.buildMessageSchema(so, fieldDescriptor.Message(), path+".", depth, methodType, allowedPaths)
		if err != nil {
			return nil, false, fmt.Errorf("building message %q schema: %w", fieldDescriptor.Message().FullName(), err)
		}
		if description != "" {
			schema.Description = description + " (" + schema.Description + ")"
		}
		return schema, isRequired, nil
	}

	return b.scalarSchema(fieldDescriptor, description), isRequired, nil
}

func (b *SchemaBuilder) scalarSchema(fieldDescriptor protoreflect.FieldDescriptor, description string) *jsonpb.Schema {
	schema := &jsonpb.Schema{Description: description}
	switch fieldDescriptor.Kind() {
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
		values := fieldDescriptor.Enum().Values()
		for i := 0; i < values.Len(); i++ {
			schema.Enum = append(schema.Enum, string(values.Get(i).Name()))
		}
	default:
		schema.Type = "string"
	}
	return schema
}

func (b *SchemaBuilder) elementSchema(so *schemaOptions, fieldDescriptor protoreflect.FieldDescriptor, prefix string, depth int, methodType pbreflection.StandardMethodType, allowedPaths map[string]bool) (*jsonpb.Schema, error) {
	if fieldDescriptor.Kind() == protoreflect.MessageKind {
		return b.buildMessageSchema(so, fieldDescriptor.Message(), prefix+".", depth, methodType, allowedPaths)
	}
	return b.scalarSchema(fieldDescriptor, ""), nil
}

func isPathAllowed(path string, allowedPaths map[string]bool) bool {
	if allowedPaths[path] {
		return true
	}
	for allowed := range allowedPaths {
		if strings.HasPrefix(allowed, path+".") {
			return true
		}
		if strings.HasPrefix(path, allowed+".") {
			return true
		}
	}
	return false
}

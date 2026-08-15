package pbjson

import (
	"fmt"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/genproto/googleapis/type/money"
	"google.golang.org/genproto/googleapis/type/postaladdress"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
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
	timestampFullName     = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().FullName()
	decimalFullName       = (&decimal.Decimal{}).ProtoReflect().Descriptor().FullName()
	durationFullName      = (&durationpb.Duration{}).ProtoReflect().Descriptor().FullName()
	fieldMaskFullName     = (&fieldmaskpb.FieldMask{}).ProtoReflect().Descriptor().FullName()
	dateFullName          = (&date.Date{}).ProtoReflect().Descriptor().FullName()
	timeOfDayFullName     = (&timeofday.TimeOfDay{}).ProtoReflect().Descriptor().FullName()
	moneyFullName         = (&money.Money{}).ProtoReflect().Descriptor().FullName()
	postalAddressFullName = (&postaladdress.PostalAddress{}).ProtoReflect().Descriptor().FullName()
	anyFullName           = (&anypb.Any{}).ProtoReflect().Descriptor().FullName()
	rpcStatusFullName     = (&rpcstatus.Status{}).ProtoReflect().Descriptor().FullName()
	structFullName        = (&structpb.Struct{}).ProtoReflect().Descriptor().FullName()
	valueFullName         = (&structpb.Value{}).ProtoReflect().Descriptor().FullName()
	listValueFullName     = (&structpb.ListValue{}).ProtoReflect().Descriptor().FullName()
	jsonSchemaFullName    = (&jsonpb.Schema{}).ProtoReflect().Descriptor().FullName()
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
	titleDescription           string
	withResponseSchemaMaxDepth int
	responseDescriptor         protoreflect.MessageDescriptor
}

type SchemaOption func(*schemaOptions)

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

// WithTitle injects a required `tool_call_title` field documented with the given description,
// which the caller uses to tell the model what to put there (e.g. a user-facing summary of the action).
func WithTitle(description string) SchemaOption {
	return func(o *schemaOptions) {
		o.titleDescription = description
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
		so.responseDescriptor = d.Output()
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
			Description: buildResponseReadMaskDescription(so.responseDescriptor),
		}
		schema.Required = append(schema.Required, responseReadMaskKey)
	}
	if so.titleDescription != "" {
		// Guard against shadowing a genuine request field of the same name.
		if _, ok := schema.Properties[titleKey]; ok {
			return nil, fmt.Errorf("cannot inject %q: message %s already has a field with that name", titleKey, msg.FullName())
		}
		schema.Properties[titleKey] = &jsonpb.Schema{
			Type:        "string",
			Description: appendDescription("Generate this field first, before any other field.", so.titleDescription),
		}
		schema.Required = append(schema.Required, titleKey)
	}

	if responseDesc != "" {
		if schema.Description != "" {
			schema.Description += "\n\n"
		}
		schema.Description += responseDesc
	}
	return schema, nil
}

// buildResponseReadMaskDescription builds a context-aware description for the response_read_mask
// field. It inspects the response message to find the resource that the mask will be applied to,
// mirroring the middleware's resource-finding logic.
func buildResponseReadMaskDescription(responseDescriptor protoreflect.MessageDescriptor) string {
	base := "Comma-separated snake_case field paths controlling which fields are returned. Uses dot notation for nested fields. Use the wildcard `*` to return all fields."

	if responseDescriptor == nil {
		return base
	}

	// Check if the response itself is a resource.
	if hasResourceAnnotation(responseDescriptor) {
		return base + fmt.Sprintf(" Paths are relative to the %s resource.", responseDescriptor.Name()) + buildResourceFieldExample(responseDescriptor)
	}

	// Scan one level deep for a resource field, matching the middleware's logic.
	fields := responseDescriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Kind() != protoreflect.MessageKind {
			continue
		}
		if !hasResourceAnnotation(field.Message()) {
			continue
		}
		resourceName := field.Message().Name()
		if field.IsList() {
			return base + fmt.Sprintf(" Paths are relative to each %s resource in the response (not the response envelope).", resourceName) + buildResourceFieldExample(field.Message())
		}
		return base + fmt.Sprintf(" Paths are relative to the %s resource in the response (not the response envelope).", resourceName) + buildResourceFieldExample(field.Message())
	}

	return base
}

// buildResourceFieldExample returns an example string showing a few field names from the resource.
func buildResourceFieldExample(resourceDescriptor protoreflect.MessageDescriptor) string {
	var fieldNames []string
	fields := resourceDescriptor.Fields()
	for i := 0; i < fields.Len() && len(fieldNames) < 3; i++ {
		field := fields.Get(i)
		fieldNames = append(fieldNames, string(field.Name()))
	}
	if len(fieldNames) == 0 {
		return ""
	}
	return fmt.Sprintf(" Example: '%s'.", strings.Join(fieldNames, ","))
}

func hasResourceAnnotation(descriptor protoreflect.MessageDescriptor) bool {
	_, err := pbutil.GetExtension[*annotations.ResourceDescriptor](descriptor.Options(), annotations.E_Resource)
	return err == nil
}

// postalAddressSchema returns a hand-pruned schema for google.type.PostalAddress. The upstream
// message carries ~2.5KB of field comments across 11 fields, which dominates any tool schema that
// embeds an address and crowds out the fields the model actually needs.
//
// The exposed subset is exactly what every consumer renders: address_lines, locality,
// administrative_area, postal_code and region_code (see formatPostalAddress in onikisu
// app/core/format.ts and app/kanshi/src/lib/format.ts, and the address line in
// onikisu user/tmpl/utils.tmpl). The omitted fields are either fixed (revision), formatting-only
// (language_code), or region-specific edge cases (sorting_code, sublocality, recipients,
// organization).
func postalAddressSchema() *jsonpb.Schema {
	return &jsonpb.Schema{
		Type:        "object",
		Description: "A postal address. Populate region_code plus as much structure as is known; anything that does not fit the other fields goes in address_lines.",
		Properties: map[string]*jsonpb.Schema{
			"region_code": {
				Type:        "string",
				Description: `CLDR region code of the country/region, e.g. "US", "CH".`,
			},
			"postal_code": {
				Type:        "string",
				Description: "Postal/ZIP code.",
			},
			"administrative_area": {
				Type:        "string",
				Description: `State/province/prefecture, e.g. "CA". Omit for countries that do not use one, such as Switzerland.`,
			},
			"locality": {
				Type:        "string",
				Description: "City/town.",
			},
			"address_lines": {
				Type:        "array",
				Description: `Street address lines, in envelope order for the country, e.g. ["1 Market St", "Suite 300"].`,
				Items:       &jsonpb.Schema{Type: "string"},
			},
		},
		Required: []string{"region_code"},
	}
}

// rpcStatusSchema returns a hand-pruned schema for google.rpc.Status. The message itself is small,
// but its `details` field is a repeated google.protobuf.Any, which recurses into an opaque
// type_url/value pair carrying ~5KB of upstream documentation. Details cannot meaningfully be
// produced by a model anyway, so we expose only the code and message.
func rpcStatusSchema() *jsonpb.Schema {
	return &jsonpb.Schema{
		Type:        "object",
		Description: "An error status.",
		Properties: map[string]*jsonpb.Schema{
			"code": {
				Type:        "integer",
				Description: "The canonical google.rpc.Code, e.g. 0 (OK), 3 (INVALID_ARGUMENT), 5 (NOT_FOUND), 7 (PERMISSION_DENIED), 13 (INTERNAL).",
			},
			"message": {
				Type:        "string",
				Description: "A developer-facing error message, in English.",
			},
		},
	}
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
	case moneyFullName:
		return &jsonpb.Schema{Type: "string", Description: "ISO 4217 currency code followed by amount, e.g. 'USD 25.50', 'EUR -1.75', 'JPY 1000'"}, nil
	case postalAddressFullName:
		return postalAddressSchema(), nil
	case rpcStatusFullName:
		return rpcStatusSchema(), nil
	case anyFullName:
		// Any is an opaque type_url + serialized bytes pair that a model cannot fill in, and it
		// drags ~5KB of upstream documentation into the schema. Describe it rather than recurse.
		return &jsonpb.Schema{Type: "object", Description: "An arbitrary serialized protobuf message (google.protobuf.Any)."}, nil
	case structFullName:
		return &jsonpb.Schema{Type: "object", Description: "JSON object (google.protobuf.Struct)"}, nil
	case valueFullName:
		return &jsonpb.Schema{Type: "object", Description: "JSON value (google.protobuf.Value)"}, nil
	case listValueFullName:
		return &jsonpb.Schema{Type: "array", Description: "JSON array (google.protobuf.ListValue)"}, nil
	case jsonSchemaFullName:
		return &jsonpb.Schema{Type: "object", Description: "JSON Schema object"}, nil
	case decimalFullName:
		return &jsonpb.Schema{Type: "string", Description: "x.x eg '10', '15.12'"}, nil
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

	description := b.schema.GetComment(msg.FullName(), pbreflection.CommentStyleMultiline)
	// Oneofs have no JSON Schema equivalent, so we describe them in prose.
	oneofDescription, err := describeOneofs(msg)
	if err != nil {
		return nil, fmt.Errorf("describing oneofs of %q: %w", msg.FullName(), err)
	}

	return &jsonpb.Schema{
		Type:        "object",
		Description: appendDescription(description, oneofDescription),
		Properties:  properties,
		Required:    required,
	}, nil
}

func (b *SchemaBuilder) buildFieldSchema(so *schemaOptions, fieldDescriptor protoreflect.FieldDescriptor, prefix string, depth int, methodType pbreflection.StandardMethodType, allowedPaths map[string]bool) (*jsonpb.Schema, bool, error) {
	if depth > so.maxDepth {
		return nil, false, nil
	}

	path := prefix + string(fieldDescriptor.Name())
	if len(allowedPaths) > 0 && !isPathAllowed(path, allowedPaths) {
		return nil, false, nil
	}

	fieldBehavior, err := pbutil.GetFieldBehavior(fieldDescriptor)
	if err != nil {
		return nil, false, fmt.Errorf("getting field behavior: %w", err)
	}

	fieldRules, err := getFieldRules(fieldDescriptor)
	if err != nil {
		return nil, false, fmt.Errorf("getting field rules %q: %w", fieldDescriptor.FullName(), err)
	}

	mustIncludeFieldSchema := len(allowedPaths) > 0 // We always respect the allowed path (specified by the mask).
	if fieldBehavior.OutputOnly && !mustIncludeFieldSchema {
		return nil, false, nil
	}

	var isRequired bool
	switch methodType {
	case pbreflection.StandardMethodTypeCreate:
		if fieldBehavior.Identifier && !mustIncludeFieldSchema {
			return nil, false, nil
		}
		isRequired = fieldBehavior.Required
	case pbreflection.StandardMethodTypeUpdate:
		if fieldBehavior.Immutable && !mustIncludeFieldSchema {
			return nil, false, nil
		}
		isRequired = fieldBehavior.Identifier
	default:
		isRequired = fieldBehavior.Required
	}
	// buf.validate and google.api.field_behavior are independent sources of requiredness.
	isRequired = isRequired || isRequiredRule(fieldRules)

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
		schema := &jsonpb.Schema{
			Type:        "array",
			Description: description,
			Items:       items,
		}
		applyRepeatedRules(schema, fieldDescriptor, fieldRules)
		schema.Description = appendDescription(schema.Description, describeConstraints(schema))
		return schema, isRequired, nil
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

	schema := b.scalarSchema(fieldDescriptor, description)
	applyScalarRules(schema, fieldDescriptor, fieldRules)
	schema.Description = appendDescription(schema.Description, describeConstraints(schema))
	return schema, isRequired, nil
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

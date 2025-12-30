package pbai

import (
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

var (
	timestampFullName = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().FullName()
	durationFullName  = (&durationpb.Duration{}).ProtoReflect().Descriptor().FullName()
	fieldMaskFullName = (&fieldmaskpb.FieldMask{}).ProtoReflect().Descriptor().FullName()
)

type ToolBuilderOption func(*toolBuilderOptions)

type toolBuilderOptions struct {
	fieldMask map[string]struct{}
	maxDepth  int
}

func WithFieldMask(paths string) ToolBuilderOption {
	return func(o *toolBuilderOptions) {
		if paths == "" {
			return
		}
		o.fieldMask = make(map[string]struct{})
		for _, p := range strings.Split(paths, ",") {
			o.fieldMask[strings.TrimSpace(p)] = struct{}{}
		}
	}
}

func WithMaxDepth(depth int) ToolBuilderOption {
	return func(o *toolBuilderOptions) {
		o.maxDepth = depth
	}
}

type ToolBuilder struct {
	schema *pbreflection.Schema
}

func NewToolBuilder(schema *pbreflection.Schema) *ToolBuilder {
	return &ToolBuilder{schema: schema}
}

func (b *ToolBuilder) BuildAll(opts ...BuildAllOption) []*aipb.Tool {
	options := &buildAllOptions{maxDepth: 10}
	for _, opt := range opts {
		opt(options)
	}

	var tools []*aipb.Tool
	b.schema.Services(func(svc protoreflect.ServiceDescriptor) bool {
		if !options.serviceAllowed(string(svc.FullName())) {
			return true
		}
		methods := svc.Methods()
		for i := 0; i < methods.Len(); i++ {
			tools = append(tools, b.Build(methods.Get(i), WithMaxDepth(options.maxDepth)))
		}
		return true
	})
	return tools
}

type BuildAllOption func(*buildAllOptions)

type buildAllOptions struct {
	services map[string]struct{}
	maxDepth int
}

func (o *buildAllOptions) serviceAllowed(name string) bool {
	if o.services == nil {
		return true
	}
	_, ok := o.services[name]
	return ok
}

func WithServices(services ...string) BuildAllOption {
	return func(o *buildAllOptions) {
		o.services = make(map[string]struct{})
		for _, s := range services {
			o.services[s] = struct{}{}
		}
	}
}

func WithBuildAllMaxDepth(depth int) BuildAllOption {
	return func(o *buildAllOptions) {
		o.maxDepth = depth
	}
}

func (b *ToolBuilder) Build(method protoreflect.MethodDescriptor, opts ...ToolBuilderOption) *aipb.Tool {
	options := &toolBuilderOptions{maxDepth: 10}
	for _, opt := range opts {
		opt(options)
	}

	svc := method.Parent().(protoreflect.ServiceDescriptor)
	name := string(svc.Name()) + "_" + string(method.Name())
	description := b.schema.Comments[string(method.FullName())]
	methodName := string(method.Name())

	schema := b.buildMessageSchema(method.Input(), "", 0, methodName, options)

	return &aipb.Tool{
		Name:        name,
		Description: description,
		JsonSchema:  schema,
	}
}

func (b *ToolBuilder) buildMessageSchema(msg protoreflect.MessageDescriptor, prefix string, depth int, methodName string, options *toolBuilderOptions) *aipb.JsonSchema {
	properties := make(map[string]*aipb.JsonSchema)
	var required []string

	fields := msg.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		schema, isRequired := b.buildFieldSchema(field, prefix, depth, methodName, options)
		if schema != nil {
			properties[string(field.Name())] = schema
			if isRequired {
				required = append(required, string(field.Name()))
			}
		}
	}

	return &aipb.JsonSchema{
		Type:       "object",
		Properties: properties,
		Required:   required,
	}
}

func (b *ToolBuilder) buildFieldSchema(field protoreflect.FieldDescriptor, prefix string, depth int, methodName string, options *toolBuilderOptions) (*aipb.JsonSchema, bool) {
	if depth > options.maxDepth {
		return nil, false
	}

	behaviors := getFieldBehaviors(field)
	if behaviors.outputOnly {
		return nil, false
	}

	isCreate := strings.HasPrefix(methodName, "Create")
	isUpdate := strings.HasPrefix(methodName, "Update")
	if isCreate && behaviors.identifier {
		return nil, false
	}
	if isUpdate && behaviors.immutable {
		return nil, false
	}

	path := prefix + string(field.Name())
	if !b.fieldAllowed(path, options) {
		return nil, false
	}

	description := b.schema.Comments[string(field.FullName())]
	isRequired := behaviors.required || (isUpdate && behaviors.identifier)

	if field.IsList() {
		return &aipb.JsonSchema{
			Type:        "array",
			Description: description,
			Items:       b.elementSchema(field, path, depth, methodName, options),
		}, isRequired
	}

	if field.Kind() == protoreflect.MessageKind {
		switch field.Message().FullName() {
		case timestampFullName:
			return &aipb.JsonSchema{
				Type:        "string",
				Description: description + " (RFC3339, e.g. 2006-01-02T15:04:05Z)",
			}, isRequired
		case durationFullName:
			return &aipb.JsonSchema{
				Type:        "string",
				Description: description + " (e.g. 1h30m)",
			}, isRequired
		case fieldMaskFullName:
			return &aipb.JsonSchema{
				Type:        "string",
				Description: description + " (comma-separated paths)",
			}, isRequired
		default:
			schema := b.buildMessageSchema(field.Message(), path+".", depth+1, methodName, options)
			schema.Description = description
			return schema, isRequired
		}
	}

	return b.scalarSchema(field, description), isRequired
}

func (b *ToolBuilder) fieldAllowed(name string, options *toolBuilderOptions) bool {
	if options.fieldMask == nil {
		return true
	}
	if _, ok := options.fieldMask[name]; ok {
		return true
	}
	for masked := range options.fieldMask {
		if strings.HasPrefix(masked, name+".") {
			return true
		}
	}
	return false
}

func (b *ToolBuilder) scalarSchema(field protoreflect.FieldDescriptor, description string) *aipb.JsonSchema {
	schema := &aipb.JsonSchema{Description: description}
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

func (b *ToolBuilder) elementSchema(field protoreflect.FieldDescriptor, prefix string, depth int, methodName string, options *toolBuilderOptions) *aipb.JsonSchema {
	if field.Kind() == protoreflect.MessageKind {
		return b.buildMessageSchema(field.Message(), prefix+".", depth+1, methodName, options)
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

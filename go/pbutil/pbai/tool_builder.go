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

func toolName(svcName, methodName protoreflect.Name) string {
	return string(string(svcName) + "_" + string(methodName))
}

func (m *ToolManager) buildMethodTool(method protoreflect.MethodDescriptor) *aipb.Tool {
	svc := method.Parent().(protoreflect.ServiceDescriptor)
	description := m.schema.GetComment(method.FullName(), pbreflection.CommentStyleMultiline)
	methodName := string(method.Name())

	schema := m.buildMessageSchema(method.Input(), "", 0, methodName)

	return &aipb.Tool{
		Name:        toolName(svc.Name(), method.Name()),
		Description: description,
		JsonSchema:  schema,
		Metadata: map[string]string{
			annotationKeyType:   annotationValueToolTypeMethod,
			annotationKeyMethod: string(method.FullName()),
		},
	}
}

func (m *ToolManager) buildMessageSchema(msg protoreflect.MessageDescriptor, prefix string, depth int, methodName string) *aipb.JsonSchema {
	properties := make(map[string]*aipb.JsonSchema)
	var required []string

	fields := msg.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		schema, isRequired := m.buildFieldSchema(field, prefix, depth, methodName)
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

func (m *ToolManager) buildFieldSchema(field protoreflect.FieldDescriptor, prefix string, depth int, methodName string) (*aipb.JsonSchema, bool) {
	if depth > m.maxDepth {
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
	description := m.schema.GetComment(field.FullName(), pbreflection.CommentStyleMultiline)
	isRequired := behaviors.required || (isUpdate && behaviors.identifier)

	if field.IsList() {
		return &aipb.JsonSchema{
			Type:        "array",
			Description: description,
			Items:       m.elementSchema(field, path, depth, methodName),
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
			schema := m.buildMessageSchema(field.Message(), path+".", depth+1, methodName)
			schema.Description = description
			return schema, isRequired
		}
	}

	return m.scalarSchema(field, description), isRequired
}

func (m *ToolManager) scalarSchema(field protoreflect.FieldDescriptor, description string) *aipb.JsonSchema {
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

func (m *ToolManager) elementSchema(field protoreflect.FieldDescriptor, prefix string, depth int, methodName string) *aipb.JsonSchema {
	if field.Kind() == protoreflect.MessageKind {
		return m.buildMessageSchema(field.Message(), prefix+".", depth+1, methodName)
	}
	return m.scalarSchema(field, "")
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

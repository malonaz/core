package pbai

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

const (
	annotationKeyMethod = "malonaz.pbai.method"
)

type ToolExecutor struct {
	schema  *pbreflection.Schema
	invoker *pbreflection.MethodInvoker
}

func NewToolExecutor(schema *pbreflection.Schema, invoker *pbreflection.MethodInvoker) *ToolExecutor {
	return &ToolExecutor{schema: schema, invoker: invoker}
}

func (e *ToolExecutor) Execute(ctx context.Context, toolCall *aipb.ToolCall) (proto.Message, error) {
	if toolCall.Metadata == nil {
		return nil, fmt.Errorf("tool call %s missing metadata ", toolCall.Name)
	}
	methodFQN, ok := toolCall.Metadata[annotationKeyMethod]
	if !ok {
		return nil, fmt.Errorf("tool call %s missing method annotation", toolCall.Name)
	}

	desc, err := e.schema.Files.FindDescriptorByName(protoreflect.FullName(methodFQN))
	if err != nil {
		return nil, fmt.Errorf("method not found: %s", methodFQN)
	}
	method, ok := desc.(protoreflect.MethodDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor is not a method: %s", methodFQN)
	}

	req := dynamicpb.NewMessage(method.Input())
	if err := populateMessage(req, toolCall.Arguments.AsMap()); err != nil {
		return nil, fmt.Errorf("populating request: %w", err)
	}

	return e.invoker.Invoke(ctx, method, req)
}

func populateMessage(msg *dynamicpb.Message, args map[string]any) error {
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		val, ok := args[string(field.Name())]
		if !ok {
			continue
		}
		if err := setField(msg, field, val); err != nil {
			return err
		}
	}
	return nil
}

func setField(msg *dynamicpb.Message, field protoreflect.FieldDescriptor, val any) error {
	if field.IsList() {
		arr, ok := val.([]any)
		if !ok {
			return fmt.Errorf("expected array for %s", field.Name())
		}
		list := msg.Mutable(field).List()
		for _, item := range arr {
			v, err := convertValue(field, item)
			if err != nil {
				return err
			}
			list.Append(v)
		}
		return nil
	}

	if field.Kind() == protoreflect.MessageKind && !field.IsMap() {
		nested, ok := val.(map[string]any)
		if !ok {
			return fmt.Errorf("expected object for %s", field.Name())
		}
		nestedMsg := msg.Mutable(field).Message().(*dynamicpb.Message)
		return populateMessage(nestedMsg, nested)
	}

	v, err := convertValue(field, val)
	if err != nil {
		return err
	}
	msg.Set(field, v)
	return nil
}

func convertValue(field protoreflect.FieldDescriptor, val any) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		s, _ := val.(string)
		return protoreflect.ValueOfString(s), nil
	case protoreflect.BoolKind:
		b, _ := val.(bool)
		return protoreflect.ValueOfBool(b), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		f, _ := val.(float64)
		return protoreflect.ValueOfInt32(int32(f)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		f, _ := val.(float64)
		return protoreflect.ValueOfInt64(int64(f)), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		f, _ := val.(float64)
		return protoreflect.ValueOfUint32(uint32(f)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		f, _ := val.(float64)
		return protoreflect.ValueOfUint64(uint64(f)), nil
	case protoreflect.FloatKind:
		f, _ := val.(float64)
		return protoreflect.ValueOfFloat32(float32(f)), nil
	case protoreflect.DoubleKind:
		f, _ := val.(float64)
		return protoreflect.ValueOfFloat64(f), nil
	case protoreflect.EnumKind:
		s, _ := val.(string)
		enumVal := field.Enum().Values().ByName(protoreflect.Name(s))
		if enumVal == nil {
			return protoreflect.Value{}, fmt.Errorf("unknown enum value: %s", s)
		}
		return protoreflect.ValueOfEnum(enumVal.Number()), nil
	case protoreflect.MessageKind:
		nested, ok := val.(map[string]any)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected object for message field %s", field.Name())
		}
		nestedMsg := dynamicpb.NewMessage(field.Message())
		if err := populateMessage(nestedMsg, nested); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(nestedMsg), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field kind: %v", field.Kind())
	}
}

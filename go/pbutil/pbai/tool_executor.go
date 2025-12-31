package pbai

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
)

func (m *ToolManager) executeDiscovery(toolCall *aipb.ToolCall) (proto.Message, error) {
	serviceFQN, ok := toolCall.Metadata[annotationKeyService]
	if !ok {
		return nil, fmt.Errorf("discovery tool missing service annotation")
	}

	args := toolCall.Arguments.AsMap()
	methodsRaw, ok := args["methods"]
	if !ok {
		return nil, fmt.Errorf("discovery tool missing methods argument")
	}

	methodsArr, ok := methodsRaw.([]any)
	if !ok {
		return nil, fmt.Errorf("methods must be an array")
	}

	var methods []string
	for _, v := range methodsArr {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("method name must be a string")
		}
		methods = append(methods, s)
	}

	if err := m.enableMethods(serviceFQN, methods); err != nil {
		return nil, err
	}

	return structpb.NewStringValue("ok"), nil
}

func (m *ToolManager) executeMethod(ctx context.Context, toolCall *aipb.ToolCall) (proto.Message, error) {
	methodFQN := toolCall.Metadata[annotationKeyMethod]
	if methodFQN == "" {
		return nil, fmt.Errorf("tool call %s missing method annotation", toolCall.Name)
	}

	desc, err := m.schema.FindDescriptorByName(protoreflect.FullName(methodFQN))
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

	return m.invoker.Invoke(ctx, method, req)
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
		return setMessageField(msg, field, val)
	}

	v, err := convertValue(field, val)
	if err != nil {
		return err
	}
	msg.Set(field, v)
	return nil
}

func setMessageField(msg *dynamicpb.Message, field protoreflect.FieldDescriptor, val any) error {
	switch field.Message().FullName() {
	case timestampFullName:
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string for timestamp field %s", field.Name())
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return fmt.Errorf("parsing timestamp %s: %w", field.Name(), err)
		}
		ts := timestamppb.New(t)
		msg.Set(field, protoreflect.ValueOfMessage(ts.ProtoReflect()))
		return nil

	case durationFullName:
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string for duration field %s", field.Name())
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("parsing duration %s: %w", field.Name(), err)
		}
		dp := durationpb.New(d)
		msg.Set(field, protoreflect.ValueOfMessage(dp.ProtoReflect()))
		return nil

	case fieldMaskFullName:
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string for field_mask field %s", field.Name())
		}
		fm := &fieldmaskpb.FieldMask{}
		if s != "" {
			fm.Paths = splitPaths(s)
		}
		msg.Set(field, protoreflect.ValueOfMessage(fm.ProtoReflect()))
		return nil

	default:
		nested, ok := val.(map[string]any)
		if !ok {
			return fmt.Errorf("expected object for %s", field.Name())
		}
		nestedMsg := msg.Mutable(field).Message().(*dynamicpb.Message)
		return populateMessage(nestedMsg, nested)
	}
}

func splitPaths(s string) []string {
	var paths []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			p := strings.TrimSpace(s[start:i])
			if p != "" {
				paths = append(paths, p)
			}
			start = i + 1
		}
	}
	return paths
}

func convertValue(field protoreflect.FieldDescriptor, val any) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		s, ok := val.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string for field %s, got %T", field.Name(), val)
		}
		return protoreflect.ValueOfString(s), nil
	case protoreflect.BoolKind:
		b, ok := val.(bool)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected bool for field %s, got %T", field.Name(), val)
		}
		return protoreflect.ValueOfBool(b), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		f, ok := val.(float64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected number for field %s, got %T", field.Name(), val)
		}
		return protoreflect.ValueOfInt32(int32(f)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		f, ok := val.(float64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected number for field %s, got %T", field.Name(), val)
		}
		return protoreflect.ValueOfInt64(int64(f)), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		f, ok := val.(float64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected number for field %s, got %T", field.Name(), val)
		}
		return protoreflect.ValueOfUint32(uint32(f)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		f, ok := val.(float64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected number for field %s, got %T", field.Name(), val)
		}
		return protoreflect.ValueOfUint64(uint64(f)), nil
	case protoreflect.FloatKind:
		f, ok := val.(float64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected number for field %s, got %T", field.Name(), val)
		}
		return protoreflect.ValueOfFloat32(float32(f)), nil
	case protoreflect.DoubleKind:
		f, ok := val.(float64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected number for field %s, got %T", field.Name(), val)
		}
		return protoreflect.ValueOfFloat64(f), nil
	case protoreflect.EnumKind:
		s, ok := val.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string for enum field %s, got %T", field.Name(), val)
		}
		enumVal := field.Enum().Values().ByName(protoreflect.Name(s))
		if enumVal == nil {
			return protoreflect.Value{}, fmt.Errorf("unknown enum value %q for field %s", s, field.Name())
		}
		return protoreflect.ValueOfEnum(enumVal.Number()), nil
	case protoreflect.MessageKind:
		nested, ok := val.(map[string]any)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected object for message field %s, got %T", field.Name(), val)
		}
		nestedMsg := dynamicpb.NewMessage(field.Message())
		if err := populateMessage(nestedMsg, nested); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(nestedMsg), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field kind %v for field %s", field.Kind(), field.Name())
	}
}

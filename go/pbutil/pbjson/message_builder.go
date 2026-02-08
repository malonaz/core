package pbjson

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

const responseReadMaskKey = "response_read_mask"

func (b *SchemaBuilder) BuildMessage(messageFullName protoreflect.FullName, args map[string]any) (*dynamicpb.Message, error) {
	desc, err := b.schema.FindDescriptorByName(messageFullName)
	if err != nil {
		return nil, fmt.Errorf("message not found: %s", messageFullName)
	}
	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor is not a message: %s", messageFullName)
	}
	return BuildMessage(msgDesc, args)
}

func BuildMessage(desc protoreflect.MessageDescriptor, args map[string]any) (*dynamicpb.Message, error) {
	msg := dynamicpb.NewMessage(desc)
	if err := populateMessage(msg, args); err != nil {
		return nil, err
	}
	return msg, nil
}

func GetResponseFieldMask(args map[string]any) (*fieldmaskpb.FieldMask, bool) {
	val, ok := args[responseReadMaskKey]
	if !ok {
		return nil, false
	}
	s, ok := val.(string)
	if !ok || s == "" {
		return nil, false
	}
	return pbfieldmask.FromString(s).Proto(), true
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
	if field.IsMap() {
		return setMapField(msg, field, val)
	}

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

	if field.Kind() == protoreflect.MessageKind {
		return setMessageField(msg, field, val)
	}

	v, err := convertValue(field, val)
	if err != nil {
		return err
	}
	msg.Set(field, v)
	return nil
}

func setMapField(msg *dynamicpb.Message, field protoreflect.FieldDescriptor, val any) error {
	m, ok := val.(map[string]any)
	if !ok {
		return fmt.Errorf("expected object for map field %s, got %T", field.Name(), val)
	}

	mapVal := msg.Mutable(field).Map()
	keyField := field.MapKey()
	valueField := field.MapValue()

	for k, v := range m {
		keyVal, err := convertMapKey(keyField, k)
		if err != nil {
			return fmt.Errorf("map field %s key: %w", field.Name(), err)
		}

		valueVal, err := convertValue(valueField, v)
		if err != nil {
			return fmt.Errorf("map field %s value: %w", field.Name(), err)
		}

		mapVal.Set(keyVal.MapKey(), valueVal)
	}
	return nil
}

func convertMapKey(field protoreflect.FieldDescriptor, key string) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(key), nil
	case protoreflect.BoolKind:
		switch key {
		case "true":
			return protoreflect.ValueOfBool(true), nil
		case "false":
			return protoreflect.ValueOfBool(false), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("invalid bool key: %s", key)
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		var i int32
		if _, err := fmt.Sscanf(key, "%d", &i); err != nil {
			return protoreflect.Value{}, fmt.Errorf("invalid int32 key: %s", key)
		}
		return protoreflect.ValueOfInt32(i), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		var i int64
		if _, err := fmt.Sscanf(key, "%d", &i); err != nil {
			return protoreflect.Value{}, fmt.Errorf("invalid int64 key: %s", key)
		}
		return protoreflect.ValueOfInt64(i), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		var i uint32
		if _, err := fmt.Sscanf(key, "%d", &i); err != nil {
			return protoreflect.Value{}, fmt.Errorf("invalid uint32 key: %s", key)
		}
		return protoreflect.ValueOfUint32(i), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		var i uint64
		if _, err := fmt.Sscanf(key, "%d", &i); err != nil {
			return protoreflect.Value{}, fmt.Errorf("invalid uint64 key: %s", key)
		}
		return protoreflect.ValueOfUint64(i), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported map key kind: %v", field.Kind())
	}
}

func setMessageField(msg *dynamicpb.Message, field protoreflect.FieldDescriptor, val any) error {
	v, err := convertMessageValue(field.Message(), val)
	if err != nil {
		return fmt.Errorf("field %s: %w", field.Name(), err)
	}
	msg.Set(field, v)
	return nil
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
		return convertMessageValue(field.Message(), val)
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field kind %v for field %s", field.Kind(), field.Name())
	}
}

func convertMessageValue(msgDesc protoreflect.MessageDescriptor, val any) (protoreflect.Value, error) {
	switch msgDesc.FullName() {
	case timestampFullName:
		s, ok := val.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string for timestamp, got %T", val)
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("parsing timestamp: %w", err)
		}
		return protoreflect.ValueOfMessage(timestamppb.New(t).ProtoReflect()), nil

	case durationFullName:
		s, ok := val.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string for duration, got %T", val)
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("parsing duration: %w", err)
		}
		return protoreflect.ValueOfMessage(durationpb.New(d).ProtoReflect()), nil

	case fieldMaskFullName:
		s, ok := val.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string for field_mask, got %T", val)
		}
		fm := &fieldmaskpb.FieldMask{}
		if s != "" {
			fm.Paths = splitPaths(s)
		}
		return protoreflect.ValueOfMessage(fm.ProtoReflect()), nil

	case dateFullName:
		s, ok := val.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string for date, got %T", val)
		}
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("parsing date: %w", err)
		}
		return protoreflect.ValueOfMessage((&date.Date{
			Year: int32(t.Year()), Month: int32(t.Month()), Day: int32(t.Day()),
		}).ProtoReflect()), nil

	case timeOfDayFullName:
		s, ok := val.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string for time_of_day, got %T", val)
		}
		t, err := time.Parse("15:04:05", s)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("parsing time_of_day: %w", err)
		}
		return protoreflect.ValueOfMessage((&timeofday.TimeOfDay{
			Hours: int32(t.Hour()), Minutes: int32(t.Minute()), Seconds: int32(t.Second()),
		}).ProtoReflect()), nil

	default:
		nested, ok := val.(map[string]any)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected object for message, got %T", val)
		}
		nestedMsg := dynamicpb.NewMessage(msgDesc)
		if err := populateMessage(nestedMsg, nested); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(nestedMsg), nil
	}
}

func splitPaths(s string) []string {
	var paths []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			paths = append(paths, p)
		}
	}
	return paths
}

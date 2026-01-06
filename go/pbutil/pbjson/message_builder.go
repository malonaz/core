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
)

func BuildMessage(desc protoreflect.MessageDescriptor, args map[string]any) (*dynamicpb.Message, error) {
	msg := dynamicpb.NewMessage(desc)
	if err := populateMessage(msg, args); err != nil {
		return nil, err
	}
	return msg, nil
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

	case dateFullName:
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string for date field %s", field.Name())
		}
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return fmt.Errorf("parsing date %s: %w", field.Name(), err)
		}
		d := &date.Date{Year: int32(t.Year()), Month: int32(t.Month()), Day: int32(t.Day())}
		msg.Set(field, protoreflect.ValueOfMessage(d.ProtoReflect()))
		return nil

	case timeOfDayFullName:
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string for time_of_day field %s", field.Name())
		}
		t, err := time.Parse("15:04:05", s)
		if err != nil {
			return fmt.Errorf("parsing time_of_day %s: %w", field.Name(), err)
		}
		tod := &timeofday.TimeOfDay{Hours: int32(t.Hour()), Minutes: int32(t.Minute()), Seconds: int32(t.Second())}
		msg.Set(field, protoreflect.ValueOfMessage(tod.ProtoReflect()))
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
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			paths = append(paths, p)
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

package protofield

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func IsTimestamp(field *protogen.Field) bool {
	return field.Message != nil && string(field.Message.Desc.FullName()) == "google.protobuf.Timestamp"
}

func IsDuration(field *protogen.Field) bool {
	return field.Message != nil && string(field.Message.Desc.FullName()) == "google.protobuf.Duration"
}

func GoType(field *protogen.Field) (string, error) {
	var kind string
	switch field.Desc.Kind() {
	case protoreflect.BoolKind:
		kind = "bool"
	case protoreflect.EnumKind:
		kind = "enum"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		kind = "int32"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		kind = "uint32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		kind = "int64"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		kind = "uint64"
	case protoreflect.FloatKind:
		kind = "float32"
	case protoreflect.DoubleKind:
		kind = "float64"
	case protoreflect.StringKind:
		kind = "string"
	case protoreflect.BytesKind:
		kind = "[]byte"
	}
	if kind == "" {
		return "", fmt.Errorf("unsupported field kind %v", field.Desc.FullName())
	}
	if field.Desc.IsList() {
		return "[]" + kind, nil
	}
	return kind, nil
}

func SanitizedGoType(field *protogen.Field, fqn func(string, string) string) (string, error) {
	if field.Enum != nil {
		return "int16", nil
	}
	if IsTimestamp(field) {
		return fqn("time", "Time"), nil
	}
	if IsDuration(field) {
		return fqn("time", "Duration"), nil
	}
	return GoType(field)
}

func ZeroValue(field *protogen.Field) (string, error) {
	if field.Desc.IsList() {
		return "nil", nil
	}
	switch field.Desc.Kind() {
	case protoreflect.BoolKind:
		return "false", nil
	case protoreflect.EnumKind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind,
		protoreflect.FloatKind, protoreflect.DoubleKind:
		return "0", nil
	case protoreflect.StringKind:
		return `""`, nil
	case protoreflect.BytesKind:
		return "nil", nil
	case protoreflect.MessageKind:
		if IsTimestamp(field) || IsDuration(field) {
			return "nil", nil
		}
	}
	return "", fmt.Errorf("unsupported field kind %v", field.Desc.FullName())
}

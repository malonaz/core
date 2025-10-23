package main

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// https://github.com/protocolbuffers/protobuf-go/blob/master/cmd/protoc-gen-go/internal_gengo/main.go
func fieldGoTypeInternal(g *protogen.GeneratedFile, field *protogen.Field) (string, bool) {
	if field.Desc.IsWeak() {
		return "struct{}", false
	}

	var goType string
	pointer := field.Desc.HasPresence()
	switch field.Desc.Kind() {
	case protoreflect.BoolKind:
		goType = "bool"
	case protoreflect.EnumKind:
		goType = g.QualifiedGoIdent(field.Enum.GoIdent)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		goType = "int32"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		goType = "uint32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		goType = "int64"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		goType = "uint64"
	case protoreflect.FloatKind:
		goType = "float32"
	case protoreflect.DoubleKind:
		goType = "float64"
	case protoreflect.StringKind:
		goType = "string"
	case protoreflect.BytesKind:
		goType = "[]byte"
		pointer = false // rely on nullability of slices for presence
	case protoreflect.MessageKind, protoreflect.GroupKind:
		goType = "*" + g.QualifiedGoIdent(field.Message.GoIdent)
		pointer = false // pointer captured as part of the type
	}
	switch {
	case field.Desc.IsList():
		return "[]" + goType, false
	case field.Desc.IsMap():
		keyType, _ := fieldGoTypeInternal(g, field.Message.Fields[0])
		valType, _ := fieldGoTypeInternal(g, field.Message.Fields[1])
		return fmt.Sprintf("map[%v]%v", keyType, valType), false
	}
	return goType, pointer
}

func fieldGoType(g *protogen.GeneratedFile, field *protogen.Field) string {
	s, _ := fieldGoTypeInternal(g, field)
	return s
}

func fieldGoTypeHasPointer(g *protogen.GeneratedFile, field *protogen.Field) bool {
	_, ok := fieldGoTypeInternal(g, field)
	return ok
}

func fieldType(field *protogen.Field) (string, error) {
	var kind = ""
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

func zeroValue(field *protogen.Field) (string, error) {
	if field.Desc.IsList() {
		return "nil", nil
	}

	var v = ""
	switch field.Desc.Kind() {
	case protoreflect.BoolKind:
		v = "false"
	case protoreflect.EnumKind:
		v = "0"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		v = "0"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		v = "0"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		v = "0"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		v = "0"
	case protoreflect.FloatKind:
		v = "0"
	case protoreflect.DoubleKind:
		v = "0"
	case protoreflect.StringKind:
		v = `""`
	case protoreflect.BytesKind:
		v = "nil"
	}
	if v == "" {
		return "", fmt.Errorf("unsupported field kind %v", field.Desc.FullName())
	}
	return v, nil
}

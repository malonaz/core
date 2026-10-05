// Package pbcsv flattens proto messages into CSV records: one column per leaf field.
package pbcsv

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
)

// Encoder turns messages of one type into CSV records.
//
// Columns follow declaration order. Nested messages are flattened into dotted columns
// (`metadata.location.room`); well-known types are one cell each; lists, maps and any other message
// (google.protobuf.*, google.type.*, recursive) are one JSON cell. Unset fields with presence, and
// empty lists and maps, are empty cells. Fields marked `(malonaz.codegen.aip.v1.export).exclude` are
// left out.
type Encoder struct {
	columns []column
}

// column is a leaf field, reached through path from the root message.
type column struct {
	name string
	path []protoreflect.FieldDescriptor
}

// NewEncoder returns an Encoder for messages of descriptor's type.
func NewEncoder(descriptor protoreflect.MessageDescriptor) *Encoder {
	encoder := &Encoder{}
	encoder.addColumns(descriptor, nil, map[protoreflect.FullName]bool{descriptor.FullName(): true})
	return encoder
}

// NewEncoderFor returns an Encoder for messages of type T.
func NewEncoderFor[T proto.Message]() *Encoder {
	var message T
	return NewEncoder(message.ProtoReflect().Descriptor())
}

func (e *Encoder) addColumns(descriptor protoreflect.MessageDescriptor, path []protoreflect.FieldDescriptor, seen map[protoreflect.FullName]bool) {
	fields := descriptor.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		if isExcluded(field) {
			continue
		}
		fieldPath := append(slices.Clone(path), field)
		if !isFlattened(field, seen) {
			names := make([]string, len(fieldPath))
			for j, pathField := range fieldPath {
				names[j] = string(pathField.Name())
			}
			e.columns = append(e.columns, column{name: strings.Join(names, "."), path: fieldPath})
			continue
		}
		name := field.Message().FullName()
		seen[name] = true
		e.addColumns(field.Message(), fieldPath, seen)
		delete(seen, name)
	}
}

func isExcluded(field protoreflect.FieldDescriptor) bool {
	options, ok := proto.GetExtension(field.Options(), aippb.E_Export).(*aippb.ExportFieldOptions)
	return ok && options.GetExclude()
}

// isFlattened reports whether field is a message spread over columns of its own.
func isFlattened(field protoreflect.FieldDescriptor, seen map[protoreflect.FullName]bool) bool {
	if field.IsList() || field.IsMap() || field.Message() == nil {
		return false
	}
	name := field.Message().FullName()
	switch name.Parent() {
	case "google.protobuf", "google.type":
		return false
	}
	return !seen[name]
}

// Header is the CSV header: one dotted field path per column.
func (e *Encoder) Header() []string {
	header := make([]string, len(e.columns))
	for i, column := range e.columns {
		header[i] = column.name
	}
	return header
}

// Record is message's CSV record, aligned with Header.
func (e *Encoder) Record(message proto.Message) ([]string, error) {
	record := make([]string, len(e.columns))
	for i, column := range e.columns {
		cell, err := column.cell(message.ProtoReflect())
		if err != nil {
			return nil, fmt.Errorf("encoding %s: %w", column.name, err)
		}
		record[i] = cell
	}
	return record, nil
}

func (c column) cell(message protoreflect.Message) (string, error) {
	for _, field := range c.path[:len(c.path)-1] {
		if !message.Has(field) {
			return "", nil
		}
		message = message.Get(field).Message()
	}
	field := c.path[len(c.path)-1]
	if (field.HasPresence() || field.IsList() || field.IsMap()) && !message.Has(field) {
		return "", nil
	}
	value := message.Get(field)
	switch {
	case field.IsList():
		list := value.List()
		elements := make([]any, list.Len())
		for i := range list.Len() {
			element, err := jsonValue(field, list.Get(i))
			if err != nil {
				return "", err
			}
			elements[i] = element
		}
		return marshalJSON(elements)
	case field.IsMap():
		entries := map[string]any{}
		var err error
		value.Map().Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
			entries[key.String()], err = jsonValue(field.MapValue(), value)
			return err == nil
		})
		if err != nil {
			return "", err
		}
		return marshalJSON(entries)
	case field.Message() != nil:
		return messageCell(value.Message())
	default:
		return scalarCell(field, value), nil
	}
}

// messageCell is a message that is not flattened, as one cell.
func messageCell(message protoreflect.Message) (string, error) {
	get := func(name protoreflect.Name) protoreflect.Value {
		return message.Get(message.Descriptor().Fields().ByName(name))
	}
	switch message.Descriptor().FullName() {
	case "google.protobuf.Timestamp":
		return time.Unix(get("seconds").Int(), get("nanos").Int()).UTC().Format(time.RFC3339Nano), nil
	case "google.protobuf.Duration":
		return (time.Duration(get("seconds").Int())*time.Second + time.Duration(get("nanos").Int())).String(), nil
	case "google.type.Decimal":
		return get("value").String(), nil
	case "google.type.Date":
		return fmt.Sprintf("%04d-%02d-%02d", get("year").Int(), get("month").Int(), get("day").Int()), nil
	case "google.protobuf.DoubleValue", "google.protobuf.FloatValue",
		"google.protobuf.Int64Value", "google.protobuf.UInt64Value",
		"google.protobuf.Int32Value", "google.protobuf.UInt32Value",
		"google.protobuf.BoolValue", "google.protobuf.StringValue", "google.protobuf.BytesValue":
		field := message.Descriptor().Fields().ByName("value")
		return scalarCell(field, message.Get(field)), nil
	}
	bytes, err := protojson.Marshal(message.Interface())
	if err != nil {
		return "", err
	}
	return marshalJSON(json.RawMessage(bytes))
}

func scalarCell(field protoreflect.FieldDescriptor, value protoreflect.Value) string {
	switch field.Kind() {
	case protoreflect.EnumKind:
		return enumName(field, value.Enum())
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(value.Bytes())
	default:
		return value.String()
	}
}

// jsonValue is a list element or map value, ready for encoding/json.
func jsonValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (any, error) {
	switch field.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		bytes, err := protojson.Marshal(value.Message().Interface())
		return json.RawMessage(bytes), err
	case protoreflect.EnumKind:
		return enumName(field, value.Enum()), nil
	default:
		return value.Interface(), nil
	}
}

func enumName(field protoreflect.FieldDescriptor, number protoreflect.EnumNumber) string {
	if value := field.Enum().Values().ByNumber(number); value != nil {
		return string(value.Name())
	}
	return strconv.Itoa(int(number))
}

// marshalJSON compacts too: protojson randomises its whitespace.
func marshalJSON(value any) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

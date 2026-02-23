package pbutil

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// ///////////////////////////////// MARSHALING ///////////////////////////////////
var marshalOptions = &proto.MarshalOptions{}

func Marshal(m proto.Message) ([]byte, error) {
	return marshalOptions.Marshal(m)
}

var marshalDeterministicOptions = &proto.MarshalOptions{
	Deterministic: true,
}

func MarshalDeterministic(m proto.Message) ([]byte, error) {
	return marshalDeterministicOptions.Marshal(m)
}

var unmarshalOptions = &proto.UnmarshalOptions{
	DiscardUnknown: true,
}

func Unmarshal(b []byte, m proto.Message) error {
	return unmarshalOptions.Unmarshal(b, m)
}

var JsonUnmarshalOptions = protojson.UnmarshalOptions{
	DiscardUnknown: true,
}

func JSONUnmarshal(b []byte, m proto.Message) error {
	return JsonUnmarshalOptions.Unmarshal(b, m)
}

var JsonUnmarshalStrictOptions = protojson.UnmarshalOptions{
	DiscardUnknown: false,
}

func JSONUnmarshalStrict(b []byte, m proto.Message) error {
	return JsonUnmarshalStrictOptions.Unmarshal(b, m)
}

var JsonMarshalOptions = protojson.MarshalOptions{
	UseProtoNames: true,
}

func JSONMarshal(m proto.Message) ([]byte, error) {
	return JsonMarshalOptions.Marshal(m)
}

var JsonCamelCaseMarshalOptions = protojson.MarshalOptions{
	UseProtoNames: false,
}

func JSONCamelCaseMarshal(m proto.Message) ([]byte, error) {
	return JsonCamelCaseMarshalOptions.Marshal(m)
}

var JsonMarshalPrettyOptions = protojson.MarshalOptions{
	UseProtoNames: true,
	Multiline:     true,
	Indent:        "  ",
}

func JSONMarshalPretty(m proto.Message) ([]byte, error) {
	return JsonMarshalPrettyOptions.Marshal(m)
}

////////////////// SLICES //////////////////

// JSONMarshalSlice marshals a slice of proto messages to JSON array bytes.
func JSONMarshalSlice[T proto.Message](options protojson.MarshalOptions, messages []T) ([]byte, error) {
	if len(messages) == 0 {
		return []byte("[]"), nil
	}

	var result []json.RawMessage
	for _, msg := range messages {
		b, err := options.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("marshaling message: %w", err)
		}
		result = append(result, json.RawMessage(b))
	}

	return json.Marshal(result)
}

// JSONUnmarshalSlice unmarshals JSON array bytes into a slice of proto messages.
func JSONUnmarshalSlice[T any, PT interface {
	*T
	proto.Message
}](options protojson.UnmarshalOptions, data []byte) ([]*T, error) {
	var rawMessages []json.RawMessage
	if err := json.Unmarshal(data, &rawMessages); err != nil {
		return nil, fmt.Errorf("parsing JSON array: %w", err)
	}

	result := make([]*T, 0, len(rawMessages))
	for i, rawMessage := range rawMessages {
		msg := PT(new(T))
		if err := options.Unmarshal(rawMessage, msg); err != nil {
			return nil, fmt.Errorf("unmarshaling message at index %d: %w", i, err)
		}
		result = append(result, (*T)(msg))
	}

	return result, nil
}

func MarshalToStruct(m proto.Message) (*structpb.Struct, error) {
	bytes, err := JSONMarshal(m)
	if err != nil {
		return nil, err
	}
	s := &structpb.Struct{}
	return s, s.UnmarshalJSON(bytes)
}

func UnmarshalFromStruct(m proto.Message, s *structpb.Struct) error {
	b, err := s.MarshalJSON()
	if err != nil {
		return err
	}
	return JSONUnmarshal(b, m)
}

func UnmarshalFromDynamic(m proto.Message, d *dynamicpb.Message) error {
	b, err := JSONMarshal(d)
	if err != nil {
		return err
	}
	return JSONUnmarshal(b, m)
}

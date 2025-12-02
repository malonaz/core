package pbutil

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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

var ProtoJsonUnmarshalOptions = protojson.UnmarshalOptions{
	DiscardUnknown: true,
}

func JSONUnmarshal(b []byte, m proto.Message) error {
	return ProtoJsonUnmarshalOptions.Unmarshal(b, m)
}

var ProtoJsonUnmarshalStrictOptions = protojson.UnmarshalOptions{
	DiscardUnknown: false,
}

func JSONUnmarshalStrict(b []byte, m proto.Message) error {
	return ProtoJsonUnmarshalStrictOptions.Unmarshal(b, m)
}

var ProtoJsonMarshalOptions = protojson.MarshalOptions{
	UseProtoNames: true,
}

func JSONMarshal(m proto.Message) ([]byte, error) {
	return ProtoJsonMarshalOptions.Marshal(m)
}

var ProtoJsonMarshalPrettyOptions = protojson.MarshalOptions{
	UseProtoNames: true,
	Multiline:     true,
	Indent:        "  ",
}

func JSONMarshalPretty(m proto.Message) ([]byte, error) {
	return ProtoJsonMarshalPrettyOptions.Marshal(m)
}

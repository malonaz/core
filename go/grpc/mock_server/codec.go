package mockserver

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// frame carries an already-serialized payload past the codec, so the unknown-service handler can
// decode requests itself rather than through generated stubs.
type frame struct {
	payload []byte
}

// codec passes frame payloads through untouched and falls back to proto for every other
// message, which is what keeps the registered health service working.
type codec struct{}

func (codec) Marshal(value any) ([]byte, error) {
	if frame, ok := value.(*frame); ok {
		return frame.payload, nil
	}
	message, ok := value.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("marshaling %T: not a proto message", value)
	}
	return proto.Marshal(message)
}

func (codec) Unmarshal(bytes []byte, value any) error {
	if frame, ok := value.(*frame); ok {
		frame.payload = bytes
		return nil
	}
	message, ok := value.(proto.Message)
	if !ok {
		return fmt.Errorf("unmarshaling into %T: not a proto message", value)
	}
	return proto.Unmarshal(bytes, message)
}

func (codec) Name() string { return "proto" }

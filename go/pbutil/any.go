package pbutil

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
)

func ExtractConcreteMessageFromAnyMessage(anyMessage *anypb.Any) (proto.Message, error) {
	// Get the message type
	mt, err := protoregistry.GlobalTypes.FindMessageByURL(anyMessage.TypeUrl)
	if err != nil {
		return nil, fmt.Errorf("unknown type %s: %v", anyMessage.TypeUrl, err)
	}
	// Create a new instance of the message
	message := mt.New().Interface()
	// Unmarshal the Any message
	if err := anyMessage.UnmarshalTo(message); err != nil {
		return nil, err
	}
	return message, nil
}

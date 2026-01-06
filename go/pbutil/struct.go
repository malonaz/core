package pbutil

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func UnmarshalFromStruct(m proto.Message, s *structpb.Struct) error {
	b, err := s.MarshalJSON()
	if err != nil {
		return err
	}
	return JSONUnmarshal(b, m)
}

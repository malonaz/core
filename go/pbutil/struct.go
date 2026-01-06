package pbutil

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func ParseFromStruct[T any, PT interface {
	*T
	proto.Message
}](s *structpb.Struct) (PT, error) {
	m := PT(new(T))
	b, err := s.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return m, JSONUnmarshal(b, m)
}

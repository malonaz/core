package grpc

import (
	"google.golang.org/grpc"
)

type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	return v.([]byte), nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	*(v.(*[]byte)) = data
	return nil
}

func (rawCodec) Name() string {
	return "proto"
}

func WithRawCodec() grpc.CallOption {
	return grpc.ForceCodec(rawCodec{})
}

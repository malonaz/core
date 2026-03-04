package middleware

import (
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"google.golang.org/grpc"
)

type CallMetadata struct {
	interceptors.CallMeta
}

func newServerCallMetadata(fullMethod string, streamInfo *grpc.StreamServerInfo, reqOrNil any) *CallMetadata {
	return &CallMetadata{CallMeta: interceptors.NewServerCallMeta(fullMethod, streamInfo, reqOrNil)}
}

func newClientCallMetadata(fullMethod string, streamDesc *grpc.StreamDesc, reqOrNil any) *CallMetadata {
	return &CallMetadata{CallMeta: interceptors.NewClientCallMeta(fullMethod, streamDesc, reqOrNil)}
}

func (m *CallMetadata) FullMethod() string {
	return m.Service + "." + m.Method

}

package pbreflection

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type MethodInvoker struct {
	conn *grpc.ClientConn
}

func NewMethodInvoker(conn *grpc.ClientConn) *MethodInvoker {
	return &MethodInvoker{conn: conn}
}

func (mi *MethodInvoker) Invoke(ctx context.Context, method protoreflect.MethodDescriptor, req proto.Message) (proto.Message, error) {
	svc := method.Parent().(protoreflect.ServiceDescriptor)
	fullMethod := fmt.Sprintf("/%s/%s", svc.FullName(), method.Name())
	resp := dynamicpb.NewMessage(method.Output())
	if err := mi.conn.Invoke(ctx, fullMethod, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

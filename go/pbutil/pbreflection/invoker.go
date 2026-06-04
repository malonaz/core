package pbreflection

import (
	"context"
	"errors"
	"fmt"
	"io"

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

func (mi *MethodInvoker) InvokeServerStream(
	ctx context.Context,
	method protoreflect.MethodDescriptor,
	request proto.Message,
	yield func(proto.Message) error,
) error {
	serviceDescriptor := method.Parent().(protoreflect.ServiceDescriptor)
	fullMethod := fmt.Sprintf("/%s/%s", serviceDescriptor.FullName(), method.Name())
	streamDescriptor := &grpc.StreamDesc{ServerStreams: true}
	clientStream, err := mi.conn.NewStream(ctx, streamDescriptor, fullMethod)
	if err != nil {
		return err
	}
	if err := clientStream.SendMsg(request); err != nil {
		return err
	}
	if err := clientStream.CloseSend(); err != nil {
		return err
	}
	for {
		response := dynamicpb.NewMessage(method.Output())
		if err := clientStream.RecvMsg(response); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := yield(response); err != nil {
			return err
		}
	}
}

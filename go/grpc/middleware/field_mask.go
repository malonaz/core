package middleware

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

const (
	metadataKeyFieldMask = "x-field-mask"
)

func WithFieldMask(ctx context.Context, paths string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metadataKeyFieldMask, paths)
}

func UnaryServerFieldMask() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Early return.
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return handler(ctx, req)
		}
		values := md.Get(metadataKeyFieldMask)
		if len(values) == 0 {
			return handler(ctx, req)
		}
		fieldMask := pbfieldmask.FromString(values[0])
		if fieldMask.IsWildcardPath() {
			return handler(ctx, req)
		}

		// Convert response to proto message.
		response, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}
		message, ok := response.(proto.Message)
		if !ok {
			return nil, status.Errorf(codes.Internal, "response is not a protobuf message: %T", response)
		}
		fieldMask.Apply(message)
		return response, nil
	}
}

func StreamServerFieldMask() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Extract field mask from metadata
		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return handler(srv, stream)
		}
		values := md.Get(metadataKeyFieldMask)
		if len(values) == 0 {
			return handler(srv, stream)
		}
		fieldMask := pbfieldmask.FromString(values[0])
		if fieldMask.IsWildcardPath() {
			return handler(srv, stream)
		}

		// Wrap the stream to intercept SendMsg
		wrappedStream := &fieldMaskServerStream{
			ServerStream: stream,
			fieldMask:    fieldMask,
		}
		return handler(srv, wrappedStream)
	}
}

type fieldMaskServerStream struct {
	grpc.ServerStream
	fieldMask *pbfieldmask.FieldMask
}

func (s *fieldMaskServerStream) SendMsg(m any) error {
	message, ok := m.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "message is not a protobuf message: %T", m)
	}
	s.fieldMask.Apply(message)
	return s.ServerStream.SendMsg(m)
}

package interceptor

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/pbutil"
)

const (
	metadataKeyFieldMask = "x-field-mask"
)

// GetFieldMask extracts the field mask from the incoming context metadata.
// Returns empty string if no field mask is set or if it's "*" (all fields).
func GetFieldMask(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(metadataKeyFieldMask)
	if len(values) == 0 || values[0] == "*" {
		return ""
	}
	return values[0]
}

func WithFieldMask(ctx context.Context, paths string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metadataKeyFieldMask, paths)
}

func UnaryServerFieldMask() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		fieldMaskPaths := GetFieldMask(ctx)
		if fieldMaskPaths == "" {
			return handler(ctx, req)
		}

		response, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}
		message, ok := response.(proto.Message)
		if !ok {
			return nil, status.Errorf(codes.Internal, "response is not a protobuf message: %T", response)
		}
		if err := pbutil.ApplyMask(message, fieldMaskPaths); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		return response, nil
	}
}

func StreamServerFieldMask() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		fieldMaskPaths := GetFieldMask(stream.Context())
		if fieldMaskPaths == "" {
			return handler(srv, stream)
		}

		wrappedStream := &fieldMaskServerStream{
			ServerStream:   stream,
			fieldMaskPaths: fieldMaskPaths,
		}
		return handler(srv, wrappedStream)
	}
}

type fieldMaskServerStream struct {
	grpc.ServerStream
	fieldMaskPaths string
}

func (s *fieldMaskServerStream) SendMsg(m any) error {
	message, ok := m.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "message is not a protobuf message: %T", m)
	}

	if err := pbutil.ApplyMask(message, s.fieldMaskPaths); err != nil {
		return status.Errorf(codes.InvalidArgument, err.Error())
	}

	return s.ServerStream.SendMsg(m)
}

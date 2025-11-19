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
		fieldMaskPaths := values[0]
		if fieldMaskPaths == "*" {
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
		if err := pbutil.ApplyMask(message, fieldMaskPaths); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		return response, nil
	}
}

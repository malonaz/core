package middleware

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"

	postgrespb "github.com/malonaz/core/genproto/postgres/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/postgres"
)

func UnaryServerReadReplica() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if shouldReadFromReplica(ctx) {
			ctx = postgres.WithReadReplica(ctx)
		}
		return handler(ctx, req)
	}
}

func StreamServerReadReplica() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !shouldReadFromReplica(stream.Context()) {
			return handler(srv, stream)
		}
		ctx := postgres.WithReadReplica(stream.Context())
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
}

func shouldReadFromReplica(ctx context.Context) bool {
	methodDescriptor, ok := MethodDescriptorFromContext(ctx)
	if !ok {
		return false
	}
	// A missing option means the primary.
	readReplica, err := pbutil.GetExtension[bool](methodDescriptor.Options(), postgrespb.E_ReadReplica)
	if err != nil {
		return false
	}
	return readReplica
}

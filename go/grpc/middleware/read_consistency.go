package middleware

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"

	postgrespb "github.com/malonaz/core/genproto/postgres/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/postgres"
)

func UnaryServerReadConsistency() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if isEventuallyConsistent(ctx) {
			ctx = postgres.WithEventualConsistency(ctx)
		}
		return handler(ctx, req)
	}
}

func StreamServerReadConsistency() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !isEventuallyConsistent(stream.Context()) {
			return handler(srv, stream)
		}
		ctx := postgres.WithEventualConsistency(stream.Context())
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
}

func isEventuallyConsistent(ctx context.Context) bool {
	methodDescriptor, ok := MethodDescriptorFromContext(ctx)
	if !ok {
		return false
	}
	// A missing option means strong consistency.
	readConsistency, err := pbutil.GetExtension[postgrespb.ReadConsistency](methodDescriptor.Options(), postgrespb.E_ReadConsistency)
	if err != nil {
		return false
	}
	return readConsistency == postgrespb.ReadConsistency_READ_CONSISTENCY_EVENTUAL
}

package interceptor

import (
	"context"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// Header key for the propagation header.
	propagateHeadersMetadataKey = "x-propagate-headers"
)

// Use this method if you want to add a header to be propagated down the chain of calls.
func AppendToOutgoingContextWithPropagation(ctx context.Context, key, value string) context.Context {
	return metadata.AppendToOutgoingContext(
		ctx,
		key, value,
		propagateHeadersMetadataKey, strings.ToLower(key), // Metadata library will canonicalize the key so we do too here.
	)
}

func propagateHeadersToOutgoingContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md) == 0 {
		return ctx
	}
	propagateKeys, ok := md[propagateHeadersMetadataKey]
	if !ok || len(propagateKeys) == 0 {
		return ctx
	}

	propagateKeySet := make(map[string]struct{}, len(propagateKeys))
	for _, key := range propagateKeys {
		propagateKeySet[key] = struct{}{}
	}

	// Collect headers to propagate
	var kvPairs []string
	for k, v := range md {
		if _, ok := propagateKeySet[k]; ok || k == propagateHeadersMetadataKey {
			for _, val := range v {
				kvPairs = append(kvPairs, k, val)
			}
		}
	}
	if len(kvPairs) == 0 {
		return ctx
	}

	return metadata.AppendToOutgoingContext(ctx, kvPairs...)
}

func UnaryServerHeaderPropagation() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = propagateHeadersToOutgoingContext(ctx)
		return handler(ctx, req)
	}
}

// StreamServerContextPropagation propagates incoming context to downstream calls.
func StreamServerHeaderPropagation() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := propagateHeadersToOutgoingContext(stream.Context())
		if ctx != stream.Context() {
			stream = &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx}
		}
		return handler(srv, stream)
	}
}

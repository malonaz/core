package grpc

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

// IsStandardHeader returns true if the given key is a standard gRPC/HTTP2 header:
//   - ":"           HTTP/2 pseudo-headers (:authority, :method, :path, :scheme).
//   - "grpc-"       gRPC reserved headers (grpc-timeout, grpc-encoding, grpc-status, etc.).
//   - "content-type" required by gRPC, always "application/grpc".
//   - "user-agent"   set automatically by gRPC clients.
//   - "te"           HTTP/2 transfer encoding, always "trailers".
//   - "authorization" standard HTTP authentication header.
func IsStandardHeader(key string) bool {
	return strings.HasPrefix(key, ":") || strings.HasPrefix(key, "grpc-") ||
		key == "content-type" || key == "user-agent" || key == "te" || key == "authorization"
}

func ForwardCustomHeaders(ctx context.Context) context.Context {
	incomingMD, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	outgoingMD := metadata.MD{}
	for key, values := range incomingMD {
		if !IsStandardHeader(key) {
			outgoingMD[key] = values
		}
	}
	if len(outgoingMD) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, outgoingMD)
}

package middleware

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

const (
	// defaultTimeout defines the default client timeout for RPCs.
	defaultTimeout = 10 * time.Second
)

func UnaryClientTimeout() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		// We set a default timeout only if there isn't one already.
		if _, ok := ctx.Deadline(); !ok {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
			defer cancel()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

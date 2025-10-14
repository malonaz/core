package interceptor

import (
	"context"
	"io"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/malonaz/core/go/contexttag"
)

// UnaryServerContextTagsInterceptor initializes context tags.
func UnaryServerContextTagsInitializer() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(contexttag.SetOntoContext(ctx), req)
	}
}

// StreamServerContextTagsInterceptor initializes context tags.
func StreamServerContextTagsInitializer() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := contexttag.SetOntoContext(stream.Context())
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
}

// UnaryServerContextPropagation propagates incoming context to downstream calls.
func UnaryServerContextPropagation() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			md := md.Copy()
			for k := range md {
				if strings.HasSuffix(k, "-no-prop") {
					delete(md, k)
				}
			}
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		return handler(ctx, req)
	}
}

// StreamServerContextPropagation propagates incoming context to downstream calls.
func StreamServerContextPropagation() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return handler(srv, stream)
		}
		ctx = metadata.NewOutgoingContext(ctx, md)
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
}

// UnaryServerTrailerPropagation propagates any trailers back to the client.
func UnaryServerTrailerPropagation() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, req)
		if tags, ok := contexttag.GetTrailersTags(ctx); ok && len(tags.Values()) > 0 {
			md := metadata.MD{}
			for key, values := range tags.Values() {
				if !strings.HasPrefix(key, "x-") {
					// We only propagate our own trailers.
					continue
				}
				md.Set(key, values...)
			}
			if len(md) > 0 {
				grpc.SetTrailer(ctx, md)
			}
		}
		return response, err
	}
}

// StreamServerTrailerPropagation propagates any trailers back to the client.
func StreamServerTrailerPropagation() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		wrappedStream := &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx}
		err := handler(srv, wrappedStream)
		// After the handler is finished, add any trailers from context to metadata
		if tags, ok := contexttag.GetTrailersTags(ctx); ok && len(tags.Values()) > 0 {
			md := metadata.MD{}
			for key, values := range tags.Values() {
				if !strings.HasPrefix(key, "x-") {
					// We only propagate our own trailers.
					continue
				}
				md.Set(key, values...)
			}
			if len(md) > 0 {
				wrappedStream.SetTrailer(md)
			}
		}
		return err
	}
}

func UnaryClientTrailerPropagation() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		tags, ok := contexttag.GetTrailersTags(ctx)
		if !ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		var trailer metadata.MD
		opts = append(opts, grpc.Trailer(&trailer))
		err := invoker(ctx, method, req, reply, cc, opts...)
		for key, values := range trailer {
			tags.Append(key, values...)
		}
		return err
	}
}

func StreamClientTrailerPropagation() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Delegate to the original streamer to create the ClientStream.
		originalClientStream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		// Wrap the originalClientStream to capture trailers when available.
		return &wrappedStream{ClientStream: originalClientStream, ctx: ctx}, nil
	}
}

// wrappedStream is a wrapper around the grpc.ClientStream that captures and
// logs the trailer metadata once we're done consuming the stream messages.
type wrappedStream struct {
	grpc.ClientStream
	ctx    context.Context
	method string
}

// RecvMsg wraps the original RecvMsg call and captures trailers on EOF.
func (w *wrappedStream) RecvMsg(m interface{}) error {
	err := w.ClientStream.RecvMsg(m)
	if err == io.EOF {
		w.handleTrailers()
	}
	return err
}

// CloseSend wraps the original CloseSend call and captures trailers.
func (w *wrappedStream) CloseSend() error {
	err := w.ClientStream.CloseSend()
	if err == nil {
		w.handleTrailers()
	}
	return err
}

// handleTrailers is called when we know trailers are available (after RecvMsg returns EOF or
// CloseSend is called). It retrieves the trailers and sets them in the context tags for propagation.
func (w *wrappedStream) handleTrailers() {
	if trailers := w.ClientStream.Trailer(); len(trailers) > 0 {
		if tags, ok := contexttag.GetTrailersTags(w.ctx); ok {
			// Assume that an existing implementation of contexttag.Set is being used.
			// Note that you have to implement the logic for setting the trailer metadata.
			for key, values := range trailers {
				tags.Append(key, values...)
			}
		}
	}
}

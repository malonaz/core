package interceptor

import (
	"context"
	"io"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// Trailer key for the propagation trailer.
	propagateTrailersMetadataKey = "x-propagate-trailers"
)

var (
	trailersTagKey = trailersTagKeyType("trailers-tag-key")
)

type trailersTagKeyType string

type trailersTag struct {
	md metadata.MD
}

func setTrailersTagOntoContext(ctx context.Context) (context.Context, *trailersTag) {
	tag := &trailersTag{md: metadata.MD{}}
	return context.WithValue(ctx, trailersTagKey, tag), tag
}

// Use this method if you want to add a trailer to be propagated up the chain of calls.
func AppendTrailerWithPropagation(ctx context.Context, key string, values ...string) bool {
	tag, ok := ctx.Value(trailersTagKey).(*trailersTag)
	if !ok {
		return false
	}
	// Canonicalize the key (metadata library will lowercase it)
	canonicalKey := strings.ToLower(key)
	tag.md.Append(key, values...)
	tag.md.Append(propagateTrailersMetadataKey, canonicalKey)
	return true
}

// Inject a trailer into the context. Returns false if the context tag is not found.
func AppendTrailer(ctx context.Context, key string, values ...string) bool {
	tag, ok := ctx.Value(trailersTagKey).(*trailersTag)
	if !ok {
		return false
	}
	tag.md.Append(key, values...)
	return true
}

// propagateTrailersToTag extracts propagatable trailers from the given metadata
// and appends them to the trailer tag.
func propagateTrailersToTag(trailer metadata.MD, tag *trailersTag) {
	propagateKeys, ok := trailer[propagateTrailersMetadataKey]
	if !ok || len(propagateKeys) == 0 {
		return
	}

	propagateKeySet := make(map[string]struct{}, len(propagateKeys))
	for _, key := range propagateKeys {
		propagateKeySet[key] = struct{}{}
	}

	// Propagate the specified trailers
	for key, values := range trailer {
		if _, ok := propagateKeySet[key]; ok || key == propagateTrailersMetadataKey {
			tag.md.Append(key, values...)
		}
	}
}

// UnaryServerTrailerPropagation sets a trailers tag onto the context and sends any trailers back to the client.
func UnaryServerTrailerPropagation() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, tag := setTrailersTagOntoContext(ctx)
		response, err := handler(ctx, req)
		if len(tag.md) > 0 {
			if err := grpc.SetTrailer(ctx, tag.md); err != nil {
				return nil, err
			}
		}
		return response, err
	}
}

// StreamServerTrailer sets a trailers tag onto the context and sends any trailers back to the client.
func StreamServerTrailerPropagation() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, tag := setTrailersTagOntoContext(stream.Context())
		wrappedStream := &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx}
		err := handler(srv, wrappedStream)
		if len(tag.md) > 0 {
			wrappedStream.SetTrailer(tag.md)
		}
		return err
	}
}

// UnaryClientTrailerPropagation automatically captures trailers and sets them onto to 'context' tag if they're propagatable.
// This is only done on 'ctx' that have a trailer tag set. GRPC Servers that use the Trailer Interceptor will have that set.
func UnaryClientTrailerPropagation() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		tag, ok := ctx.Value(trailersTagKey).(*trailersTag)
		if !ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		// Capture trailers.
		var trailer metadata.MD
		opts = append(opts, grpc.Trailer(&trailer))
		err := invoker(ctx, method, req, reply, cc, opts...)

		// Propagate trailers marked for propagation
		propagateTrailersToTag(trailer, tag)
		return err
	}
}

// StreamClientTrailerPropagation automatically captures trailers and sets them onto to 'context' tag if they're propagatable.
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
	tag, ok := w.ctx.Value(trailersTagKey).(*trailersTag)
	if !ok {
		return
	}
	trailer := w.ClientStream.Trailer()

	// Propagate trailers marked for propagation
	propagateTrailersToTag(trailer, tag)
}

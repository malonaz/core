package middleware

import (
	"context"
	"strconv"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	propagateHeadersMetadataKey = "x-propagate-headers"
)

type propagateOptions struct {
	hops int
}

type PropagateOption func(*propagateOptions)

func WithHops(n int) PropagateOption {
	return func(o *propagateOptions) {
		o.hops = n
	}
}

func AppendToOutgoingContextWithPropagation(ctx context.Context, key, value string, opts ...PropagateOption) context.Context {
	o := &propagateOptions{}
	for _, opt := range opts {
		opt(o)
	}

	canonicalKey := strings.ToLower(key)

	if o.hops == 1 {
		return metadata.AppendToOutgoingContext(ctx, key, value)
	}

	propagateValue := canonicalKey
	if o.hops > 1 {
		propagateValue = canonicalKey + ":" + strconv.Itoa(o.hops-1)
	}

	return metadata.AppendToOutgoingContext(
		ctx,
		key, value,
		propagateHeadersMetadataKey, propagateValue,
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

	type keyHops struct {
		key     string
		hops    int
		limited bool
	}
	propagateKeySet := make(map[string]keyHops, len(propagateKeys))
	for _, entry := range propagateKeys {
		key := entry
		hops := 0
		limited := false
		if idx := strings.LastIndex(entry, ":"); idx != -1 {
			if h, err := strconv.Atoi(entry[idx+1:]); err == nil {
				key, hops, limited = entry[:idx], h, true
			}
		}
		propagateKeySet[key] = keyHops{key: key, hops: hops, limited: limited}
	}

	var kvPairs []string
	for k, v := range md {
		kh, ok := propagateKeySet[k]
		if !ok {
			continue
		}
		for _, val := range v {
			kvPairs = append(kvPairs, k, val)
		}
		if !kh.limited {
			kvPairs = append(kvPairs, propagateHeadersMetadataKey, kh.key)
		} else if kh.hops > 1 {
			kvPairs = append(kvPairs, propagateHeadersMetadataKey, kh.key+":"+strconv.Itoa(kh.hops-1))
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

func StreamServerHeaderPropagation() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := propagateHeadersToOutgoingContext(stream.Context())
		if ctx != stream.Context() {
			stream = &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx}
		}
		return handler(srv, stream)
	}
}

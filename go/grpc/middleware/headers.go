package middleware

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	gatewaypb "github.com/malonaz/core/genproto/codegen/gateway/v1"
	"github.com/malonaz/core/go/pbutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoreflect"
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

func ForwardCustomHeaders(ctx context.Context) context.Context {
	incomingMD, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	var pairs []string
	for key, values := range incomingMD {
		if IsStandardHeader(key) {
			continue
		}
		for _, value := range values {
			pairs = append(pairs, key, value)
		}
	}
	if len(pairs) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

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

func buildProxyMethodSet(serviceDescriptors []protoreflect.ServiceDescriptor) map[string]struct{} {
	proxyMethodSet := map[string]struct{}{}
	for _, serviceDesc := range serviceDescriptors {
		methods := serviceDesc.Methods()
		for i := 0; i < methods.Len(); i++ {
			method := methods.Get(i)
			handlerOpts, err := pbutil.GetExtension[*gatewaypb.HandlerOpts](method.Options(), gatewaypb.E_Opts)
			if err != nil {
				if errors.Is(err, pbutil.ErrExtensionNotFound) {
					continue
				}
				panic(fmt.Errorf("getting handler opts for %q: %w", method.FullName(), err))
			}
			if handlerOpts.GetProxy() != "" {
				fullMethod := "/" + string(serviceDesc.Name()) + "/" + string(method.Name())
				panic(fullMethod)
				proxyMethodSet[fullMethod] = struct{}{}
			}
		}
	}
	return proxyMethodSet
}

func UnaryServerHeaderPropagation(serviceDescriptors []protoreflect.ServiceDescriptor) grpc.UnaryServerInterceptor {
	proxyMethodSet := buildProxyMethodSet(serviceDescriptors)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = propagateHeadersToOutgoingContext(ctx)
		if _, ok := proxyMethodSet[info.FullMethod]; ok {
			ctx = ForwardCustomHeaders(ctx)
		}
		return handler(ctx, req)
	}
}

func StreamServerHeaderPropagation(serviceDescriptors []protoreflect.ServiceDescriptor) grpc.StreamServerInterceptor {
	proxyMethodSet := buildProxyMethodSet(serviceDescriptors)
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := propagateHeadersToOutgoingContext(stream.Context())
		if _, ok := proxyMethodSet[info.FullMethod]; ok {
			ctx = ForwardCustomHeaders(ctx)
		}
		if ctx != stream.Context() {
			stream = &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx}
		}
		return handler(srv, stream)
	}
}

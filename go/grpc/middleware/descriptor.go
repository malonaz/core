package middleware

import (
	"context"
	"fmt"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type methodDescriptorKeyType string

const methodDescriptorKey = methodDescriptorKeyType("method-descriptor")

func MethodDescriptorFromContext(ctx context.Context) (protoreflect.MethodDescriptor, bool) {
	methodDescriptor, ok := ctx.Value(methodDescriptorKey).(protoreflect.MethodDescriptor)
	return methodDescriptor, ok
}

func resolveMethodDescriptor(fullMethod string) (protoreflect.MethodDescriptor, error) {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	lastSlash := strings.LastIndex(trimmed, "/")
	if lastSlash < 0 {
		return nil, fmt.Errorf("invalid full method format %q: missing slash separator", fullMethod)
	}
	serviceName := trimmed[:lastSlash]
	methodName := trimmed[lastSlash+1:]

	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return nil, fmt.Errorf("finding service descriptor for %q: %w", serviceName, err)
	}
	serviceDescriptor, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor %q is %T, not a service descriptor", serviceName, descriptor)
	}
	methodDescriptor := serviceDescriptor.Methods().ByName(protoreflect.Name(methodName))
	if methodDescriptor == nil {
		return nil, fmt.Errorf("method %q not found in service %q", methodName, serviceName)
	}
	return methodDescriptor, nil
}

func UnaryServerMethodDescriptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		methodDescriptor, err := resolveMethodDescriptor(info.FullMethod)
		if err != nil {
			return nil, fmt.Errorf("resolving method descriptor: %w", err)
		}
		ctx = context.WithValue(ctx, methodDescriptorKey, methodDescriptor)
		return handler(ctx, req)
	}
}

func StreamServerMethodDescriptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		methodDescriptor, err := resolveMethodDescriptor(info.FullMethod)
		if err != nil {
			return fmt.Errorf("resolving method descriptor: %w", err)
		}
		ctx := context.WithValue(stream.Context(), methodDescriptorKey, methodDescriptor)
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
}

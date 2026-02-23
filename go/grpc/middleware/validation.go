package middleware

import (
	"context"

	"buf.build/go/protovalidate"
	grpc_protovalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// unaryClientValidateInterceptor returns a new unary client interceptor that validates outgoing messages.
// Invalid messages will be rejected with `InvalidArgument` before sending the request to server.
func UnaryClientValidate(validator protovalidate.Validator) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if m, ok := req.(proto.Message); ok {
			if err := validator.Validate(m); err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func UnaryServerValidate(validator protovalidate.Validator) grpc.UnaryServerInterceptor {
	return grpc_protovalidate.UnaryServerInterceptor(validator)
}

func StreamServerValidate(validator protovalidate.Validator) grpc.StreamServerInterceptor {
	return grpc_protovalidate.StreamServerInterceptor(validator)
}

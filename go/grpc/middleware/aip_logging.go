package middleware

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/logging"
)

func UnaryServerAIPLogging() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if message, ok := req.(proto.Message); ok {
			logging.InjectAIPLogFields(ctx, "grpc.request.", message)
		}
		return handler(ctx, req)
	}
}

func StreamServerAIPLogging() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &aipLoggingServerStream{ServerStream: stream})
	}
}

type aipLoggingServerStream struct {
	grpc.ServerStream
	firstRecv bool
}

func (s *aipLoggingServerStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if !s.firstRecv {
		s.firstRecv = true
		if message, ok := m.(proto.Message); ok {
			logging.InjectAIPLogFields(s.Context(), "grpc.request.", message)
		}
	}
	return nil
}

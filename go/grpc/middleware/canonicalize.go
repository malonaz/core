package middleware

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	canonicalizepb "github.com/malonaz/core/genproto/canonicalize/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbcanonicalize"
)

func UnaryServerCanonicalize() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if shouldSkipCanonicalization(ctx) {
			return handler(ctx, req)
		}
		if message, ok := req.(proto.Message); ok {
			if err := pbcanonicalize.Message(message); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

func StreamServerCanonicalize() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if shouldSkipCanonicalization(stream.Context()) {
			return handler(srv, stream)
		}
		return handler(srv, &canonicalizeServerStream{ServerStream: stream})
	}
}

type canonicalizeServerStream struct {
	grpc.ServerStream
}

func (s *canonicalizeServerStream) SendMsg(m any) error {
	if message, ok := m.(proto.Message); ok {
		if err := pbcanonicalize.Message(message); err != nil {
			return err
		}
	}
	return s.ServerStream.SendMsg(m)
}

func (s *canonicalizeServerStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if message, ok := m.(proto.Message); ok {
		if err := pbcanonicalize.Message(message); err != nil {
			return err
		}
	}
	return nil
}

func shouldSkipCanonicalization(ctx context.Context) bool {
	methodDescriptor, ok := MethodDescriptorFromContext(ctx)
	if !ok {
		return false
	}
	skip, err := pbutil.GetExtension[bool](methodDescriptor.Options(), canonicalizepb.E_Skip)
	if err != nil {
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			return false
		}
		// Log but don't fail the RPC for a missing option.
		return false
	}
	return skip
}

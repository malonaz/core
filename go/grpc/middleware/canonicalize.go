package middleware

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	canonicalizepb "github.com/malonaz/core/genproto/canonicalize/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbcanonicalize"
)

func UnaryServerCanonicalize() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		skip, err := shouldSkipCanonicalization(info.FullMethod)
		if err != nil {
			return nil, err
		}
		if skip {
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
		skip, err := shouldSkipCanonicalization(info.FullMethod)
		if err != nil {
			return err
		}
		if skip {
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

func shouldSkipCanonicalization(fullMethod string) (bool, error) {
	methodName := strings.Replace(strings.TrimPrefix(fullMethod, "/"), "/", ".", 1)
	skip, err := pbutil.GetMethodOption[bool](methodName, canonicalizepb.E_Skip)
	if err != nil {
		if err == pbutil.ErrExtensionNotFound {
			return false, nil
		}
		return false, fmt.Errorf("get method option: %w", err)
	}
	return skip, nil
}

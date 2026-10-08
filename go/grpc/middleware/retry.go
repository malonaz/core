package middleware

import (
	"context"
	"slices"
	"sync/atomic"
	"time"

	grpc_interceptors "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	grpc_selector "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// maxRetries is the number of type we retry a retryable client
	maxRetries = 5
	// retryBackoff is the default timeout we apply between retries for a retryable client.
	retryBackoff = 100 * time.Millisecond
)

// DefaultRetriableCodes is a set of well known types gRPC codes that should be retri-able.
// `Unavailable` means that system is currently unavailable and the client should retry again.
var (
	retriableCodes = []codes.Code{
		codes.Unavailable,
	}

	allButClientStream = grpc_selector.MatchFunc(func(ctx context.Context, callMeta grpc_interceptors.CallMeta) bool {
		return callMeta.Typ != grpc_interceptors.ClientStream && callMeta.Typ != grpc_interceptors.BidiStream
	})
)

// UnaryClientRetry returns a gRPC DialOption that adds a default retrying interceptor to all unary RPC calls.
// Only retries on ResourceExhausted and Unavailable errors.
func UnaryClientRetry() grpc.UnaryClientInterceptor {
	return grpc_retry.UnaryClientInterceptor(
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(retryBackoff)),
		grpc_retry.WithMax(maxRetries),
		grpc_retry.WithCodes(retriableCodes...),
	)
}

// StreamClientRetry returns a grpc retry interceptor. A server stream is only
// retried until its first message: past that, a retry would replay the call
// from the start, re-running the server's work and re-delivering messages the
// caller already consumed.
func StreamClientRetry() grpc.StreamClientInterceptor {
	interceptor := grpc_retry.StreamClientInterceptor(
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(retryBackoff)),
		grpc_retry.WithMax(maxRetries),
	)
	retryBeforeFirstMessage := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		received := &atomic.Bool{}
		trackingStreamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			stream, err := streamer(ctx, desc, cc, method, opts...)
			if err != nil {
				return nil, err
			}
			return &receiveTrackingClientStream{ClientStream: stream, received: received}, nil
		}
		retriable := grpc_retry.WithRetriable(func(err error) bool {
			return !received.Load() && slices.Contains(retriableCodes, status.Code(err))
		})
		return interceptor(ctx, desc, cc, method, trackingStreamer, append(opts, retriable)...)
	}
	return grpc_selector.StreamClientInterceptor(retryBeforeFirstMessage, allButClientStream)
}

// receiveTrackingClientStream records whether any attempt of a call has
// delivered a message.
type receiveTrackingClientStream struct {
	grpc.ClientStream
	received *atomic.Bool
}

func (s *receiveTrackingClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err == nil {
		s.received.Store(true)
	}
	return err
}

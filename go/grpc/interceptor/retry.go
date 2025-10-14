package interceptor

import (
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	// maxRetries is the number of type we retry a retryable client
	maxRetries = 5
	// retryBackoff is the default timeout we apply between retries for a retryable client.
	retryBackoff = 100 * time.Millisecond
)

// DefaultRetriableCodes is a set of well known types gRPC codes that should be retri-able.
// `Unavailable` means that system is currently unavailable and the client should retry again.
var retriableCodes = []codes.Code{
	codes.Unavailable,
}

// UnaryClientRetry returns a gRPC DialOption that adds a default retrying interceptor to all unary RPC calls.
// Only retries on ResourceExhausted and Unavailable errors.
func UnaryClientRetry() grpc.UnaryClientInterceptor {
	return grpc_retry.UnaryClientInterceptor(
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(retryBackoff)),
		grpc_retry.WithMax(maxRetries),
		grpc_retry.WithCodes(retriableCodes...),
	)
}

// StreamClientRetry returns a grpc retry interceptor.
func StreamClientRetry() grpc.StreamClientInterceptor {
	return grpc_retry.StreamClientInterceptor(
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(retryBackoff)),
		grpc_retry.WithMax(maxRetries),
		grpc_retry.WithCodes(retriableCodes...),
	)
}

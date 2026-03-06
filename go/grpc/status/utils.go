package status

import (
	"iter"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ErrorDetails returns an iterator over the gRPC error details of type M attached to err.
// If err does not carry a gRPC status, the iterator yields nothing.
func ErrorDetails[M proto.Message](err error) iter.Seq[M] {
	return func(yield func(M) bool) {
		st, ok := status.FromError(err)
		if !ok {
			return
		}
		for _, detail := range st.Details() {
			if m, ok := detail.(M); ok {
				if !yield(m) {
					return
				}
			}
		}
	}
}

// HasCode reports whether the gRPC status code of err matches any of the provided codes.
func HasCode(err error, codesToMatch ...codes.Code) bool {
	code := status.Code(err)
	for _, codeToMatch := range codesToMatch {
		if code == codeToMatch {
			return true
		}
	}
	return false
}

package status

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RangeErrorDetails extracts typed error details from a gRPC error.
// For each detail matching the type parameter M, fn is called. Iteration
// stops early if fn returns false or if the error does not carry a gRPC status.
func RangeErrorDetails[M proto.Message](err error, fn func(M) bool) {
	st, ok := status.FromError(err)
	if !ok {
		return
	}
	for _, detail := range st.Details() {
		if m, ok := detail.(M); ok {
			if !fn(m) {
				return
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

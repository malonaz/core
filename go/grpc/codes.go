package grpc

import (
	"net/http"

	"google.golang.org/grpc/codes"
)

// httpStatusToCodeMap provides efficient lookup for HTTP status to gRPC code conversion.
// This is the inverse of the mapping defined in:
// https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
var httpStatusToCodeMap = map[int]codes.Code{
	http.StatusOK:                  codes.OK,
	499:                            codes.Canceled, // Client closed request
	http.StatusBadRequest:          codes.InvalidArgument,
	http.StatusGatewayTimeout:      codes.DeadlineExceeded,
	http.StatusNotFound:            codes.NotFound,
	http.StatusConflict:            codes.AlreadyExists,
	http.StatusForbidden:           codes.PermissionDenied,
	http.StatusUnauthorized:        codes.Unauthenticated,
	http.StatusTooManyRequests:     codes.ResourceExhausted,
	http.StatusNotImplemented:      codes.Unimplemented,
	http.StatusInternalServerError: codes.Internal,
	http.StatusServiceUnavailable:  codes.Unavailable,
}

// CodeFromHTTPStatus converts an HTTP status code to the corresponding gRPC code.
// This follows the inverse mapping of HTTPStatusFromCode as defined in the gRPC spec.
// If the HTTP status code doesn't have a direct mapping, it returns codes.Unknown.
//
// See: https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
func CodeFromHTTPStatus(status int) codes.Code {
	if code, ok := httpStatusToCodeMap[status]; ok {
		return code
	}
	return codes.Unknown
}

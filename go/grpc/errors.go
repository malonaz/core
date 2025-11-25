package grpc

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

type Error struct {
	code    codes.Code
	message string
	details []protoadapt.MessageV1
}

func Errorf(code codes.Code, message string, params ...any) *Error {
	return &Error{
		code:    code,
		message: fmt.Sprintf(message, params...),
	}
}

func (e *Error) WithLocalizedMessage(message string, params ...any) *Error {
	localizedMessage := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: fmt.Sprintf(message, params...),
	}
	e.details = append(e.details, localizedMessage)
	return e
}

func (e *Error) WithErrorInfo(reason, domain string, metadata map[string]string) *Error {
	errorInfo := &errdetails.ErrorInfo{
		Reason:   reason,
		Domain:   domain,
		Metadata: metadata,
	}
	e.details = append(e.details, errorInfo)
	return e
}

func (e *Error) Err() error {
	st := status.New(e.code, e.message)

	if len(e.details) > 0 {
		var err error
		st, err = st.WithDetails(e.details...)
		if err != nil {
			// Fallback to status without details if adding details fails
			return status.New(e.code, fmt.Sprintf("%s (failed to add details: %v)", e.message, err)).Err()
		}
	}

	return st.Err()
}

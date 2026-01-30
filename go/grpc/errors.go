package grpc

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

var maxStackDepth = 5

// A client of this library may chose to set a higher or lower stack depth.
// If depth == 0 => we do not set a debug info.
func SetErrorMaxStackDepth(depth int) {
	maxStackDepth = depth
}

type Error struct {
	code    codes.Code
	message string
	details []protoadapt.MessageV1
}

// MapContextError converts context errors to gRPC errors with debug info.
// If err is not a context error, it panics.
// For context.Canceled, it returns a Canceled error.
// For context.DeadlineExceeded, it returns a DeadlineExceeded error.
func FromContextError(err error) error {
	var grpcErr *Error
	switch err {
	case context.Canceled:
		grpcErr = Errorf(codes.Canceled, "request canceled")
	case context.DeadlineExceeded:
		grpcErr = Errorf(codes.DeadlineExceeded, "deadline exceeded")
	default:
		panic(fmt.Sprintf("unexpected error: %v", err))
	}
	return grpcErr.Err()
}

func Errorf(code codes.Code, message string, params ...any) *Error {
	e := &Error{
		code:    code,
		message: fmt.Sprintf(message, params...),
	}

	// Only capture stack trace if maxStackDepth > 0
	if maxStackDepth > 0 {
		stackEntries := make([]string, 0, maxStackDepth)

		// Capture stack trace.
		for i := 0; i < maxStackDepth; i++ {
			pc, file, line, ok := runtime.Caller(i + 1) // Skip 1 to exclude Errorf itself.
			if !ok {
				break
			}
			fn := runtime.FuncForPC(pc)
			funcName := "unknown"
			if fn != nil {
				funcName = fn.Name()
			}
			stackEntries = append(stackEntries, fmt.Sprintf("%s %s:%d", funcName, filepath.Base(file), line))
		}

		debugInfo := &errdetails.DebugInfo{
			StackEntries: stackEntries,
		}
		e.details = append(e.details, debugInfo)
	}

	return e
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

func (e *Error) Status() *status.Status {
	st := status.New(e.code, e.message)

	if len(e.details) > 0 {
		var err error
		st, err = st.WithDetails(e.details...)
		if err != nil {
			return status.New(e.code, fmt.Sprintf("%s (failed to add details: %v)", e.message, err))
		}
	}

	return st
}

func (e *Error) Proto() *spb.Status {
	return e.Status().Proto()
}

func (e *Error) Err() error {
	return e.Status().Err()
}

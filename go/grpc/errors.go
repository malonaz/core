package grpc

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var maxStackDepth = 5

// A client of this library may chose to set a higher or lower stack depth.
// If depth == 0 => we do not set a debug info.
func SetErrorMaxStackDepth(depth int) {
	maxStackDepth = depth
}

type Error struct {
	status *spb.Status
}

// Construct an error from an existing error.
func FromError(err error) *Error {
	// Check if the error is a context error.
	if errors.Is(err, context.Canceled) {
		return &Error{
			status: &spb.Status{Code: int32(codes.Canceled), Message: err.Error()},
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{
			status: &spb.Status{Code: int32(codes.DeadlineExceeded), Message: err.Error()},
		}
	}

	st, ok := status.FromError(err)
	if !ok {
		return &Error{
			status: &spb.Status{Code: int32(codes.Unknown), Message: err.Error()},
		}
	}

	return &Error{
		status: st.Proto(),
	}
}

func Errorf(code codes.Code, message string, params ...any) *Error {
	e := &Error{
		status: &spb.Status{Code: int32(code), Message: fmt.Sprintf(message, params...)},
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
		any, err := anypb.New(debugInfo)
		if err != nil {
			panic(err)
		}
		e.status.Details = append(e.status.Details, any)
	}

	return e
}

func (e *Error) WithLocalizedMessage(message string, params ...any) *Error {
	localizedMessage := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: fmt.Sprintf(message, params...),
	}
	any, err := anypb.New(localizedMessage)
	if err != nil {
		panic(err)
	}
	e.status.Details = append(e.status.Details, any)
	return e
}

func (e *Error) WithErrorInfo(reason, domain string, metadata map[string]string) *Error {
	errorInfo := &errdetails.ErrorInfo{
		Reason:   reason,
		Domain:   domain,
		Metadata: metadata,
	}
	any, err := anypb.New(errorInfo)
	if err != nil {
		panic(err)
	}
	e.status.Details = append(e.status.Details, any)
	return e
}

func (e *Error) WithDetails(messages ...proto.Message) *Error {
	for _, m := range messages {
		if a, ok := m.(*anypb.Any); ok {
			e.status.Details = append(e.status.Details, a)
			continue
		}
		a, err := anypb.New(m)
		if err != nil {
			panic(err)
		}
		e.status.Details = append(e.status.Details, a)
	}
	return e
}

func (e *Error) Status() *status.Status {
	return status.FromProto(e.status)
}

func (e *Error) Proto() *spb.Status {
	return proto.Clone(e.status).(*spb.Status)
}

func (e *Error) Err() error {
	return e.Status().Err()
}

// Range over error details in an error.
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

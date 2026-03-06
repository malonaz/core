package status

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

// maxStackDepth controls how many stack frames are captured in DebugInfo
// when creating errors via Errorf. A value of 0 disables stack capture entirely.
var maxStackDepth = 5

// SetErrorMaxStackDepth configures the number of stack frames captured in the
// DebugInfo detail attached to errors created by Errorf.
// Setting depth to 0 disables stack trace capture.
func SetErrorMaxStackDepth(depth int) {
	maxStackDepth = depth
}

// Error wraps a google.rpc.Status proto, providing a builder pattern for
// attaching structured error details (debug info, localized messages, error info)
// before converting to a gRPC-compatible error.
type Error struct {
	status *spb.Status
}

// FromError converts a standard Go error into an *Error.
//
// It handles three cases:
//   - context.Canceled and context.DeadlineExceeded are mapped to their
//     corresponding gRPC codes (Canceled, DeadlineExceeded).
//   - Errors already carrying a gRPC status are unwrapped and preserved.
//   - All other errors are wrapped with codes.Unknown.
func FromError(err error) *Error {
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

// Errorf creates a new *Error with the given gRPC status code and a formatted message.
// If maxStackDepth > 0, a DebugInfo detail containing the caller's stack trace
// (up to maxStackDepth frames, excluding Errorf itself) is automatically attached.
func Errorf(code codes.Code, message string, params ...any) *Error {
	e := &Error{
		status: &spb.Status{Code: int32(code), Message: fmt.Sprintf(message, params...)},
	}

	if maxStackDepth > 0 {
		stackEntries := make([]string, 0, maxStackDepth)

		for i := 0; i < maxStackDepth; i++ {
			pc, file, line, ok := runtime.Caller(i + 1)
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

// WithLocalizedMessage appends a LocalizedMessage detail (locale "en-US") to the error.
// The message is formatted using fmt.Sprintf. Returns the receiver for chaining.
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

// WithErrorInfo appends an ErrorInfo detail containing a machine-readable reason,
// domain, and optional metadata map. Returns the receiver for chaining.
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

// WithDetails appends arbitrary proto messages as error details.
// Messages that are already *anypb.Any are appended directly; all others
// are marshaled into Any first. Panics if marshaling fails.
// Returns the receiver for chaining.
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

// Status converts the Error into a *grpc/status.Status, suitable for
// inspection or further manipulation via the standard gRPC status API.
func (e *Error) Status() *status.Status {
	return status.FromProto(e.status)
}

// Proto returns a deep copy of the underlying google.rpc.Status proto.
func (e *Error) Proto() *spb.Status {
	return proto.CloneOf(e.status)
}

// Err converts the Error into a standard Go error carrying the gRPC status.
// This is typically the final call in an RPC handler's error return path.
func (e *Error) Err() error {
	return e.Status().Err()
}

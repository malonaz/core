// go/grpc/middleware/errors.go
package middleware

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/malonaz/core/go/pbutil"
)

// ===== Debug Info Scrubber Interceptor =====

type debugInfoScrubber struct{}

func UnaryServerDebugInfoScrubber() grpc.UnaryServerInterceptor {
	scrubber := &debugInfoScrubber{}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			err = scrubber.scrubDebugInfo(ctx, info.FullMethod, err)
		}
		return resp, err
	}
}

func StreamServerDebugInfoScrubber() grpc.StreamServerInterceptor {
	scrubber := &debugInfoScrubber{}

	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)
		if err != nil {
			err = scrubber.scrubDebugInfo(stream.Context(), info.FullMethod, err)
		}
		return err
	}
}

func (s *debugInfoScrubber) scrubDebugInfo(ctx context.Context, fullMethod string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	var hasDebugInfo bool
	var otherDetails []protoadapt.MessageV1

	for _, detail := range st.Details() {
		switch d := detail.(type) {
		case *errdetails.DebugInfo:
			hasDebugInfo = true
			if len(d.StackEntries) > 0 {
				slog.DebugContext(ctx, "error debug info",
					"method", fullMethod,
					"stack", d.StackEntries,
					"detail", d.Detail,
				)
			}
		default:
			if msg, ok := detail.(protoadapt.MessageV1); ok {
				otherDetails = append(otherDetails, msg)
			}
		}
	}

	if !hasDebugInfo {
		return err
	}

	newSt := status.New(st.Code(), st.Message())
	if len(otherDetails) > 0 {
		var newErr error
		newSt, newErr = newSt.WithDetails(otherDetails...)
		if newErr != nil {
			slog.ErrorContext(ctx, "attaching error details after scrubbing debug info",
				"error", newErr,
				"method", fullMethod,
			)
			return newSt.Err()
		}
	}

	return newSt.Err()
}

// ===== ErrorInfo Injector Interceptor =====

func UnaryServerErrorInfoInjector() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			err = injectErrorInfo(ctx, info.FullMethod, err)
		}
		return resp, err
	}
}

func StreamServerErrorInfoInjector() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)
		if err != nil {
			err = injectErrorInfo(stream.Context(), info.FullMethod, err)
		}
		return err
	}
}

func injectErrorInfo(ctx context.Context, fullMethod string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	methodDescriptor, ok := MethodDescriptorFromContext(ctx)
	if !ok {
		return err
	}

	serviceDescriptor := methodDescriptor.Parent().(protoreflect.ServiceDescriptor)
	defaultHost, hostErr := pbutil.GetExtension[string](serviceDescriptor.Options(), annotations.E_DefaultHost)
	if hostErr != nil {
		if errors.Is(hostErr, pbutil.ErrExtensionNotFound) {
			return err
		}
		return err
	}

	var domain string
	if _, after, ok := strings.Cut(defaultHost, "."); ok {
		domain = after
	}

	for _, detail := range st.Details() {
		if _, ok := detail.(*errdetails.ErrorInfo); ok {
			return err
		}
	}

	var existingDetails []protoadapt.MessageV1
	for _, detail := range st.Details() {
		if msg, ok := detail.(protoadapt.MessageV1); ok {
			existingDetails = append(existingDetails, msg)
		}
	}

	newErrorInfo := &errdetails.ErrorInfo{
		Domain: domain,
		Metadata: map[string]string{
			"service": defaultHost,
			"method":  string(methodDescriptor.Name()),
		},
	}

	allDetails := append([]protoadapt.MessageV1{newErrorInfo}, existingDetails...)

	newSt := status.New(st.Code(), st.Message())
	newSt, detailsErr := newSt.WithDetails(allDetails...)
	if detailsErr != nil {
		slog.ErrorContext(ctx, "injecting error info",
			"error", detailsErr,
			"method", fullMethod,
		)
		return st.Err()
	}

	return newSt.Err()
}

package middleware

import (
	"context"
	"log/slog"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/malonaz/core/go/pbutil"
)

// serviceMetadata caches service metadata extracted from proto definitions
type serviceMetadata struct {
	defaultHost string
	domain      string
}

// ===== Debug Info Scrubber Interceptor =====

// debugInfoScrubber removes debug information from errors
type debugInfoScrubber struct{}

// UnaryServerDebugInfoScrubber strips debug info from errors before they're sent to clients.
// Debug info is logged but not included in the response.
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

// StreamServerDebugInfoScrubber strips debug info from errors before they're sent to clients.
// Debug info is logged but not included in the response.
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

// scrubDebugInfo removes DebugInfo details from errors after logging them
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
			// Log debug info but don't include it in the response
			hasDebugInfo = true
			if len(d.StackEntries) > 0 {
				slog.DebugContext(ctx, "error debug info",
					"method", fullMethod,
					"stack", d.StackEntries,
					"detail", d.Detail,
				)
			}
		default:
			// Keep all other details
			if msg, ok := detail.(protoadapt.MessageV1); ok {
				otherDetails = append(otherDetails, msg)
			}
		}
	}

	// If no debug info was found, return original error
	if !hasDebugInfo {
		return err
	}

	// Create new status without debug info
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

// errorInfoInjector adds ErrorInfo metadata to errors
type errorInfoInjector struct {
	fullMethodNameToServiceMetadata map[string]*serviceMetadata
}

// newErrorInfoInjector creates a new error info injector with service metadata
func newErrorInfoInjector() *errorInfoInjector {
	ei := &errorInfoInjector{
		fullMethodNameToServiceMetadata: make(map[string]*serviceMetadata),
	}
	ei.buildServiceMetadataCache()
	return ei
}

// buildServiceMetadataCache extracts service metadata from proto file descriptors
func (ei *errorInfoInjector) buildServiceMetadataCache() {
	files := protoregistry.GlobalFiles
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			serviceFullName := string(service.FullName())

			// Extract default_host from service options
			if !pbutil.HasExtension(service.Options(), annotations.E_DefaultHost) {
				continue
			}

			defaultHost := pbutil.Must(pbutil.GetExtension[string](service.Options(), annotations.E_DefaultHost))

			// Extract domain from default_host (everything after first dot)
			var domain string
			if _, after, ok := strings.Cut(defaultHost, "."); ok {
				domain = after
			}

			metadata := &serviceMetadata{
				defaultHost: defaultHost,
				domain:      domain,
			}

			// Store metadata for each method in the service
			methods := service.Methods()
			for j := 0; j < methods.Len(); j++ {
				method := methods.Get(j)
				fullMethodName := "/" + serviceFullName + "/" + string(method.Name())
				ei.fullMethodNameToServiceMetadata[fullMethodName] = metadata
			}
		}
		return true
	})
}

// UnaryServerErrorInfoInjector adds ErrorInfo metadata to errors if not already present.
func UnaryServerErrorInfoInjector() grpc.UnaryServerInterceptor {
	injector := newErrorInfoInjector()

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)

		if err != nil {
			err = injector.injectErrorInfo(ctx, info.FullMethod, err)
		}

		return resp, err
	}
}

// StreamServerErrorInfoInjector adds ErrorInfo metadata to errors if not already present.
func StreamServerErrorInfoInjector() grpc.StreamServerInterceptor {
	injector := newErrorInfoInjector()

	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)

		if err != nil {
			err = injector.injectErrorInfo(stream.Context(), info.FullMethod, err)
		}

		return err
	}
}

// injectErrorInfo adds ErrorInfo to errors that don't already have it
func (ei *errorInfoInjector) injectErrorInfo(ctx context.Context, fullMethod string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	// Get service metadata using full method name
	metadata := ei.fullMethodNameToServiceMetadata[fullMethod]

	// If no metadata available, return original error
	if metadata == nil {
		return err
	}

	// Check if ErrorInfo already exists
	var existingErrorInfo bool
	for _, detail := range st.Details() {
		if _, ok := detail.(*errdetails.ErrorInfo); ok {
			existingErrorInfo = true
			break
		}
	}

	// If ErrorInfo already exists, return original error
	if existingErrorInfo {
		return err
	}

	// Convert existing details to MessageV1
	var existingDetails []protoadapt.MessageV1
	for _, detail := range st.Details() {
		if msg, ok := detail.(protoadapt.MessageV1); ok {
			existingDetails = append(existingDetails, msg)
		}
	}

	// Extract just the method name from full method path
	// fullMethod format: "/package.ServiceName/MethodName"
	methodName := fullMethod
	if lastSlash := strings.LastIndex(fullMethod, "/"); lastSlash != -1 {
		methodName = fullMethod[lastSlash+1:]
	}

	// Create new ErrorInfo with service metadata
	newErrorInfo := &errdetails.ErrorInfo{
		Domain: metadata.domain,
		Metadata: map[string]string{
			"service": metadata.defaultHost,
			"method":  methodName,
		},
	}

	// Prepend ErrorInfo to existing details
	allDetails := append([]protoadapt.MessageV1{newErrorInfo}, existingDetails...)

	// Create new status with ErrorInfo
	newSt := status.New(st.Code(), st.Message())
	newSt, err = newSt.WithDetails(allDetails...)
	if err != nil {
		slog.ErrorContext(ctx, "injecting error info",
			"error", err,
			"method", fullMethod,
		)
		return st.Err() // Return original error on failure
	}

	return newSt.Err()
}

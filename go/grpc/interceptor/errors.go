package interceptor

import (
	"context"
	"log/slog"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// serviceMetadata caches service metadata extracted from proto definitions
type serviceMetadata struct {
	defaultHost string
	domain      string
}

// errorInterceptor processes errors before they're sent to clients
type errorInterceptor struct {
	fullMethodNameToServiceMetadata map[string]*serviceMetadata
}

// newErrorInterceptor creates a new error interceptor with service metadata
func newErrorInterceptor() *errorInterceptor {
	ei := &errorInterceptor{
		fullMethodNameToServiceMetadata: make(map[string]*serviceMetadata),
	}
	ei.buildServiceMetadataCache()
	return ei
}

// buildServiceMetadataCache extracts service metadata from proto file descriptors
func (ei *errorInterceptor) buildServiceMetadataCache() {
	files := protoregistry.GlobalFiles
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			serviceFullName := string(service.FullName())

			// Extract default_host from service options
			if proto.HasExtension(service.Options(), annotations.E_DefaultHost) {
				defaultHost := proto.GetExtension(service.Options(), annotations.E_DefaultHost).(string)

				// Extract domain from default_host (everything after first dot)
				domain := ""
				if idx := strings.Index(defaultHost, "."); idx != -1 {
					domain = defaultHost[idx+1:]
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
		}
		return true
	})
}

// UnaryServerError processes errors before they're sent to clients.
// It strips debug info (after logging) and injects ErrorInfo metadata.
// Pass nil for files if you don't want service metadata injection.
func UnaryServerError() grpc.UnaryServerInterceptor {
	ei := newErrorInterceptor()

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)

		if err != nil {
			err = ei.processError(ctx, info.FullMethod, err)
		}

		return resp, err
	}
}

// StreamServerError processes errors before they're sent to clients.
// It strips debug info (after logging) and injects ErrorInfo metadata.
// Pass nil for files if you don't want service metadata injection.
func StreamServerError() grpc.StreamServerInterceptor {
	ei := newErrorInterceptor()

	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)

		if err != nil {
			err = ei.processError(stream.Context(), info.FullMethod, err)
		}

		return err
	}
}

// processError handles error transformation: strips debug info and adds ErrorInfo
func (ei *errorInterceptor) processError(ctx context.Context, fullMethod string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	// Get service metadata using full method name
	metadata := ei.fullMethodNameToServiceMetadata[fullMethod]

	// Process existing details
	var existingErrorInfo bool
	var otherDetails []protoadapt.MessageV1

	for _, detail := range st.Details() {
		switch d := detail.(type) {
		case *errdetails.DebugInfo:
			// Log debug info but don't include it in the response
			if len(d.StackEntries) > 0 {
				slog.DebugContext(ctx, "error debug info",
					"method", fullMethod,
					"stack", d.StackEntries,
					"detail", d.Detail,
				)
			}
		case *errdetails.ErrorInfo:
			// Keep existing ErrorInfo as-is
			existingErrorInfo = true
			if msg, ok := detail.(protoadapt.MessageV1); ok {
				otherDetails = append(otherDetails, msg)
			}
		default:
			// Convert to MessageV1 for status.WithDetails
			if msg, ok := detail.(protoadapt.MessageV1); ok {
				otherDetails = append(otherDetails, msg)
			}
		}
	}

	// Only create ErrorInfo if one doesn't exist and we have metadata
	if !existingErrorInfo && metadata != nil {
		// Create new ErrorInfo with service metadata
		newErrorInfo := &errdetails.ErrorInfo{
			Domain: metadata.domain,
			Metadata: map[string]string{
				"service": metadata.defaultHost,
			},
		}
		otherDetails = append([]protoadapt.MessageV1{newErrorInfo}, otherDetails...)
	}

	// Build new error details (without debug info, but with original or new ErrorInfo)
	newDetails := otherDetails

	// Create new status
	newSt := status.New(st.Code(), st.Message())
	if len(newDetails) > 0 {
		var newErr error
		newSt, newErr = newSt.WithDetails(newDetails...)
		if newErr != nil {
			slog.ErrorContext(ctx, "attaching error details",
				"error", newErr,
				"method", fullMethod,
			)
			return newSt.Err()
		}
	}

	return newSt.Err()
}

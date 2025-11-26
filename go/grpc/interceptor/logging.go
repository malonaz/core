package interceptor

import (
	"context"
	"fmt"
	"log/slog"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	logFieldsTagKey = logFieldsTagKeyType("log-fields-tag-key")

	loggingInterceptorOptions = []grpc_logging.Option{
		grpc_logging.WithLogOnEvents(grpc_logging.FinishCall),
		grpc_logging.WithLevels(errorCodeToLogLevel),
		grpc_logging.WithErrorFields(extractErrorDetails),
	}
)

type logFieldsTagKeyType string

type logFieldsTag struct {
	fields []any
}

func setLogFieldsTagOntoContext(ctx context.Context) context.Context {
	tag := &logFieldsTag{}
	return context.WithValue(ctx, logFieldsTagKey, tag)
}

// Inject fields onto the log context tag.
func InjectLogFields(ctx context.Context, args ...any) bool {
	tag, ok := ctx.Value(logFieldsTagKey).(*logFieldsTag)
	if !ok {
		return false
	}
	tag.fields = append(tag.fields, args...)
	return true
}

func GetFields(ctx context.Context) ([]any, bool) {
	tag, ok := ctx.Value(logFieldsTagKey).(*logFieldsTag)
	if !ok {
		return nil, false
	}
	return tag.fields, true
}

// UnaryServerContextTagsInterceptor initializes context tags.
func UnaryServerLogContextTagInitializer() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = setLogFieldsTagOntoContext(ctx)
		return handler(ctx, req)
	}
}

// StreamServerContextTagsInterceptor initializes context tags.
func StreamServerLogContextTagInitializer() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := setLogFieldsTagOntoContext(stream.Context())
		return handler(srv, &grpc_middleware.WrappedServerStream{ServerStream: stream, WrappedContext: ctx})
	}
}

func UnaryServerLogging(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return grpc_logging.UnaryServerInterceptor(loggingInterceptor(logger), loggingInterceptorOptions...)
}

func StreamServerLogging(logger *slog.Logger) grpc.StreamServerInterceptor {
	return grpc_logging.StreamServerInterceptor(loggingInterceptor(logger), loggingInterceptorOptions...)
}

// Map gRPC return codes to log levels.
func errorCodeToLogLevel(code codes.Code) grpc_logging.Level {
	switch code {
	case codes.OK, codes.Canceled, codes.AlreadyExists:
		return grpc_logging.LevelDebug

	case codes.DeadlineExceeded, codes.PermissionDenied, codes.ResourceExhausted, codes.FailedPrecondition, codes.Aborted,
		codes.OutOfRange, codes.Unavailable, codes.Unauthenticated, codes.InvalidArgument, codes.NotFound:
		return grpc_logging.LevelWarn

	case codes.Unknown, codes.Unimplemented, codes.Internal, codes.DataLoss:
		return grpc_logging.LevelError

	default:
		return grpc_logging.LevelError
	}
}

// interceptorLogger adapts logrus logger to interceptor logger.
func loggingInterceptor(logger *slog.Logger) grpc_logging.Logger {
	return grpc_logging.LoggerFunc(func(ctx context.Context, level grpc_logging.Level, msg string, fields ...any) {
		// Get fields from context tag and append them
		if ctxFields, ok := GetFields(ctx); ok {
			fields = append(fields, ctxFields...)
		}

		log := logger.With(fields...)
		switch level {
		case grpc_logging.LevelDebug:
			log.DebugContext(ctx, msg)
		case grpc_logging.LevelInfo:
			log.InfoContext(ctx, msg)
		case grpc_logging.LevelWarn:
			log.WarnContext(ctx, msg)
		case grpc_logging.LevelError:
			log.ErrorContext(ctx, msg)
		default:
			log.InfoContext(ctx, msg)
		}
	})
}

func extractErrorDetails(err error) grpc_logging.Fields {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	details := st.Details()
	if len(details) == 0 {
		return nil
	}
	fields := make(grpc_logging.Fields, 0, len(details)*2)
	for _, detail := range details {
		var detailType string
		if msg, ok := detail.(proto.Message); ok {
			detailType = string(msg.ProtoReflect().Descriptor().Name())
		} else {
			detailType = fmt.Sprintf("%T", detail)
		}
		fields = append(fields, "grpc.error."+detailType, detail)
	}
	return fields
}

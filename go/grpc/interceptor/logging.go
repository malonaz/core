package interceptor

import (
	"context"
	"log/slog"

	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	loggingInterceptorOptions = []grpc_logging.Option{
		grpc_logging.WithLogOnEvents(grpc_logging.FinishCall),
		grpc_logging.WithLevels(errorCodeToLogLevel),
	}
)

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

		logger = logger.With(fields...)
		switch level {
		case grpc_logging.LevelDebug:
			logger.DebugContext(ctx, msg)
		case grpc_logging.LevelInfo:
			logger.InfoContext(ctx, msg)
		case grpc_logging.LevelWarn:
			logger.WarnContext(ctx, msg)
		case grpc_logging.LevelError:
			logger.ErrorContext(ctx, msg)
		default:
			logger.InfoContext(ctx, msg)
		}
	})
}

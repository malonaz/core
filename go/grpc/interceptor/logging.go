package interceptor

import (
	"context"

	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/malonaz/core/go/logging"
)

var (
	loggingInterceptorOptions = []grpc_logging.Option{
		grpc_logging.WithLogOnEvents(grpc_logging.FinishCall),
		grpc_logging.WithLevels(errorCodeToLogLevel),
	}
)

func UnaryServerLogging(log *logging.Logger) grpc.UnaryServerInterceptor {
	return grpc_logging.UnaryServerInterceptor(loggingInterceptor(log), loggingInterceptorOptions...)
}

func StreamServerLogging(log *logging.Logger) grpc.StreamServerInterceptor {
	return grpc_logging.StreamServerInterceptor(loggingInterceptor(log), loggingInterceptorOptions...)
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
func loggingInterceptor(log *logging.Logger) grpc_logging.Logger {
	return grpc_logging.LoggerFunc(func(ctx context.Context, level grpc_logging.Level, msg string, fields ...any) {
		f := make(map[string]any, len(fields)/2)
		for iterator := grpc_logging.Fields(fields).Iterator(); iterator.Next(); {
			k, v := iterator.At()
			f[k] = v
		}

		logger := log.WithFields(f).WithContext(ctx)
		switch level {
		case grpc_logging.LevelDebug:
			logger.Debug(msg)
		case grpc_logging.LevelInfo:
			logger.Info(msg)
		case grpc_logging.LevelWarn:
			logger.Warn(msg)
		case grpc_logging.LevelError:
			logger.Error(msg)
		default:
			logger.Panicf("unknown level %v", level)
		}
	})
}

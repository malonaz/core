package middleware

import (
	"context"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/malonaz/core/go/pbutil"
)

func UnaryServerAIPLogging() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if message, ok := req.(proto.Message); ok {
			injectAIPLogFields(ctx, message)
		}
		return handler(ctx, req)
	}
}

func StreamServerAIPLogging() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &aipLoggingServerStream{ServerStream: stream})
	}
}

type aipLoggingServerStream struct {
	grpc.ServerStream
	firstRecv bool
}

func (s *aipLoggingServerStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if !s.firstRecv {
		s.firstRecv = true
		if message, ok := m.(proto.Message); ok {
			injectAIPLogFields(s.Context(), message)
		}
	}
	return nil
}

func injectAIPLogFields(ctx context.Context, message proto.Message) {
	injectResourceReferenceFields(ctx, message.ProtoReflect(), "", false)
}

// aip_logging.go

func injectResourceReferenceFields(ctx context.Context, reflectMessage protoreflect.Message, prefix string, nested bool) {
	descriptor := reflectMessage.Descriptor()
	fields := descriptor.Fields()

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsList() || field.IsMap() {
			continue
		}

		fieldName := string(field.Name())
		key := "grpc.request." + prefix + fieldName

		if field.Kind() == protoreflect.StringKind {
			_, refErr := pbutil.GetExtension[*annotations.ResourceReference](field.Options(), annotations.E_ResourceReference)
			if refErr != nil {
				if !nested || fieldName != "name" {
					continue
				}
				behaviors, behaviorErr := pbutil.GetExtension[[]annotations.FieldBehavior](field.Options(), annotations.E_FieldBehavior)
				if behaviorErr != nil {
					continue
				}
				isIdentifier := false
				for _, b := range behaviors {
					if b == annotations.FieldBehavior_IDENTIFIER {
						isIdentifier = true
						break
					}
				}
				if !isIdentifier {
					continue
				}
			}
			InjectLogFields(ctx, key, reflectMessage.Get(field).String())
			continue
		}

		if !nested && field.Kind() == protoreflect.MessageKind && reflectMessage.Has(field) {
			injectResourceReferenceFields(ctx, reflectMessage.Get(field).Message(), fieldName+".", true)
		}
	}
}

package middleware

import (
	"context"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

const (
	metadataKeyFieldMask       = "x-field-mask"
	metadataKeyFieldMaskStrict = "x-field-mask-strict"
	metadataKeyReadMask        = "x-read-mask"
	metadataKeyReadMaskStrict  = "x-read-mask-strict"
)

func WithFieldMask(ctx context.Context, paths string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metadataKeyFieldMask, paths)
}

func WithFieldMaskStrict(ctx context.Context, paths string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metadataKeyFieldMaskStrict, paths)
}

func WithReadMask(ctx context.Context, paths string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metadataKeyReadMask, paths)
}

func WithReadMaskStrict(ctx context.Context, paths string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metadataKeyReadMaskStrict, paths)
}

func UnaryServerFieldMask() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return handler(ctx, req)
		}

		fieldMask, strict := extractFieldMask(md)
		readMask, readMaskStrict := extractReadMask(md)
		if fieldMask == nil && readMask == nil {
			return handler(ctx, req)
		}

		response, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}
		message, ok := response.(proto.Message)
		if !ok {
			return nil, status.Errorf(codes.Internal, "response is not a protobuf message: %T", response)
		}

		if fieldMask != nil && !fieldMask.IsWildcardPath() {
			if strict {
				if err := fieldMask.Validate(message); err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "invalid field mask: %v", err)
				}
			}
			fieldMask.Apply(message)
		}

		if readMask != nil && !readMask.IsWildcardPath() {
			if err := applyReadMask(message, readMask, readMaskStrict); err != nil {
				return nil, err
			}
		}

		return response, nil
	}
}

func StreamServerFieldMask() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return handler(srv, stream)
		}

		fieldMask, fieldMaskStrict := extractFieldMask(md)
		readMask, readMaskStrict := extractReadMask(md)
		if fieldMask == nil && readMask == nil {
			return handler(srv, stream)
		}

		wrappedStream := &fieldMaskServerStream{
			ServerStream:    stream,
			fieldMask:       fieldMask,
			fieldMaskStrict: fieldMaskStrict,
			readMask:        readMask,
			readMaskStrict:  readMaskStrict,
		}
		return handler(srv, wrappedStream)
	}
}

func extractFieldMask(md metadata.MD) (*pbfieldmask.FieldMask, bool) {
	if values := md.Get(metadataKeyFieldMaskStrict); len(values) > 0 {
		return pbfieldmask.FromString(values[0]), true
	}
	if values := md.Get(metadataKeyFieldMask); len(values) > 0 {
		return pbfieldmask.FromString(values[0]), false
	}
	return nil, false
}

func extractReadMask(md metadata.MD) (*pbfieldmask.FieldMask, bool) {
	if values := md.Get(metadataKeyReadMaskStrict); len(values) > 0 {
		return pbfieldmask.FromString(values[0]), true
	}
	if values := md.Get(metadataKeyReadMask); len(values) > 0 {
		return pbfieldmask.FromString(values[0]), false
	}
	return nil, false
}

type fieldMaskServerStream struct {
	grpc.ServerStream
	fieldMask       *pbfieldmask.FieldMask
	fieldMaskStrict bool
	readMask        *pbfieldmask.FieldMask
	readMaskStrict  bool
}

func (s *fieldMaskServerStream) SendMsg(m any) error {
	message, ok := m.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "message is not a protobuf message: %T", m)
	}
	if s.fieldMask != nil && !s.fieldMask.IsWildcardPath() {
		if s.fieldMaskStrict {
			if err := s.fieldMask.Validate(message); err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid field mask: %v", err)
			}
		}
		s.fieldMask.Apply(message)
	}
	if s.readMask != nil && !s.readMask.IsWildcardPath() {
		if err := applyReadMask(message, s.readMask, s.readMaskStrict); err != nil {
			return err
		}
	}
	return s.ServerStream.SendMsg(m)
}

// applyReadMask applies a read mask to the resource within the response message.
// It first checks if the response itself has a google.api.resource annotation.
// If not, it recurses one level deep to find a field whose message type has one.
// If a resource field is found (repeated or singular), the mask is applied there.
// If nothing is found, the mask is applied to the top-level response as a fallback.
func applyReadMask(message proto.Message, readMask *pbfieldmask.FieldMask, strict bool) error {
	reflectMessage := message.ProtoReflect()
	descriptor := reflectMessage.Descriptor()

	// Check if the response itself is a resource.
	if hasResourceAnnotation(descriptor) {
		return applyReadMaskToMessage(message, readMask, strict)
	}

	// Scan fields one level deep for a resource message.
	fields := descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Kind() != protoreflect.MessageKind {
			continue
		}

		targetDescriptor := field.Message()
		if field.IsList() {
			targetDescriptor = field.Message()
		}

		if !hasResourceAnnotation(targetDescriptor) {
			continue
		}

		if field.IsList() {
			list := reflectMessage.Get(field).List()
			for j := 0; j < list.Len(); j++ {
				element := list.Get(j).Message().Interface()
				if err := applyReadMaskToMessage(element, readMask, strict); err != nil {
					return err
				}
			}
			return nil
		}

		if reflectMessage.Has(field) {
			nested := reflectMessage.Get(field).Message().Interface()
			return applyReadMaskToMessage(nested, readMask, strict)
		}
		return nil
	}

	// Fallback: apply to the top-level response.
	return applyReadMaskToMessage(message, readMask, strict)
}

func applyReadMaskToMessage(message proto.Message, readMask *pbfieldmask.FieldMask, strict bool) error {
	if strict {
		if err := readMask.Validate(message); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid read mask: %v", err)
		}
	}
	readMask.Apply(message)
	return nil
}

func hasResourceAnnotation(descriptor protoreflect.MessageDescriptor) bool {
	_, err := pbutil.GetExtension[*annotations.ResourceDescriptor](descriptor.Options(), annotations.E_Resource)
	return err == nil
}

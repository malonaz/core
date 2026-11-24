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

// UnaryServerFieldMask prunes responses to the request's field and read masks. Strict masks are
// validated against the response type before the handler runs: a write must not be performed only to
// fail on its mask.
func UnaryServerFieldMask() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		masks, ok := extractResponseMasks(ctx)
		if !ok {
			return handler(ctx, req)
		}
		if err := masks.validate(ctx, info.FullMethod); err != nil {
			return nil, err
		}

		response, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}
		message, ok := response.(proto.Message)
		if !ok {
			return nil, status.Errorf(codes.Internal, "response is not a protobuf message: %T", response)
		}
		masks.apply(message)
		return response, nil
	}
}

// StreamServerFieldMask is UnaryServerFieldMask for streams: masks are validated before the handler
// runs and applied to every message sent.
func StreamServerFieldMask() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		masks, ok := extractResponseMasks(stream.Context())
		if !ok {
			return handler(srv, stream)
		}
		if err := masks.validate(stream.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, &fieldMaskServerStream{ServerStream: stream, masks: masks})
	}
}

// responseMasks are the masks a request sets on its response.
type responseMasks struct {
	fieldMask       *pbfieldmask.FieldMask
	fieldMaskStrict bool
	readMask        *pbfieldmask.FieldMask
	readMaskStrict  bool
}

func extractResponseMasks(ctx context.Context) (*responseMasks, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, false
	}
	masks := &responseMasks{}
	masks.fieldMask, masks.fieldMaskStrict = extractMask(md, metadataKeyFieldMaskStrict, metadataKeyFieldMask)
	masks.readMask, masks.readMaskStrict = extractMask(md, metadataKeyReadMaskStrict, metadataKeyReadMask)
	return masks, masks.fieldMask != nil || masks.readMask != nil
}

// extractMask returns the mask set under either key, the strict one first. A wildcard masks nothing.
func extractMask(md metadata.MD, strictKey, key string) (*pbfieldmask.FieldMask, bool) {
	values, strict := md.Get(strictKey), true
	if len(values) == 0 {
		values, strict = md.Get(key), false
	}
	if len(values) == 0 {
		return nil, false
	}
	mask := pbfieldmask.FromString(values[0])
	if mask.IsWildcardPath() {
		return nil, false
	}
	return mask, strict
}

// validate checks the strict masks against the method's response type.
func (m *responseMasks) validate(ctx context.Context, fullMethod string) error {
	if !m.fieldMaskStrict && !m.readMaskStrict {
		return nil
	}
	methodDescriptor, ok := MethodDescriptorFromContext(ctx)
	if !ok {
		var err error
		if methodDescriptor, err = resolveMethodDescriptor(fullMethod); err != nil {
			return status.Errorf(codes.Internal, "resolving method descriptor: %v", err)
		}
	}
	response := methodDescriptor.Output()
	if m.fieldMaskStrict {
		if err := m.fieldMask.ValidateDescriptor(response); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid field mask: %v", err)
		}
	}
	if m.readMaskStrict {
		target := response
		if field := readMaskTarget(response); field != nil {
			target = field.Message()
		}
		if err := m.readMask.ValidateDescriptor(target); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid read mask: %v", err)
		}
	}
	return nil
}

func (m *responseMasks) apply(message proto.Message) {
	if m.fieldMask != nil {
		m.fieldMask.Apply(message)
	}
	if m.readMask != nil {
		applyReadMask(message, m.readMask)
	}
}

type fieldMaskServerStream struct {
	grpc.ServerStream
	masks *responseMasks
}

func (s *fieldMaskServerStream) SendMsg(m any) error {
	message, ok := m.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "message is not a protobuf message: %T", m)
	}
	s.masks.apply(message)
	return s.ServerStream.SendMsg(m)
}

// readMaskTarget returns the field holding the resource a read mask applies to, or nil when it
// applies to the response itself: a resource response, or one with no top-level resource field.
// The response_read_mask tool description (go/pbutil/pbjson) mirrors this choice.
func readMaskTarget(response protoreflect.MessageDescriptor) protoreflect.FieldDescriptor {
	if hasResourceAnnotation(response) {
		return nil
	}
	fields := response.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Kind() == protoreflect.MessageKind && hasResourceAnnotation(field.Message()) {
			return field
		}
	}
	return nil
}

// applyReadMask applies a read mask to the resource(s) within the response message.
func applyReadMask(message proto.Message, readMask *pbfieldmask.FieldMask) {
	reflectMessage := message.ProtoReflect()
	field := readMaskTarget(reflectMessage.Descriptor())
	switch {
	case field == nil:
		readMask.Apply(message)
	case field.IsList():
		list := reflectMessage.Get(field).List()
		for i := 0; i < list.Len(); i++ {
			readMask.Apply(list.Get(i).Message().Interface())
		}
	case reflectMessage.Has(field):
		readMask.Apply(reflectMessage.Get(field).Message().Interface())
	}
}

func hasResourceAnnotation(descriptor protoreflect.MessageDescriptor) bool {
	_, err := pbutil.GetExtension[*annotations.ResourceDescriptor](descriptor.Options(), annotations.E_Resource)
	return err == nil
}

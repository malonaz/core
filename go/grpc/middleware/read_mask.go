package middleware

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

const (
	metadataKeyReadMask       = "x-read-mask"
	metadataKeyReadMaskStrict = "x-read-mask-strict"
)

func WithReadMask(ctx context.Context, paths string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metadataKeyReadMask, paths)
}

func WithReadMaskStrict(ctx context.Context, paths string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metadataKeyReadMaskStrict, paths)
}

type readMaskConfig struct {
	paths  string
	strict bool
}

func extractReadMaskConfig(ctx context.Context) *readMaskConfig {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	if values := md.Get(metadataKeyReadMaskStrict); len(values) > 0 {
		return &readMaskConfig{paths: values[0], strict: true}
	}
	if values := md.Get(metadataKeyReadMask); len(values) > 0 {
		return &readMaskConfig{paths: values[0], strict: false}
	}
	return nil
}

type readMaskTargetInfo struct {
	fieldName protoreflect.Name
	isList    bool
}

type readMaskInterceptor struct {
	fullMethodToTargetInfo map[string]*readMaskTargetInfo
}

func newReadMaskInterceptor(serviceDescriptors []protoreflect.ServiceDescriptor) *readMaskInterceptor {
	fullMethodToTargetInfo := map[string]*readMaskTargetInfo{}
	for _, serviceDescriptor := range serviceDescriptors {
		methods := serviceDescriptor.Methods()
		for i := 0; i < methods.Len(); i++ {
			method := methods.Get(i)
			readMaskTarget, err := pbutil.GetExtension[string](method.Options(), aippb.E_ReadMaskTarget)
			if err != nil {
				if errors.Is(err, pbutil.ErrExtensionNotFound) {
					continue
				}
				panic(err)
			}

			outputDescriptor := method.Output()
			field := outputDescriptor.Fields().ByName(protoreflect.Name(readMaskTarget))
			if field == nil {
				panic(fmt.Sprintf("read mask target field %q not found on %s", readMaskTarget, outputDescriptor.FullName()))
			}
			if field.Kind() != protoreflect.MessageKind {
				continue
			}

			fullMethod := "/" + string(serviceDescriptor.FullName()) + "/" + string(method.Name())
			fullMethodToTargetInfo[fullMethod] = &readMaskTargetInfo{
				fieldName: field.Name(),
				isList:    field.IsList(),
			}
		}
	}
	return &readMaskInterceptor{fullMethodToTargetInfo: fullMethodToTargetInfo}
}

func UnaryServerReadMask(serviceDescriptors []protoreflect.ServiceDescriptor) grpc.UnaryServerInterceptor {
	interceptor := newReadMaskInterceptor(serviceDescriptors)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		config := extractReadMaskConfig(ctx)
		if config == nil {
			return handler(ctx, req)
		}
		fieldMask := pbfieldmask.FromString(config.paths)
		if fieldMask.IsWildcardPath() {
			return handler(ctx, req)
		}

		response, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}
		message, ok := response.(proto.Message)
		if !ok {
			return nil, status.Errorf(codes.Internal, "response is not a protobuf message: %T", response).Err()
		}

		if err := interceptor.applyReadMask(info.FullMethod, message, fieldMask, config.strict); err != nil {
			return nil, err
		}
		return response, nil
	}
}

func StreamServerReadMask(serviceDescriptors []protoreflect.ServiceDescriptor) grpc.StreamServerInterceptor {
	interceptor := newReadMaskInterceptor(serviceDescriptors)
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		config := extractReadMaskConfig(stream.Context())
		if config == nil {
			return handler(srv, stream)
		}
		fieldMask := pbfieldmask.FromString(config.paths)
		if fieldMask.IsWildcardPath() {
			return handler(srv, stream)
		}

		return handler(srv, &readMaskServerStream{
			ServerStream: stream,
			interceptor:  interceptor,
			fullMethod:   info.FullMethod,
			fieldMask:    fieldMask,
			strict:       config.strict,
		})
	}
}

type readMaskServerStream struct {
	grpc.ServerStream
	interceptor *readMaskInterceptor
	fullMethod  string
	fieldMask   *pbfieldmask.FieldMask
	strict      bool
	validated   bool
}

func (s *readMaskServerStream) SendMsg(m any) error {
	message, ok := m.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "message is not a protobuf message: %T", m).Err()
	}
	strict := s.strict && !s.validated
	if err := s.interceptor.applyReadMask(s.fullMethod, message, s.fieldMask, strict); err != nil {
		return err
	}
	s.validated = true
	return s.ServerStream.SendMsg(m)
}

func (ri *readMaskInterceptor) applyReadMask(fullMethod string, message proto.Message, fieldMask *pbfieldmask.FieldMask, validate bool) error {
	targetInfo := ri.fullMethodToTargetInfo[fullMethod]
	if targetInfo != nil {
		return ri.applyToTargetField(message, targetInfo, fieldMask, validate)
	}
	if validate {
		if err := fieldMask.Validate(message); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid read mask: %v", err).Err()
		}
	}
	fieldMask.Apply(message)
	return nil
}

func (ri *readMaskInterceptor) applyToTargetField(response proto.Message, info *readMaskTargetInfo, fieldMask *pbfieldmask.FieldMask, validate bool) error {
	reflectMessage := response.ProtoReflect()
	field := reflectMessage.Descriptor().Fields().ByName(info.fieldName)
	if field == nil {
		return nil
	}
	if info.isList {
		listValue := reflectMessage.Get(field).List()
		for i := 0; i < listValue.Len(); i++ {
			target := listValue.Get(i).Message().Interface()
			if validate && i == 0 {
				if err := fieldMask.Validate(target); err != nil {
					return status.Errorf(codes.InvalidArgument, "invalid read mask: %v", err).Err()
				}
			}
			fieldMask.Apply(target)
		}
		return nil
	}
	if !reflectMessage.Has(field) {
		return nil
	}
	target := reflectMessage.Get(field).Message().Interface()
	if validate {
		if err := fieldMask.Validate(target); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid read mask: %v", err).Err()
		}
	}
	fieldMask.Apply(target)
	return nil
}

package interceptor

import (
	"context"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	gatewaypb "github.com/malonaz/core/genproto/codegen/gateway/v1"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

const (
	metadataKeyFieldMask = "x-field-mask"
)

func WithFieldMask(ctx context.Context, paths string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metadataKeyFieldMask, paths)
}

func UnaryServerFieldMask() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return handler(ctx, req)
		}
		values := md.Get(metadataKeyFieldMask)
		if len(values) == 0 {
			return handler(ctx, req)
		}
		fieldMask := pbfieldmask.FromString(values[0])
		if fieldMask.IsWildcardPath() {
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

		if collectionField := getListCollectionField(info.FullMethod, message); collectionField.IsValid() {
			applyFieldMaskToRepeatedField(collectionField, fieldMask)
		} else {
			fieldMask.Apply(message)
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
		values := md.Get(metadataKeyFieldMask)
		if len(values) == 0 {
			return handler(srv, stream)
		}
		fieldMask := pbfieldmask.FromString(values[0])
		if fieldMask.IsWildcardPath() {
			return handler(srv, stream)
		}

		wrappedStream := &fieldMaskServerStream{
			ServerStream: stream,
			fieldMask:    fieldMask,
		}
		return handler(srv, wrappedStream)
	}
}

type fieldMaskServerStream struct {
	grpc.ServerStream
	fieldMask *pbfieldmask.FieldMask
}

func (s *fieldMaskServerStream) SendMsg(m any) error {
	message, ok := m.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "message is not a protobuf message: %T", m)
	}
	s.fieldMask.Apply(message)
	return s.ServerStream.SendMsg(m)
}

func getListCollectionField(fullMethod string, response proto.Message) protoreflect.Value {
	parts := strings.Split(fullMethod, "/")
	if len(parts) < 2 {
		return protoreflect.Value{}
	}
	serviceName := parts[len(parts)-2]
	methodName := parts[len(parts)-1]

	serviceDesc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return protoreflect.Value{}
	}
	svc, ok := serviceDesc.(protoreflect.ServiceDescriptor)
	if !ok {
		return protoreflect.Value{}
	}
	methodDesc := svc.Methods().ByName(protoreflect.Name(methodName))
	if methodDesc == nil {
		return protoreflect.Value{}
	}

	targetMethodDesc := methodDesc
	opts := methodDesc.Options()
	if proto.HasExtension(opts, gatewaypb.E_Opts) {
		handlerOpts := proto.GetExtension(opts, gatewaypb.E_Opts).(*gatewaypb.HandlerOpts)
		if handlerOpts.GetProxy() != "" {
			resolved, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(handlerOpts.Proxy))
			if err == nil {
				if methodDesc, ok := resolved.(protoreflect.MethodDescriptor); ok {
					targetMethodDesc = methodDesc
					methodName = string(methodDesc.Name())
				}
			}
		}
	}

	targetOpts := targetMethodDesc.Options()
	if !proto.HasExtension(targetOpts, aippb.E_StandardMethod) {
		return protoreflect.Value{}
	}
	standardMethod := proto.GetExtension(targetOpts, aippb.E_StandardMethod).(*aippb.StandardMethod)
	if standardMethod == nil || standardMethod.Resource == "" {
		return protoreflect.Value{}
	}

	if !(strings.HasPrefix(methodName, "List") || strings.HasPrefix(methodName, "BatchGet")) {
		return protoreflect.Value{}
	}

	msgReflect := response.ProtoReflect()
	fields := msgReflect.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !fd.IsList() || fd.Kind() != protoreflect.MessageKind {
			continue
		}
		itemMsgDesc := fd.Message()
		itemOpts := itemMsgDesc.Options()
		if !proto.HasExtension(itemOpts, annotations.E_Resource) {
			continue
		}
		resource := proto.GetExtension(itemOpts, annotations.E_Resource).(*annotations.ResourceDescriptor)
		resourcePlural := xstrings.ToPascalCase(resource.GetPlural())
		if resource.GetType() == standardMethod.Resource {
			expectedList := "List" + resourcePlural
			expectedBatchGet := "BatchGet" + resourcePlural
			if methodName == expectedList || methodName == expectedBatchGet {
				return msgReflect.Get(fd)
			}
		}
	}
	return protoreflect.Value{}
}

func getListCollectionFieldOld(fullMethod string, response proto.Message) protoreflect.Value {
	parts := strings.Split(fullMethod, "/")
	if len(parts) < 2 {
		return protoreflect.Value{}
	}
	serviceName := parts[len(parts)-2]
	methodName := parts[len(parts)-1]

	serviceDesc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return protoreflect.Value{}
	}
	svc, ok := serviceDesc.(protoreflect.ServiceDescriptor)
	if !ok {
		return protoreflect.Value{}
	}
	methodDesc := svc.Methods().ByName(protoreflect.Name(methodName))
	if methodDesc == nil {
		return protoreflect.Value{}
	}

	opts := methodDesc.Options()
	if !proto.HasExtension(opts, aippb.E_StandardMethod) {
		return protoreflect.Value{}
	}
	standardMethod := proto.GetExtension(opts, aippb.E_StandardMethod).(*aippb.StandardMethod)
	if standardMethod == nil || standardMethod.Resource == "" {
		return protoreflect.Value{}
	}

	if !(strings.HasPrefix(methodName, "List") || strings.HasPrefix(methodName, "BatchGet")) {
		return protoreflect.Value{}
	}

	msgReflect := response.ProtoReflect()
	fields := msgReflect.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !fd.IsList() || fd.Kind() != protoreflect.MessageKind {
			continue
		}
		itemMsgDesc := fd.Message()
		itemOpts := itemMsgDesc.Options()
		if !proto.HasExtension(itemOpts, annotations.E_Resource) {
			continue
		}
		resource := proto.GetExtension(itemOpts, annotations.E_Resource).(*annotations.ResourceDescriptor)
		resourcePlural := xstrings.ToPascalCase(resource.GetPlural())
		if resource.GetType() == standardMethod.Resource {
			if methodName == "List"+resourcePlural || methodName == "BatchGet"+resourcePlural {
				return msgReflect.Get(fd)
			}
		}
	}

	return protoreflect.Value{}
}

func applyFieldMaskToRepeatedField(listValue protoreflect.Value, fieldMask *pbfieldmask.FieldMask) {
	list := listValue.List()
	for i := 0; i < list.Len(); i++ {
		item := list.Get(i).Message().Interface()
		fieldMask.Apply(item)
	}
}

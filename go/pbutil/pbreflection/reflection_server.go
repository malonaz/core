package pbreflection

import (
	"context"
	"fmt"

	"github.com/malonaz/core/go/grpc/grpcinproc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	grpc_reflection_v1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func NewTypesFromFiles(files *protoregistry.Files) (*protoregistry.Types, error) {
	types := new(protoregistry.Types)
	var registrationErr error
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if err := registerTypes(types, fd); err != nil {
			registrationErr = fmt.Errorf("registering types from %q: %v", fd.Path(), err)
			return false
		}
		return true
	})
	if registrationErr != nil {
		return nil, registrationErr
	}
	return types, nil
}

func registerTypes(types *protoregistry.Types, fd protoreflect.FileDescriptor) error {
	for i := 0; i < fd.Messages().Len(); i++ {
		if err := registerMessageTypes(types, fd.Messages().Get(i)); err != nil {
			return err
		}
	}
	for i := 0; i < fd.Extensions().Len(); i++ {
		if err := types.RegisterExtension(dynamicpb.NewExtensionType(fd.Extensions().Get(i))); err != nil {
			return fmt.Errorf("registering extension %q: %v", fd.Extensions().Get(i).FullName(), err)
		}
	}
	return nil
}

func registerMessageTypes(types *protoregistry.Types, md protoreflect.MessageDescriptor) error {
	if err := types.RegisterMessage(dynamicpb.NewMessageType(md)); err != nil {
		return fmt.Errorf("registering message %q: %v", md.FullName(), err)
	}
	for i := 0; i < md.Messages().Len(); i++ {
		if err := registerMessageTypes(types, md.Messages().Get(i)); err != nil {
			return err
		}
	}
	for i := 0; i < md.Extensions().Len(); i++ {
		if err := types.RegisterExtension(dynamicpb.NewExtensionType(md.Extensions().Get(i))); err != nil {
			return fmt.Errorf("registering extension %q: %v", md.Extensions().Get(i).FullName(), err)
		}
	}
	for i := 0; i < md.Enums().Len(); i++ {
		if err := types.RegisterEnum(dynamicpb.NewEnumType(md.Enums().Get(i))); err != nil {
			return fmt.Errorf("registering enum %q: %v", md.Enums().Get(i).FullName(), err)
		}
	}
	return nil
}

func NewServiceInfoProvider(files *protoregistry.Files, serviceNames []string) (reflection.ServiceInfoProvider, error) {
	serviceNameSet := make(map[string]struct{}, len(serviceNames))
	for _, name := range serviceNames {
		serviceNameSet[name] = struct{}{}
	}
	serviceNameToInfo := make(map[string]grpc.ServiceInfo, len(serviceNames))
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Services().Len(); i++ {
			sd := fd.Services().Get(i)
			fqn := string(sd.FullName())
			if _, ok := serviceNameSet[fqn]; !ok {
				continue
			}
			methods := make([]grpc.MethodInfo, 0, sd.Methods().Len())
			for j := 0; j < sd.Methods().Len(); j++ {
				md := sd.Methods().Get(j)
				methods = append(methods, grpc.MethodInfo{
					Name:           string(md.Name()),
					IsClientStream: md.IsStreamingClient(),
					IsServerStream: md.IsStreamingServer(),
				})
			}
			serviceNameToInfo[fqn] = grpc.ServiceInfo{Methods: methods}
			delete(serviceNameSet, fqn)
		}
		return true
	})
	if len(serviceNameSet) > 0 {
		missing := make([]string, 0, len(serviceNameSet))
		for name := range serviceNameSet {
			missing = append(missing, name)
		}
		return nil, fmt.Errorf("services not found in files: %v", missing)
	}
	return &serviceInfoProvider{serviceNameToInfo: serviceNameToInfo}, nil
}

type serviceInfoProvider struct {
	serviceNameToInfo map[string]grpc.ServiceInfo
}

func (p *serviceInfoProvider) GetServiceInfo() map[string]grpc.ServiceInfo {
	return p.serviceNameToInfo
}

type serverReflectionClientInProc struct {
	serverReflectionInfo func(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[grpc_reflection_v1.ServerReflectionRequest, grpc_reflection_v1.ServerReflectionResponse], error)
}

func NewServerReflectionClientInProc(reflectionServerOptions reflection.ServerOptions) grpc_reflection_v1.ServerReflectionClient {
	reflectionServer := reflection.NewServerV1(reflectionServerOptions)
	return &serverReflectionClientInProc{
		serverReflectionInfo: grpcinproc.NewBidiStreamAsClient[
			grpc_reflection_v1.ServerReflectionRequest,
			grpc_reflection_v1.ServerReflectionResponse,
			grpc_reflection_v1.ServerReflection_ServerReflectionInfoServer,
		](reflectionServer.ServerReflectionInfo),
	}
}

func (s *serverReflectionClientInProc) ServerReflectionInfo(
	ctx context.Context, opts ...grpc.CallOption,
) (grpc.BidiStreamingClient[grpc_reflection_v1.ServerReflectionRequest, grpc_reflection_v1.ServerReflectionResponse], error) {
	return s.serverReflectionInfo(ctx, opts...)
}

package grpc

import (
	"fmt"

	"github.com/malonaz/core/go/pbutil"

	grpcpb "github.com/malonaz/core/genproto/grpc/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func getMethodNameToGatewayOptions() (map[string]*grpcpb.GatewayOptions, error) {
	methodNameToGatewayOptions := map[string]*grpcpb.GatewayOptions{}
	var rangeFilesErr error

	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			methods := service.Methods()
			for j := 0; j < methods.Len(); j++ {
				method := methods.Get(j)
				gatewayOptions, err := pbutil.GetExtension[*grpcpb.GatewayOptions](method.Options(), grpcpb.E_GatewayOptions)
				if err != nil {
					if err == pbutil.ErrExtensionNotFound {
						continue
					}
					rangeFilesErr = fmt.Errorf("getting gateway options for %q: %w", method.FullName(), err)
					return false
				}

				methodName := fmt.Sprintf("/%s/%s", service.FullName(), method.Name())
				methodNameToGatewayOptions[methodName] = gatewayOptions
			}
		}
		return true
	})
	return methodNameToGatewayOptions, rangeFilesErr
}

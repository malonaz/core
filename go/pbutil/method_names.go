package pbutil

import (
	"fmt"

	"google.golang.org/grpc"
)

// MustFullMethodNames returns full method names (service.method) for the given method names
// from a gRPC service descriptor. Panics if any method name is not found, ensuring code
// doesn't drift from the service definition.
func MustFullMethodNames(serviceDesc grpc.ServiceDesc, methodNames ...string) []string {
	methodNameSet := make(map[string]struct{}, len(methodNames))
	for _, name := range methodNames {
		methodNameSet[name] = struct{}{}
	}
	fullMethodNames := make([]string, 0, len(methodNames))
	for _, method := range serviceDesc.Methods {
		if _, ok := methodNameSet[method.MethodName]; ok {
			fullMethodNames = append(fullMethodNames, serviceDesc.ServiceName+"."+method.MethodName)
			delete(methodNameSet, method.MethodName)
		}
	}
	if len(methodNameSet) > 0 {
		panic(fmt.Sprintf("methods not found in service %s: %v", serviceDesc.ServiceName, methodNameSet))
	}
	return fullMethodNames
}

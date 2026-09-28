package grpc

import (
	"fmt"
	"regexp"

	"github.com/malonaz/core/go/pbutil"

	grpcpb "github.com/malonaz/core/genproto/grpc/v1"
	"google.golang.org/genproto/googleapis/api/annotations"
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

// shorthandVariable matches a `{name}` path variable, which the gateway renders as `{name=*}`.
var shorthandVariable = regexp.MustCompile(`\{([^{}=]+)\}`)

// getRouteToCustomMime maps `METHOD /path/template` to the custom mime of every method that sets
// one. Templates are in the form [runtime.HTTPPathPattern] reports them, so a request's route can
// be looked up directly.
func getRouteToCustomMime() (map[string]string, error) {
	methodNameToGatewayOptions, err := getMethodNameToGatewayOptions()
	if err != nil {
		return nil, err
	}
	routeToCustomMime := map[string]string{}
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			methods := service.Methods()
			for j := 0; j < methods.Len(); j++ {
				method := methods.Get(j)
				methodName := fmt.Sprintf("/%s/%s", service.FullName(), method.Name())
				customMime := methodNameToGatewayOptions[methodName].GetCustomMime()
				if customMime == "" {
					continue
				}
				rule, err := pbutil.GetExtension[*annotations.HttpRule](method.Options(), annotations.E_Http)
				if err != nil {
					continue
				}
				for _, route := range httpRuleRoutes(rule) {
					routeToCustomMime[route] = customMime
				}
			}
		}
		return true
	})
	return routeToCustomMime, nil
}

// httpRuleRoutes returns `METHOD /path/template` for a rule and its additional bindings.
func httpRuleRoutes(rule *annotations.HttpRule) []string {
	var routes []string
	for _, r := range append([]*annotations.HttpRule{rule}, rule.GetAdditionalBindings()...) {
		var method, path string
		switch pattern := r.GetPattern().(type) {
		case *annotations.HttpRule_Get:
			method, path = "GET", pattern.Get
		case *annotations.HttpRule_Put:
			method, path = "PUT", pattern.Put
		case *annotations.HttpRule_Post:
			method, path = "POST", pattern.Post
		case *annotations.HttpRule_Delete:
			method, path = "DELETE", pattern.Delete
		case *annotations.HttpRule_Patch:
			method, path = "PATCH", pattern.Patch
		case *annotations.HttpRule_Custom:
			method, path = pattern.Custom.GetKind(), pattern.Custom.GetPath()
		default:
			continue
		}
		routes = append(routes, method+" "+shorthandVariable.ReplaceAllString(path, "{$1=*}"))
	}
	return routes
}

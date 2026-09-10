package manifest

import (
	"fmt"
	"os"
	"strings"

	"github.com/bufbuild/protocompile/ast"
	"github.com/bufbuild/protocompile/parser"
	"github.com/bufbuild/protocompile/reporter"
	"github.com/huandu/xstrings"
)

const (
	operationFullName = "google.longrunning.Operation"
	// The gateway codegen's proxy option: the internal method a gateway method forwards to.
	proxyOptionName = "(malonaz.codegen.gateway.v1.opts).proxy"
)

// GRPCService is a gRPC service resolved from the proto library declaring it.
type GRPCService struct {
	// Kebab-case name, as manifests spell it.
	Name string
	// The Go identifier of the service: `SchedulerService`.
	GoName string
	// The proto full name of the service: `malonaz.scheduler.scheduler_service.v1.SchedulerService`.
	FullName string
	// Go import path of the generated package.
	GoImportPath string
	// The scheduler methods the service's long-running methods (AIP-151) are run as, as job
	// method paths: the method itself, or the one it proxies to when it is a gateway method.
	LongrunningMethods []string
}

// ResolveGRPCService finds the service in the proto files of the label and the package they generate.
func ResolveGRPCService(proto Label, service, goImportPath string) (*GRPCService, error) {
	goName := xstrings.ToPascalCase(service)
	resolved := &GRPCService{Name: service, GoName: goName, GoImportPath: proto.GoImportPath(goImportPath)}
	var found bool
	for _, filename := range proto.Files {
		file, err := parseProto(filename)
		if err != nil {
			return nil, err
		}
		// A go_package option wins over the label-derived path.
		if goPackage, ok := stringOption(file, "go_package"); ok {
			resolved.GoImportPath, _, _ = strings.Cut(goPackage, ";")
		}
		for _, node := range file.Decls {
			declaration, ok := node.(*ast.ServiceNode)
			if !ok || declaration.Name.Val != goName {
				continue
			}
			found = true
			resolved.FullName = qualify(packageOf(file), goName)
			resolved.LongrunningMethods = append(resolved.LongrunningMethods, longrunningMethods(resolved.FullName, declaration)...)
		}
	}
	if !found {
		return nil, fmt.Errorf("service %q (%s) not found in %s", service, goName, proto.Target)
	}
	return resolved, nil
}

func parseProto(filename string) (*ast.FileNode, error) {
	reader, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	file, err := parser.Parse(filename, reader, reporter.NewHandler(nil))
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", filename, err)
	}
	return file, nil
}

func packageOf(file *ast.FileNode) string {
	for _, node := range file.Decls {
		if declaration, ok := node.(*ast.PackageNode); ok {
			return string(declaration.Name.AsIdentifier())
		}
	}
	return ""
}

// stringOption returns the file-level option's string value.
func stringOption(file *ast.FileNode, name string) (string, bool) {
	for _, node := range file.Decls {
		if option, ok := node.(*ast.OptionNode); ok && optionName(option) == name {
			return stringValue(option)
		}
	}
	return "", false
}

// longrunningMethods returns the job method paths of the service's methods returning an Operation.
func longrunningMethods(serviceFullName string, service *ast.ServiceNode) []string {
	var methods []string
	for _, node := range service.Decls {
		rpc, ok := node.(*ast.RPCNode)
		if !ok || !isOperation(rpc.Output.MessageType.AsIdentifier()) {
			continue
		}
		method := methodPath(serviceFullName, rpc.Name.Val)
		for _, node := range rpc.Decls {
			if option, ok := node.(*ast.OptionNode); ok && optionName(option) == proxyOptionName {
				if proxy, ok := stringValue(option); ok {
					// `pkg.Service.Method`, as the gateway codegen spells it.
					service, name := proxy[:strings.LastIndex(proxy, ".")], proxy[strings.LastIndex(proxy, ".")+1:]
					method = methodPath(service, name)
				}
			}
		}
		methods = append(methods, method)
	}
	return methods
}

func isOperation(messageType ast.Identifier) bool {
	return strings.TrimPrefix(string(messageType), ".") == operationFullName
}

func optionName(option *ast.OptionNode) string {
	parts := make([]string, len(option.Name.Parts))
	for i, part := range option.Name.Parts {
		parts[i] = part.Value()
	}
	return strings.Join(parts, ".")
}

func stringValue(option *ast.OptionNode) (string, bool) {
	value, ok := option.Val.(ast.StringValueNode)
	if !ok {
		return "", false
	}
	return value.AsString(), true
}

func qualify(pkg, name string) string {
	if pkg == "" {
		return name
	}
	return pkg + "." + name
}

func methodPath(serviceFullName, method string) string {
	return "/" + serviceFullName + "/" + method
}

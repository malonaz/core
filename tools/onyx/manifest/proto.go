package manifest

import (
	"fmt"
	"os"
	"regexp"

	"github.com/huandu/xstrings"
)

var (
	goPackageRegex = regexp.MustCompile(`option\s+go_package\s*=\s*"([^";]+)(?:;[^"]*)?";`)
	serviceRegex   = regexp.MustCompile(`service\s+(\w+)\s+{`)
)

// GRPCService is a gRPC service resolved from the proto library declaring it.
type GRPCService struct {
	// Kebab-case name, as manifests spell it.
	Name string
	// The Go identifier of the service: `SchedulerService`.
	GoName string
	// Go import path of the generated package.
	GoImportPath string
}

// ResolveGRPCService finds the service in the proto files of the label and the package they generate.
func ResolveGRPCService(proto Label, service, goImportPath string) (*GRPCService, error) {
	goName := xstrings.ToPascalCase(service)
	importPath := proto.GoImportPath(goImportPath)
	var found bool
	for _, filename := range proto.Files {
		bytes, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		// A go_package option wins over the label-derived path.
		if m := goPackageRegex.FindSubmatch(bytes); m != nil {
			importPath = string(m[1])
		}
		for _, m := range serviceRegex.FindAllSubmatch(bytes, -1) {
			if string(m[1]) == goName {
				found = true
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("service %q (%s) not found in %s", service, goName, proto.Target)
	}
	return &GRPCService{Name: service, GoName: goName, GoImportPath: importPath}, nil
}

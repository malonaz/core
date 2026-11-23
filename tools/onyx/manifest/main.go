package manifest

import (
	"fmt"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
)

// LoadMain loads a MainManifest, or with a label, the deployment of the ServiceManifest that label
// builds: its servers naming no service manifest serve that service.
func LoadMain(filename, label string) (*onyxpb.MainManifest, error) {
	if label == "" {
		m := &onyxpb.MainManifest{}
		if err := Load(filename, m); err != nil {
			return nil, err
		}
		return m, nil
	}
	service := &onyxpb.ServiceManifest{}
	if err := Load(filename, service); err != nil {
		return nil, err
	}
	m := service.GetDeployment()
	if m == nil {
		return nil, fmt.Errorf("%s: service %s has no deployment", filename, service.GetName())
	}
	// Before validating it: every server must name its services.
	self := label + "(" + filename + ")"
	for _, server := range m.GetServers() {
		switch kind := server.GetKind().(type) {
		case *onyxpb.Server_Grpc_:
			for _, grpcService := range kind.Grpc.GetServices() {
				if grpcService.GetManifest() == "" {
					grpcService.Manifest = self
				}
			}
		case *onyxpb.Server_Http_:
			if len(kind.Http.GetServices()) == 0 {
				kind.Http.Services = []*onyxpb.Server_Http_Service{{Manifest: self}}
			}
		case *onyxpb.Server_Processor_:
			if kind.Processor.GetManifest() == "" {
				kind.Processor.Manifest = self
			}
		}
	}
	if err := validate(filename, m); err != nil {
		return nil, err
	}
	return m, nil
}

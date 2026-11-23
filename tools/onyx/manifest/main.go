package manifest

import (
	"fmt"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
)

// LoadMain loads a MainManifest. With a label, filename is instead the ServiceManifest the label
// builds, and its deployment is loaded: a server naming no service manifest serves that service.
func LoadMain(filename, label string) (*onyxpb.MainManifest, error) {
	if label == "" {
		m := &onyxpb.MainManifest{}
		if err := Load(filename, m); err != nil {
			return nil, err
		}
		if err := defaultServices(m, ""); err != nil {
			return nil, fmt.Errorf("%s: %w", filename, err)
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
	if err := defaultServices(m, label+"("+filename+")"); err != nil {
		return nil, fmt.Errorf("%s: %w", filename, err)
	}
	return m, nil
}

// defaultServices sets every service manifest a server leaves empty to self, or rejects it when
// there is no self.
func defaultServices(m *onyxpb.MainManifest, self string) error {
	resolve := func(server *onyxpb.Server, manifest *string) error {
		if *manifest != "" {
			return nil
		}
		if self == "" {
			return fmt.Errorf("server %s names no service manifest", server.GetName())
		}
		*manifest = self
		return nil
	}
	for _, server := range m.GetServers() {
		switch kind := server.GetKind().(type) {
		case *onyxpb.Server_Grpc_:
			for _, service := range kind.Grpc.GetServices() {
				if err := resolve(server, &service.Manifest); err != nil {
					return err
				}
			}
		case *onyxpb.Server_Http_:
			if len(kind.Http.GetServices()) == 0 {
				kind.Http.Services = []*onyxpb.Server_Http_Service{{}}
			}
			for _, service := range kind.Http.GetServices() {
				if err := resolve(server, &service.Manifest); err != nil {
					return err
				}
			}
		case *onyxpb.Server_Processor_:
			if err := resolve(server, &kind.Processor.Manifest); err != nil {
				return err
			}
		}
	}
	return nil
}

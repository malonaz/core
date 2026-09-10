// Package binary generates the main package of a MainManifest: the flags, dependency wiring and
// server lifecycle of every service it hosts.
package binary

import (
	"fmt"
	"slices"
	"strings"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
	"github.com/malonaz/core/tools/onyx/gen"
	"github.com/malonaz/core/tools/onyx/manifest"
)

// Binary is a MainManifest resolved against the ServiceManifests it names.
type Binary struct {
	Name  string
	Setup bool
	// Root import path of the repository's Go packages.
	GoImportPath string
	// In start order: a server after every server it dials in-process.
	Servers []*Server
	// Distinct service manifests, in manifest order.
	Services []*Service
	// Distinct gRPC dependencies, in manifest order.
	GRPCClients []*GRPCClient
	// Distinct databases, in manifest order.
	Databases []*Database
	// Distinct generated stores, in manifest order.
	Stores []*Store
	// Distinct plain components, in manifest order.
	Components []*Component
	// Distinct interceptors, in manifest order.
	Interceptors []onyxpb.Interceptor
	Nats         bool
}

// Server is one server of the binary.
type Server struct {
	*onyxpb.Server
	// Services registered on it; a processor's is the one it runs.
	Services []*Service
	// gRPC registrations, for grpc servers.
	GRPC []*Registration
}

// Registration is a gRPC service registered on a grpc server.
type Registration struct {
	*onyxpb.Server_Grpc_Service
	Service *Service
	GRPC    *manifest.GRPCService
}

// Service is a distinct ServiceManifest of the binary.
type Service struct {
	*onyxpb.ServiceManifest
	// The manifest label, which identifies it.
	Label manifest.Label
	// The Go package implementing it.
	Target manifest.Label
	// Constructor arguments, in order.
	Dependencies []*Dependency
	// Servers registering it, in manifest order; the first one instantiates it.
	Servers []*Server
}

// Dependency is one resolved constructor argument.
type Dependency struct {
	GRPCClient *GRPCClient
	Store      *Store
	Database   *Database
	Component  *Component
	Nats       bool
}

// GRPCClient is a distinct gRPC dependency of the binary.
type GRPCClient struct {
	*manifest.GRPCService
	Proto manifest.Label
	key   string
	// The server of this binary serving it, else nil: the client is remote.
	Server *Server
	// The service of this binary implementing it, else nil.
	Service *Service
	// A server health-checks it.
	HealthChecked bool
}

// Database is a distinct postgres database.
type Database struct {
	Name string
}

// Store is a distinct generated store.
type Store struct {
	Name     string
	Target   manifest.Label
	Database *Database
}

// Component is a distinct plain Go component.
type Component struct {
	Name   string
	Target manifest.Label
}

// Load resolves the manifest and every manifest it references.
func Load(m *onyxpb.MainManifest, goImportPath string) (*Binary, error) {
	l := &loader{
		goImportPath: goImportPath,
		binary:       &Binary{Name: m.GetName(), Setup: m.GetSetup(), GoImportPath: goImportPath},
		services:     map[string]*Service{},
		grpcClients:  map[string]*GRPCClient{},
		databases:    map[string]*Database{},
		stores:       map[string]*Store{},
		components:   map[string]*Component{},
	}
	return l.load(m)
}

type loader struct {
	goImportPath string
	binary       *Binary
	services     map[string]*Service // by manifest label
	grpcClients  map[string]*GRPCClient
	databases    map[string]*Database
	stores       map[string]*Store
	components   map[string]*Component
}

func (l *loader) load(m *onyxpb.MainManifest) (*Binary, error) {
	// Which server serves which gRPC service: the first one wins.
	served := map[string]*Server{}
	var servers []*Server
	for _, s := range m.GetServers() {
		server := &Server{Server: s}
		servers = append(servers, server)
		if grpc := s.GetGrpc(); grpc != nil {
			for _, interceptor := range grpc.GetInterceptors() {
				if !slices.Contains(l.binary.Interceptors, interceptor) {
					l.binary.Interceptors = append(l.binary.Interceptors, interceptor)
				}
			}
			for _, svc := range grpc.GetServices() {
				key := grpcKey(svc.GetProto(), svc.GetService())
				if _, ok := served[key]; !ok {
					served[key] = server
				}
			}
		}
	}

	for _, server := range servers {
		var manifests []struct {
			label string
			grpc  *onyxpb.Server_Grpc_Service
		}
		switch kind := server.GetKind().(type) {
		case *onyxpb.Server_Grpc_:
			for _, svc := range kind.Grpc.GetServices() {
				manifests = append(manifests, struct {
					label string
					grpc  *onyxpb.Server_Grpc_Service
				}{svc.GetManifest(), svc})
			}
		case *onyxpb.Server_Http_:
			for _, svc := range kind.Http.GetServices() {
				manifests = append(manifests, struct {
					label string
					grpc  *onyxpb.Server_Grpc_Service
				}{svc.GetManifest(), nil})
			}
		case *onyxpb.Server_Processor_:
			manifests = append(manifests, struct {
				label string
				grpc  *onyxpb.Server_Grpc_Service
			}{kind.Processor.GetManifest(), nil})
		case *onyxpb.Server_GrpcWebProxy_:
		default:
			return nil, fmt.Errorf("server %s: unknown kind %T", server.GetName(), kind)
		}
		for _, entry := range manifests {
			service, err := l.service(entry.label, served)
			if err != nil {
				return nil, fmt.Errorf("server %s: %w", server.GetName(), err)
			}
			if !slices.Contains(server.Services, service) {
				server.Services = append(server.Services, service)
				service.Servers = append(service.Servers, server)
			}
			if entry.grpc != nil {
				proto, err := manifest.ParseLabel(entry.grpc.GetProto())
				if err != nil {
					return nil, err
				}
				grpc, err := manifest.ResolveGRPCService(proto, entry.grpc.GetService(), l.goImportPath)
				if err != nil {
					return nil, fmt.Errorf("server %s: %w", server.GetName(), err)
				}
				server.GRPC = append(server.GRPC, &Registration{Server_Grpc_Service: entry.grpc, Service: service, GRPC: grpc})
			}
		}
	}

	// A gRPC dependency served in this binary is implemented by the service registering it.
	for _, client := range l.binary.GRPCClients {
		if client.Server == nil {
			continue
		}
		for _, registration := range client.Server.GRPC {
			if grpcKey(registration.GetProto(), registration.GetService()) == client.key {
				client.Service = registration.Service
			}
		}
	}
	for _, service := range l.binary.Services {
		for _, dep := range service.Dependencies {
			// A service's health does not gate on itself.
			if dep.GRPCClient != nil && dep.GRPCClient.Service != service {
				dep.GRPCClient.HealthChecked = true
			}
		}
	}

	if err := l.checkServiceCycles(); err != nil {
		return nil, err
	}
	ordered, err := orderServers(servers)
	if err != nil {
		return nil, err
	}
	l.binary.Servers = ordered
	if err := l.checkIdentifiers(); err != nil {
		return nil, err
	}
	return l.binary, nil
}

func grpcKey(proto, service string) string {
	label, err := manifest.ParseLabel(proto)
	if err != nil {
		return proto + ":" + service
	}
	return label.Target + ":" + service
}

func (l *loader) service(label string, served map[string]*Server) (*Service, error) {
	if service, ok := l.services[label]; ok {
		return service, nil
	}
	parsed, err := manifest.ParseLabel(label)
	if err != nil {
		return nil, err
	}
	m := &onyxpb.ServiceManifest{}
	if err := manifest.LoadLabel(label, m); err != nil {
		return nil, err
	}
	for _, other := range l.binary.Services {
		if other.GetName() == m.GetName() {
			return nil, fmt.Errorf("service %q is defined by both %s and %s", m.GetName(), other.Label.Target, parsed.Target)
		}
	}
	target, err := manifest.ParseLabel(m.GetTarget())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", parsed.Target, err)
	}
	service := &Service{ServiceManifest: m, Label: parsed, Target: target}
	for _, dep := range m.GetDependencies() {
		resolved, err := l.dependency(m, dep, served)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", parsed.Target, err)
		}
		service.Dependencies = append(service.Dependencies, resolved)
	}
	l.services[label] = service
	l.binary.Services = append(l.binary.Services, service)
	return service, nil
}

func (l *loader) dependency(m *onyxpb.ServiceManifest, dep *onyxpb.Dependency, served map[string]*Server) (*Dependency, error) {
	switch kind := dep.GetKind().(type) {
	case *onyxpb.Dependency_GrpcClient_:
		key := grpcKey(kind.GrpcClient.GetProto(), kind.GrpcClient.GetService())
		client, ok := l.grpcClients[key]
		if !ok {
			proto, err := manifest.ParseLabel(kind.GrpcClient.GetProto())
			if err != nil {
				return nil, err
			}
			grpc, err := manifest.ResolveGRPCService(proto, kind.GrpcClient.GetService(), l.goImportPath)
			if err != nil {
				return nil, err
			}
			client = &GRPCClient{GRPCService: grpc, Proto: proto, key: key, Server: served[key]}
			l.grpcClients[key] = client
			l.binary.GRPCClients = append(l.binary.GRPCClients, client)
		}
		return &Dependency{GRPCClient: client}, nil

	case *onyxpb.Dependency_PostgresDbClient_:
		database := l.database(orDefault(kind.PostgresDbClient.GetDatabase(), kind.PostgresDbClient.GetName()))
		target, err := manifest.ParseLabel(kind.PostgresDbClient.GetTarget())
		if err != nil {
			return nil, err
		}
		store, ok := l.stores[target.Target]
		if !ok {
			store = &Store{Name: kind.PostgresDbClient.GetName(), Target: target, Database: database}
			l.stores[target.Target] = store
			l.binary.Stores = append(l.binary.Stores, store)
		}
		if store.Database != database {
			return nil, fmt.Errorf("store %s is declared over both databases %s and %s", target.Target, store.Database.Name, database.Name)
		}
		return &Dependency{Store: store}, nil

	case *onyxpb.Dependency_PostgresClient_:
		database := l.database(orDefault(kind.PostgresClient.GetDatabase(), kind.PostgresClient.GetName()))
		return &Dependency{Database: database}, nil

	case *onyxpb.Dependency_Nats_:
		l.binary.Nats = true
		return &Dependency{Nats: true}, nil

	case *onyxpb.Dependency_Service_:
		target, err := manifest.ParseLabel(kind.Service.GetTarget())
		if err != nil {
			return nil, err
		}
		component, ok := l.components[target.Target]
		if !ok {
			component = &Component{Name: kind.Service.GetName(), Target: target}
			l.components[target.Target] = component
			l.binary.Components = append(l.binary.Components, component)
		}
		if component.Name != kind.Service.GetName() {
			return nil, fmt.Errorf("component %s is named both %q and %q", target.Target, component.Name, kind.Service.GetName())
		}
		return &Dependency{Component: component}, nil

	default:
		return nil, fmt.Errorf("unknown dependency kind %T", kind)
	}
}

func (l *loader) database(name string) *Database {
	database, ok := l.databases[name]
	if !ok {
		database = &Database{Name: name}
		l.databases[name] = database
		l.binary.Databases = append(l.binary.Databases, database)
	}
	return database
}

func orDefault(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

// checkServiceCycles rejects services that dial each other, transitively, within this binary: no
// start order satisfies both.
func (l *loader) checkServiceCycles() error {
	edges := map[*Service][]*Service{}
	for _, service := range l.binary.Services {
		for _, dep := range service.Dependencies {
			if dep.GRPCClient != nil && dep.GRPCClient.Service != nil && dep.GRPCClient.Service != service {
				edges[service] = append(edges[service], dep.GRPCClient.Service)
			}
		}
	}
	cycle := findCycle(l.binary.Services, edges)
	if cycle == nil {
		return nil
	}
	names := make([]string, len(cycle))
	for i, service := range cycle {
		names[i] = service.GetName()
	}
	return fmt.Errorf("services depend on each other: %s", strings.Join(names, " -> "))
}

// orderServers sorts servers so each comes after the servers its services dial in-process,
// preserving manifest order otherwise.
func orderServers(servers []*Server) ([]*Server, error) {
	edges := map[*Server][]*Server{}
	for _, server := range servers {
		for _, service := range server.Services {
			for _, dep := range service.Dependencies {
				if dep.GRPCClient == nil || dep.GRPCClient.Server == nil || dep.GRPCClient.Server == server {
					continue
				}
				if !slices.Contains(edges[server], dep.GRPCClient.Server) {
					edges[server] = append(edges[server], dep.GRPCClient.Server)
				}
			}
		}
	}
	if cycle := findCycle(servers, edges); cycle != nil {
		names := make([]string, len(cycle))
		for i, server := range cycle {
			names[i] = server.GetName()
		}
		return nil, fmt.Errorf("servers depend on each other: %s; host the services in one server or split them differently", strings.Join(names, " -> "))
	}
	var ordered []*Server
	done := map[*Server]bool{}
	var visit func(*Server)
	visit = func(server *Server) {
		if done[server] {
			return
		}
		done[server] = true
		for _, dep := range edges[server] {
			visit(dep)
		}
		ordered = append(ordered, server)
	}
	for _, server := range servers {
		visit(server)
	}
	return ordered, nil
}

// findCycle returns a cycle of the graph as a path ending where it starts, or nil.
func findCycle[T comparable](nodes []T, edges map[T][]T) []T {
	const (
		visiting = 1
		done     = 2
	)
	state := map[T]int{}
	var path []T
	var visit func(T) []T
	visit = func(node T) []T {
		switch state[node] {
		case done:
			return nil
		case visiting:
			return append(path[slices.Index(path, node):], node)
		}
		state[node] = visiting
		path = append(path, node)
		for _, next := range edges[node] {
			if cycle := visit(next); cycle != nil {
				return cycle
			}
		}
		path = path[:len(path)-1]
		state[node] = done
		return nil
	}
	for _, node := range nodes {
		if cycle := visit(node); cycle != nil {
			return cycle
		}
	}
	return nil
}

// checkIdentifiers rejects manifests whose generated identifiers collide.
func (l *loader) checkIdentifiers() error {
	owners := map[string]string{}
	claim := func(identifier, owner string) error {
		if existing, ok := owners[identifier]; ok && existing != owner {
			return fmt.Errorf("%s and %s both generate %s", existing, owner, identifier)
		}
		owners[identifier] = owner
		return nil
	}
	for _, service := range l.binary.Services {
		if err := claim(gen.Camel(service.GetName()), "service "+service.GetName()); err != nil {
			return err
		}
		if err := claim("opts."+gen.Pascal(service.GetName()), "service "+service.GetName()); err != nil {
			return err
		}
	}
	for _, client := range l.binary.GRPCClients {
		if err := claim(gen.Camel(client.Name)+"Client", "gRPC client "+client.Name); err != nil {
			return err
		}
		if client.Server == nil {
			if err := claim("opts."+client.GoName+"GRPC", "gRPC client "+client.Name); err != nil {
				return err
			}
		}
	}
	for _, store := range l.binary.Stores {
		if err := claim(gen.Camel(store.Name)+"Store", "store "+store.Target.Target); err != nil {
			return err
		}
	}
	for _, component := range l.binary.Components {
		if err := claim(gen.Camel(component.Name)+"Service", "component "+component.Name); err != nil {
			return err
		}
		if err := claim("opts."+gen.Pascal(component.Name)+"Service", "component "+component.Name); err != nil {
			return err
		}
	}
	for _, server := range l.binary.Servers {
		owner := "server " + server.GetName()
		var identifiers []string
		switch server.GetKind().(type) {
		case *onyxpb.Server_Grpc_:
			identifiers = []string{"opts." + gen.Pascal(server.GetName()) + "GRPC", gen.Camel(server.GetName()) + "GRPCServer"}
		case *onyxpb.Server_Http_:
			identifiers = []string{"opts." + gen.Pascal(server.GetName()) + "HTTP", gen.Camel(server.GetName()) + "HTTPServer"}
		case *onyxpb.Server_GrpcWebProxy_:
			identifiers = []string{"opts." + gen.Pascal(server.GetName()) + "GRPCWebProxy", gen.Camel(server.GetName()) + "GRPCWebProxy"}
		}
		for _, identifier := range identifiers {
			if err := claim(identifier, owner); err != nil {
				return err
			}
		}
	}
	return nil
}

package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/huandu/xstrings"
	"gopkg.in/yaml.v3"
)

// serverConfig and serverService are the typed view of a main manifest's servers. The template
// engine hands serviceGraph the untyped map yaml.Unmarshal produced, which decodeServers turns
// into these before any of the graph logic runs.
type serverConfig struct {
	Type     string          `yaml:"type"`
	Name     string          `yaml:"name"`
	Manifest string          `yaml:"manifest"`
	Services []serverService `yaml:"services"`
}

type serverService struct {
	Service  string `yaml:"service"`
	Proto    string `yaml:"proto"`
	Manifest string `yaml:"manifest"`
}

// serviceManifest and serviceDependency are the typed view of one service's manifest, holding
// only the fields the graph walks.
type serviceManifest struct {
	Name         string              `yaml:"name"`
	Dependencies []serviceDependency `yaml:"dependencies"`
}

type serviceDependency struct {
	Type           string `yaml:"type"`
	Name           string `yaml:"name"`
	Service        string `yaml:"service"`
	Manifest       string `yaml:"manifest"`
	Implementation string `yaml:"implementation"`
}

// parsedManifest is one service manifest in both the forms the graph needs: raw as the template
// consumes it, keys the graph does not model included, and typed as the graph walks it. The
// graph writes what it works out — a dependency's name, the level, the servers — back into the
// raw map, which is what the template ultimately ranges over.
type parsedManifest struct {
	raw   map[string]any
	typed *serviceManifest
}

var targetToServiceManifest = map[string]*parsedManifest{}

// parseServiceManifest reads the manifest at target into both of its views. Manifests are
// cached by target: a service reached from several places is one parse and, more importantly,
// one raw map, so that what the graph writes into it is seen everywhere it is referenced.
func parseServiceManifest(target string) (*parsedManifest, error) {
	if manifest, ok := targetToServiceManifest[target]; ok {
		return manifest, nil
	}
	_, filenames, err := parseTarget(target)
	if err != nil {
		return nil, err
	}
	if len(filenames) != 1 {
		return nil, fmt.Errorf("expected 1 filename, got %d [%s]", len(filenames), filenames)
	}
	content, err := readFile(filenames[0])
	if err != nil {
		return nil, err
	}
	manifest := &parsedManifest{raw: map[string]any{}, typed: &serviceManifest{}}
	if err := yaml.Unmarshal(content, &manifest.raw); err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(content, manifest.typed); err != nil {
		return nil, err
	}
	targetToServiceManifest[target] = manifest
	return manifest, nil
}

// decodeServers converts the servers the template engine passes in, which reach us as the
// `map[string]any` tree yaml.Unmarshal built, into the typed view above.
func decodeServers(servers []any) ([]serverConfig, error) {
	encoded, err := yaml.Marshal(servers)
	if err != nil {
		return nil, fmt.Errorf("re-encoding servers: %w", err)
	}
	var result []serverConfig
	if err := yaml.Unmarshal(encoded, &result); err != nil {
		return nil, fmt.Errorf("decoding servers: %w", err)
	}
	return result, nil
}

// addUnique appends value unless it is already there. The graph's edge and server lists want
// both properties: declaration order, so the generated code does not churn between builds, and
// deduplication, so a service declared twice is one edge.
func addUnique(values []string, value string) []string {
	if slices.Contains(values, value) {
		return values
	}
	return append(values, value)
}

// serviceNode is one service in the dependency graph assembled by serviceGraph.
type serviceNode struct {
	manifest *parsedManifest
	// deps holds the services this one declares as `type: service` dependencies, which are
	// construction edges: the service is an argument to this one's constructor. callDeps holds
	// the services in this binary serving its `grpc_client` dependencies, which are call edges:
	// they order startup but not construction, since the caller holds a client.
	deps     []string
	callDeps []string
	// servers holds the servers whose health checks cover this service: the ones hosting it,
	// plus, for a service pulled in as a dependency, the ones hosting whatever led here.
	servers   []string
	level     int
	resolving bool
}

func (n *serviceNode) name() string { return n.manifest.typed.Name }

// graphBuilder assembles the dependency graph. Nodes are keyed by service name and kept in the
// order the manifests declared them, so that the generated code does not churn between builds.
type graphBuilder struct {
	servers    []serverConfig
	nameToNode map[string]*serviceNode
	nodes      []*serviceNode
}

// node returns the graph's node for a service, adding it if this is the first sight of it.
func (b *graphBuilder) node(manifest *parsedManifest) *serviceNode {
	if node, ok := b.nameToNode[manifest.typed.Name]; ok {
		return node
	}
	node := &serviceNode{manifest: manifest, level: -1}
	b.nameToNode[manifest.typed.Name] = node
	b.nodes = append(b.nodes, node)
	return node
}

// collect registers the service at target and everything it depends on, returning its name.
// Cycles are left to assignLevels, which can report the path that forms them.
func (b *graphBuilder) collect(target string, visited map[string]bool) (string, error) {
	manifest, err := parseServiceManifest(target)
	if err != nil {
		return "", fmt.Errorf("reading service manifest %s (is it in the deps of the manifest rule referencing it?): %w", target, err)
	}
	if manifest.typed.Name == "" {
		return "", fmt.Errorf("service manifest %s declares no name", target)
	}
	node := b.node(manifest)
	if visited[node.name()] {
		return node.name(), nil
	}
	visited[node.name()] = true

	rawDependencies, _ := manifest.raw["dependencies"].([]any)
	for i, dependency := range manifest.typed.Dependencies {
		if dependency.Type != "service" {
			continue
		}
		name, err := b.collectDependency(dependency, visited)
		if err != nil {
			return "", fmt.Errorf("service %s: %w", node.name(), err)
		}
		node.deps = addUnique(node.deps, name)
		// The template instantiates a service dependency by name, which only the graph knows:
		// the manifest points at the depended-on service's own manifest, not at its name.
		if i < len(rawDependencies) {
			if raw, ok := rawDependencies[i].(map[string]any); ok {
				raw["name"] = name
			}
		}
	}
	return node.name(), nil
}

// collectDependency resolves a single `type: service` dependency to the name of the service it
// points at, registering that service in the graph.
func (b *graphBuilder) collectDependency(dependency serviceDependency, visited map[string]bool) (string, error) {
	if dependency.Manifest != "" {
		return b.collect(dependency.Manifest, visited)
	}
	// A dependency given as a bare implementation has no manifest to read, so it is a leaf: it
	// takes no dependencies of its own and can always start in the first level.
	if dependency.Name == "" {
		return "", fmt.Errorf("service dependency needs a manifest or a name")
	}
	b.node(&parsedManifest{
		raw:   map[string]any{"name": dependency.Name, "implementation": dependency.Implementation},
		typed: &serviceManifest{Name: dependency.Name},
	})
	return dependency.Name, nil
}

// collectCallEdges resolves every `grpc_client` dependency to the service in this binary that
// serves it. A dependency served from outside the binary is nothing this binary can order, so
// it is dropped.
func (b *graphBuilder) collectCallEdges() error {
	servedByName, err := b.servedServices()
	if err != nil {
		return err
	}
	for _, node := range b.nodes {
		for _, dependency := range node.manifest.typed.Dependencies {
			if dependency.Type != "grpc_client" {
				continue
			}
			name, ok := servedByName[xstrings.ToPascalCase(dependency.Service)]
			if !ok || name == node.name() {
				continue
			}
			node.callDeps = addUnique(node.callDeps, name)
		}
	}
	return nil
}

// servedServices maps each proto service a server hosts, PascalCased as `grpc_client`
// dependencies name it, to the name of the service manifest serving it. One manifest can serve
// several proto services, which is why the mapping runs this way round.
func (b *graphBuilder) servedServices() (map[string]string, error) {
	result := map[string]string{}
	for _, server := range b.servers {
		for _, service := range server.Services {
			if service.Service == "" || service.Manifest == "" {
				continue
			}
			manifest, err := parseServiceManifest(service.Manifest)
			if err != nil {
				return nil, fmt.Errorf("reading service manifest %s: %w", service.Manifest, err)
			}
			result[xstrings.ToPascalCase(service.Service)] = manifest.typed.Name
		}
	}
	return result, nil
}

// assignLevels gives each service the level after the deepest service it depends on, and is
// where cycles are resolved. resolving marks the services on the path currently being walked,
// so an edge to one of them is an edge back up the path we came down — a cycle rather than a
// diamond. What that means depends on the edge:
//
// A construction edge cannot be honored in either direction, since one service is an argument
// to the other's constructor, so a cycle in those is an error and names the path.
//
// A call edge is only an ordering, and two services that call each other are a legitimate
// arrangement: both are up before either serves traffic. One of them has to start first, so the
// edge that says otherwise is dropped and the walk carries on.
func (b *graphBuilder) assignLevels(name string, path []string) (int, error) {
	node := b.nameToNode[name]
	if node.level >= 0 {
		return node.level, nil
	}
	node.resolving = true
	defer func() { node.resolving = false }()

	level := 0
	// Cloned, so that the two loops below cannot append into each other's backing array and
	// garble the path a cycle error reports.
	path = append(slices.Clone(path), name)
	deepen := func(dep string) error {
		depLevel, err := b.assignLevels(dep, path)
		if err != nil {
			return err
		}
		if depLevel+1 > level {
			level = depLevel + 1
		}
		return nil
	}
	for _, dep := range node.deps {
		if b.nameToNode[dep].resolving {
			return 0, fmt.Errorf("service dependency cycle: %s", strings.Join(append(slices.Clone(path), dep), " -> "))
		}
		if err := deepen(dep); err != nil {
			return 0, err
		}
	}
	for _, dep := range node.callDeps {
		if b.nameToNode[dep].resolving {
			continue // Closes a cycle: this service starts before the one it calls.
		}
		if err := deepen(dep); err != nil {
			return 0, err
		}
	}
	node.level = level
	return level, nil
}

// serviceGraph resolves the services a main manifest's servers host, and everything those
// services depend on, into dependency-ordered levels. Level 0 holds the services that depend
// on no other service; a service in level N depends only on services in earlier levels, so a
// binary can bring up one level at a time and know that everything a service needs is already
// started. Returns an error on a cycle in the construction dependencies.
//
// Each returned service is its manifest, with `type: service` dependencies resolved so they
// carry the depended-on service's name, plus two added keys: `level` and `servers`.
func serviceGraph(rawServers []any) ([][]map[string]any, error) {
	servers, err := decodeServers(rawServers)
	if err != nil {
		return nil, err
	}
	builder := &graphBuilder{servers: servers, nameToNode: map[string]*serviceNode{}}

	// Collect every service the servers host, noting which server hosts it, and everything
	// those services are built from.
	visited := map[string]bool{}
	for _, server := range servers {
		for _, target := range serverManifestTargets(server) {
			name, err := builder.collect(target, visited)
			if err != nil {
				return nil, err
			}
			node := builder.nameToNode[name]
			node.servers = addUnique(node.servers, server.Name)
		}
	}
	if err := builder.collectCallEdges(); err != nil {
		return nil, err
	}

	maxLevel := 0
	for _, node := range builder.nodes {
		level, err := builder.assignLevels(node.name(), nil)
		if err != nil {
			return nil, err
		}
		if level > maxLevel {
			maxLevel = level
		}
	}

	// Group by level, keeping services in the order the manifests declared them.
	levels := make([][]*serviceNode, maxLevel+1)
	for _, node := range builder.nodes {
		levels[node.level] = append(levels[node.level], node)
	}

	// Hand each service's servers down to whatever it is built from, so that a dependency's
	// health checks are reported by every server that ultimately relies on it. A construction
	// dependency always sits at a lower level than the service holding it, so walking down
	// from the top propagates in a single pass.
	for level := len(levels) - 1; level >= 0; level-- {
		for _, node := range levels[level] {
			for _, dep := range node.deps {
				for _, name := range node.servers {
					dependency := builder.nameToNode[dep]
					dependency.servers = addUnique(dependency.servers, name)
				}
			}
		}
	}

	result := make([][]map[string]any, len(levels))
	for i, level := range levels {
		for _, node := range level {
			node.manifest.raw["level"] = node.level
			node.manifest.raw["servers"] = node.servers
			result[i] = append(result[i], node.manifest.raw)
		}
	}
	return result, nil
}

// serverManifestTargets returns the manifest target of every service one server hosts, in
// declaration order. A processor carries its service on the server itself rather than in a
// services list.
func serverManifestTargets(server serverConfig) []string {
	var targets []string
	for _, service := range server.Services {
		if service.Manifest != "" {
			targets = append(targets, service.Manifest)
		}
	}
	if server.Manifest != "" {
		targets = append(targets, server.Manifest)
	}
	return targets
}

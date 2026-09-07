package main

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"
	"text/template"

	"github.com/huandu/xstrings"
	"gopkg.in/yaml.v3"
)

var (
	protoGoPackageRegex     = regexp.MustCompile(`option\s+go_package\s*=\s*"([^";]+)(?:;[^"]*)?";`)
	pleaseFilenamesRegex    = regexp.MustCompile(`\(([^)]+)\)`)
	serviceRegex            = regexp.MustCompile(`service\s+([\w]+)\s+{`)
	publisherRegex          = regexp.MustCompile(`require_nats_publishers:\s*\[([\s\S]*?)\]`)
	goPackageRegex          = regexp.MustCompile(`(?m)^package\s+\w+\s*\n`)
	doOnceCache             = map[string]bool{}
	targetToServiceManifest = map[string]*parsedManifest{}
	filepathToContent       = map[string][]byte{}
	keyToGrpcServiceName    = map[string]string{}
	goImportPathToAlias     = map[string]string{}
	aliasToGoImportPath     = map[string]string{}
	customFuncMap           = template.FuncMap{
		"debug": func(v any) error {
			fmt.Printf("%+v\n", v)
			return nil
		},

		"doOnce": func(key string) bool {
			if _, ok := doOnceCache[key]; ok {
				return false
			}
			doOnceCache[key] = true
			return true
		},

		"parseYaml":    parseYaml,
		"parseGRPC":    parseGRPC,
		"serviceGraph": serviceGraph,

		"plzGoImport":      plzGoImport,
		"plzGoImportAlias": plzGoImportAlias,
		"goImport":         goImport,
		"goImportAlias":    goImportAlias,

		"grpcNatsPublishers": func(filepath string) ([]string, error) {
			sanitizedFilepath := strings.TrimPrefix(filepath, "//")
			sanitizedFilepath = strings.ReplaceAll(sanitizedFilepath, ":", "/")
			content, err := readFile(sanitizedFilepath + ".proto")
			if err != nil {
				return nil, err
			}

			// Find all matches
			matches := publisherRegex.FindStringSubmatch(string(content))
			if len(matches) != 2 {
				return []string{}, nil // No publishers found
			}

			// Split the publishers string and clean up each publisher
			publishers := strings.Split(matches[1], ",")
			var result []string
			for _, publisher := range publishers {
				// Clean up the string (remove quotes and whitespace)
				publisher = strings.TrimSpace(publisher)
				publisher = strings.Trim(publisher, `"'`)
				if publisher != "" {
					result = append(result, publisher)
				}
			}
			return result, nil
		},
	}
)

// findAvailableAlias finds an available alias for the given requested alias and import path
func findAvailableAlias(requestedAlias, importPath string) string {
	// First, check if the requested alias is available
	if existingImportPath, exists := aliasToGoImportPath[requestedAlias]; !exists || existingImportPath == importPath {
		return requestedAlias
	}

	// If there's a conflict, try adding numbers
	counter := 2
	for {
		candidateAlias := fmt.Sprintf("%s%d", requestedAlias, counter)
		if existingImportPath, exists := aliasToGoImportPath[candidateAlias]; !exists || existingImportPath == importPath {
			return candidateAlias
		}
		counter++

		// Safety check to avoid infinite loop (though this should never happen in practice)
		if counter > 1000 {
			// Fallback to a unique alias based on the full import path
			return strings.ReplaceAll(strings.ReplaceAll(importPath, "/", "_"), ".", "_")
		}
	}
}

func readFile(filepath string) ([]byte, error) {
	if content, ok := filepathToContent[filepath]; ok {
		return content, nil
	}
	bytes, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	filepathToContent[filepath] = bytes
	return bytes, nil
}

func goImport(importPath string) (string, error) {
	return goImportAlias(importPath, "")
}

func goImportAlias(importPath, alias string) (string, error) {
	if alias == "" {
		alias = path.Base(importPath)
	}

	// Check if we've already processed this import path
	if existingAlias, exists := goImportPathToAlias[importPath]; exists {
		return existingAlias, nil
	}

	// Find an available alias
	finalAlias := findAvailableAlias(alias, importPath)

	// Store the mapping both ways
	goImportPathToAlias[importPath] = finalAlias
	aliasToGoImportPath[finalAlias] = importPath

	return finalAlias, nil
}

func plzGoImport(labelOrTarget string) (string, error) {
	return plzGoImportAlias(labelOrTarget, "")
}

// Imports plz go labels like "//user/proto:api".
func plzGoImportAlias(labelOrTarget, alias string) (string, error) {
	label := labelOrTarget
	if strings.Contains(labelOrTarget, "(") {
		parsedLabel, filenames, err := parseTarget(labelOrTarget)
		if err != nil {
			return "", err
		}
		label = parsedLabel

		// If the import is a protofile, we check if it defines a 'go_package' and honor it.
		for _, filename := range filenames {
			if strings.Contains(filename, ".proto") {
				// Check if the proto file declares a 'option go_package'!
				bytes, err := readFile(filename)
				if err != nil {
					return "", fmt.Errorf("reading file %s", filename)
				}
				matches := protoGoPackageRegex.FindSubmatch(bytes)
				if len(matches) >= 2 {
					// Extract the import path (everything before the optional semicolon)
					importPath := string(matches[1])
					return goImportAlias(importPath, alias)
				}
			}
		}
	}

	importPath, _, err := parseLabel(label)
	if err != nil {
		return "", err
	}
	return goImportAlias(importPath, alias)
}

func injectGoImports(content []byte) []byte {
	if len(goImportPathToAlias) == 0 {
		return content // No imports to inject
	}

	// Generate imports
	var imports []string
	for importPath, alias := range goImportPathToAlias {
		baseName := path.Base(importPath)
		if alias == baseName {
			// No alias needed, use standard import
			imports = append(imports, fmt.Sprintf("\t\"%s\"", importPath))
		} else {
			// Use alias
			imports = append(imports, fmt.Sprintf("\t%s \"%s\"", alias, importPath))
		}
	}

	// Create import block (always use import () notation)
	importBlock := fmt.Sprintf("import (\n%s\n)\n\n", strings.Join(imports, "\n"))

	// Find the package declaration
	packageEnd := goPackageRegex.FindIndex(content)
	if packageEnd == nil {
		return content // No package declaration found
	}

	// Insert the import block after the package declaration
	importBytes := []byte(importBlock)
	result := make([]byte, 0, len(content)+len(importBytes))
	result = append(result, content[:packageEnd[1]]...) // Everything up to end of package line
	result = append(result, importBytes...)             // Import block
	result = append(result, content[packageEnd[1]:]...) // Rest of the content

	return result
}

func parseYaml(target string) (map[string]any, error) {
	_, filenames, err := parseTarget(target)
	if err != nil {
		return nil, err
	}
	if len(filenames) != 1 {
		return nil, fmt.Errorf("expected 1 filename, got %d [%s]", len(filenames), filenames)
	}
	filename := filenames[0]

	bytes, err := readFile(filename)
	if err != nil {
		return nil, err
	}
	data := map[string]any{}
	if err := yaml.Unmarshal(bytes, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func parseLabel(label string) (string, string, error) {
	if !strings.HasPrefix(label, "//") {
		return "", "", fmt.Errorf("non-cannonical label %s", label)
	}
	importPath := strings.TrimPrefix(label, "//")
	packageName := path.Base(importPath)
	// Handle canonical labels.
	if strings.Contains(importPath, ":") {
		parts := strings.Split(importPath, ":")
		if len(parts) != 2 {
			return "", "", fmt.Errorf("non-cannonical label %s", label)
		}
		importPath = parts[0]
		packageName = parts[1]
		if packageName != path.Base(importPath) { // Handles '//user/v1:diff_name'
			importPath = importPath + "/" + packageName
		}
	}

	if strings.HasPrefix(importPath, "third_party/go") {
		importPath = strings.TrimPrefix(importPath, "third_party/go/")
		importPath = strings.ReplaceAll(importPath, "__", "/")
	} else {
		if opts.GoImportPath != "" {
			importPath = opts.GoImportPath + "/" + importPath
		}
	}
	return importPath, packageName, nil
}

// parseTarget extracts the label and filenames from a target
// For example: "//path:target(hello.proto world.proto)" returns:
// - label: "//path:target"
// - filenames: ["hello.proto", "world.proto"]
// - error: nil
func parseTarget(target string) (string, []string, error) {
	// Find the opening parenthesis
	parenIndex := strings.Index(target, "(")
	if parenIndex == -1 {
		return "", nil, fmt.Errorf("invalid target %s", target)
	}

	// Extract the base label (everything before the parenthesis)
	label := target[:parenIndex]

	// Extract filenames using the existing regex
	matches := pleaseFilenamesRegex.FindStringSubmatch(target)
	if len(matches) < 2 {
		return "", nil, fmt.Errorf("invalid target format: could not extract filenames from parentheses")
	}

	// Split by whitespace and filter out empty strings
	filenames := strings.Fields(matches[1])
	if len(filenames) == 0 {
		return "", nil, fmt.Errorf("no filenames found in parentheses")
	}

	return label, filenames, nil
}

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

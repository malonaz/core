package main

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
	"text/template"

	"github.com/huandu/xstrings"
	"gopkg.in/yaml.v3"
)

var (
	protoGoPackageRegex  = regexp.MustCompile(`option\s+go_package\s*=\s*"([^";]+)(?:;[^"]*)?";`)
	pleaseFilenamesRegex = regexp.MustCompile(`\(([^)]+)\)`)
	serviceRegex         = regexp.MustCompile(`service\s+([\w]+)\s+{`)
	publisherRegex       = regexp.MustCompile(`require_nats_publishers:\s*\[([\s\S]*?)\]`)
	goPackageRegex       = regexp.MustCompile(`(?m)^package\s+\w+\s*\n`)
	doOnceCache          = map[string]bool{}
	filepathToContent    = map[string][]byte{}
	keyToGrpcServiceName = map[string]string{}
	goImportPathToAlias  = map[string]string{}
	aliasToGoImportPath  = map[string]string{}
	customFuncMap        = template.FuncMap{
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
// only the fields the graph walks. The template consumes the manifest as a map, keys and all,
// so the raw map is kept alongside this rather than replaced by it.
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

// parseServiceManifest reads the manifest at target into its typed view. The file content is
// cached, so reading one repeatedly costs only the unmarshal.
func parseServiceManifest(target string) (*serviceManifest, error) {
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
	result := &serviceManifest{}
	if err := yaml.Unmarshal(content, result); err != nil {
		return nil, err
	}
	return result, nil
}

// dependenciesOfType returns the manifest's dependencies of one type, in declaration order.
func (m *serviceManifest) dependenciesOfType(dependencyType string) []serviceDependency {
	var result []serviceDependency
	for _, dependency := range m.Dependencies {
		if dependency.Type == dependencyType {
			result = append(result, dependency)
		}
	}
	return result
}

// orderedSet collects strings, keeping the order they first arrived in and ignoring repeats.
// The graph wants both properties everywhere: order so the generated code does not churn
// between builds, deduplication so a service declared twice is one node and one edge.
type orderedSet struct {
	values []string
	seen   map[string]bool
}

func newOrderedSet() *orderedSet {
	return &orderedSet{seen: map[string]bool{}}
}

func (s *orderedSet) add(value string) {
	if s.seen[value] {
		return
	}
	s.seen[value] = true
	s.values = append(s.values, value)
}

func (s *orderedSet) list() []string { return s.values }

// serviceNode is one service in the dependency graph assembled by serviceGraph.
type serviceNode struct {
	// raw is the manifest as the template consumes it, keys the graph does not model included;
	// manifest is the typed view the graph walks.
	raw      map[string]any
	manifest *serviceManifest
	// deps holds the services this one declares as `type: service` dependencies, which are
	// construction edges: the service is an argument to this one's constructor. callDeps holds
	// the services in this binary serving its `grpc_client` dependencies, which are call edges:
	// they order startup but not construction, since the caller holds a client.
	deps     *orderedSet
	callDeps *orderedSet
	// roots holds the services reachable from a server that lead here, so that a nested
	// service's health checks can be attributed to the servers that ultimately use it.
	roots     *orderedSet
	order     int
	level     int
	resolving bool
}

func (n *serviceNode) name() string { return n.manifest.Name }

// serviceGraph resolves the services a main manifest's servers host, and everything those
// services depend on, into dependency-ordered levels. Level 0 holds the services that depend
// on no other service; a service in level N depends only on services in earlier levels, so a
// binary can bring up one level at a time and know that everything a service needs is already
// started. Returns an error on a cycle in the construction dependencies.
//
// Two kinds of dependency order the levels. A `type: service` dependency is a construction
// edge: the service is an argument to the other's constructor, so a cycle in those is an error.
// A `grpc_client` dependency on a service this same binary serves is a call edge: the caller
// reaches it over the wire, so it needs the callee started and the server serving, but the two
// can be built in either order. A call edge that would close a cycle is dropped rather than
// rejected, since services that call each other are a legitimate arrangement.
//
// Each returned service is its manifest, with `type: service` dependencies resolved so they
// carry the depended-on service's name, plus two added keys: `level` and `roots`.
func serviceGraph(rawServers []any) ([][]map[string]any, error) {
	servers, err := decodeServers(rawServers)
	if err != nil {
		return nil, err
	}
	nameToNode := map[string]*serviceNode{}

	newNode := func(raw map[string]any, manifest *serviceManifest) *serviceNode {
		node := &serviceNode{
			raw:      raw,
			manifest: manifest,
			deps:     newOrderedSet(),
			callDeps: newOrderedSet(),
			roots:    newOrderedSet(),
			order:    len(nameToNode),
			level:    -1,
		}
		nameToNode[manifest.Name] = node
		return node
	}

	// collect registers the service at target and everything it depends on, returning its
	// name. root is the name of the service a server actually hosts; an empty root means
	// target is itself such a service. Cycles are left to the level assignment below, which
	// can report the path that forms them.
	visited := map[string]bool{}
	var collect func(target, root string) (string, error)
	collect = func(target, root string) (string, error) {
		manifest, err := parseServiceManifest(target)
		if err != nil {
			return "", fmt.Errorf("reading service manifest %s (is it in the deps of the manifest rule referencing it?): %w", target, err)
		}
		if manifest.Name == "" {
			return "", fmt.Errorf("service manifest %s declares no name", target)
		}

		node, ok := nameToNode[manifest.Name]
		if !ok {
			raw, err := parseYaml(target)
			if err != nil {
				return "", fmt.Errorf("reading service manifest %s: %w", target, err)
			}
			node = newNode(raw, manifest)
		}
		if root == "" {
			root = manifest.Name
		}
		node.roots.add(root)

		// Walking a service once per root is enough: its own dependencies do not change,
		// only the roots we propagate into them.
		if visited[root+"\x00"+manifest.Name] {
			return manifest.Name, nil
		}
		visited[root+"\x00"+manifest.Name] = true

		for _, dependency := range manifest.dependenciesOfType("service") {
			depName, err := collectDependency(dependency, root, collect, nameToNode, newNode)
			if err != nil {
				return "", fmt.Errorf("service %s: %w", manifest.Name, err)
			}
			node.deps.add(depName)
		}
		return manifest.Name, nil
	}

	for _, target := range serviceManifestTargets(servers) {
		if _, err := collect(target, ""); err != nil {
			return nil, err
		}
	}

	// Resolve `grpc_client` dependencies to the services in this binary that serve them, now
	// that every one of those is registered. A dependency served from outside the binary is
	// nothing this binary can order, so it is dropped.
	servedByName, err := servedServices(servers)
	if err != nil {
		return nil, err
	}
	for _, node := range nameToNode {
		for _, dependency := range node.manifest.dependenciesOfType("grpc_client") {
			depName, ok := servedByName[xstrings.ToPascalCase(dependency.Service)]
			if !ok || depName == node.name() {
				continue
			}
			node.callDeps.add(depName)
		}
	}

	// Drop the call edges that would close a cycle. Two services calling each other is a
	// legitimate runtime arrangement — both are up before either serves traffic — so it is not
	// an error, but one of the two has to start first and the edge that says otherwise cannot
	// be honored. Construction edges get no such reprieve below: those really are impossible,
	// since one service is an argument to the other's constructor.
	//
	// Finding those edges is a depth-first walk that marks every service with one of three
	// colors, which is the textbook way to tell a cycle from a diamond:
	//
	//   white  not visited yet.
	//   grey   on the current path: we have entered it and not yet finished its dependencies.
	//   black  finished, along with everything reachable from it.
	//
	// The color of the service on the far end of an edge says what kind of edge it is. Grey
	// means it is an ancestor of where we are standing, so following the edge walks back up the
	// path we came down — a cycle, and the edge closing it is the one we drop. Black means it
	// is finished, so it is reachable by some other route but not by a route through us: a
	// diamond, not a cycle, and the edge stays. Dropping every edge to a grey service is what
	// leaves a graph the levels below can be read off.
	//
	// Construction edges are walked but never dropped, only so that the coloring reflects the
	// whole path. A cycle among them is left intact for the level assignment to report.
	{
		const (
			white = 0
			grey  = 1
			black = 2
		)
		color := map[string]int{}
		var prune func(name string)
		prune = func(name string) {
			node := nameToNode[name]
			color[name] = grey
			for _, dep := range node.deps.list() {
				if color[dep] == white {
					prune(dep)
				}
			}
			kept := newOrderedSet()
			for _, dep := range node.callDeps.list() {
				if color[dep] == grey {
					continue // Closes a cycle: this service starts before the one it calls.
				}
				if color[dep] == white {
					prune(dep)
				}
				kept.add(dep)
			}
			node.callDeps = kept
			color[name] = black
		}
		// Every service gets a walk: the graph need not be connected, and a service nothing
		// depends on is never reached from anywhere else.
		for _, node := range orderedNodes(nameToNode) {
			if color[node.name()] == white {
				prune(node.name())
			}
		}
	}

	// Assign each service the level after the deepest service it depends on.
	var assign func(name string, path []string) (int, error)
	assign = func(name string, path []string) (int, error) {
		node := nameToNode[name]
		if node.resolving {
			return 0, fmt.Errorf("service dependency cycle: %s", strings.Join(append(path, name), " -> "))
		}
		if node.level >= 0 {
			return node.level, nil
		}
		node.resolving = true
		level := 0
		for _, dep := range slices.Concat(node.deps.list(), node.callDeps.list()) {
			depLevel, err := assign(dep, append(path, name))
			if err != nil {
				return 0, err
			}
			if depLevel+1 > level {
				level = depLevel + 1
			}
		}
		node.resolving = false
		node.level = level
		return level, nil
	}

	maxLevel := 0
	for name := range nameToNode {
		level, err := assign(name, nil)
		if err != nil {
			return nil, err
		}
		if level > maxLevel {
			maxLevel = level
		}
	}

	// Group by level, keeping services in the order the manifests declared them so that the
	// generated code does not churn between builds.
	levels := make([][]map[string]any, maxLevel+1)
	for _, node := range orderedNodes(nameToNode) {
		rendered, err := node.render()
		if err != nil {
			return nil, err
		}
		levels[node.level] = append(levels[node.level], rendered)
	}
	return levels, nil
}

// collectDependency resolves a single `type: service` dependency to the name of the service
// it points at, registering that service in the graph.
func collectDependency(
	dependency serviceDependency,
	root string,
	collect func(target, root string) (string, error),
	nameToNode map[string]*serviceNode,
	newNode func(raw map[string]any, manifest *serviceManifest) *serviceNode,
) (string, error) {
	if dependency.Manifest != "" {
		return collect(dependency.Manifest, root)
	}

	// A dependency given as a bare implementation has no manifest to read, so it is a leaf:
	// it takes no dependencies of its own and can always start in the first level.
	if dependency.Name == "" {
		return "", fmt.Errorf("service dependency needs a manifest or a name")
	}
	node, ok := nameToNode[dependency.Name]
	if !ok {
		raw := map[string]any{"name": dependency.Name, "implementation": dependency.Implementation}
		node = newNode(raw, &serviceManifest{Name: dependency.Name})
	}
	node.roots.add(root)
	return dependency.Name, nil
}

// orderedNodes returns the graph's nodes in the order the manifests declared them, so that the
// generated code does not churn between builds.
func orderedNodes(nameToNode map[string]*serviceNode) []*serviceNode {
	nodes := make([]*serviceNode, 0, len(nameToNode))
	for _, node := range nameToNode {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].order < nodes[j].order })
	return nodes
}

// serviceManifestTargets returns, in declaration order and without duplicates, the manifest
// target of every service a main manifest's servers host. A processor server carries its
// service on the server itself rather than in a services list.
func serviceManifestTargets(servers []serverConfig) []string {
	targets := newOrderedSet()
	for _, server := range servers {
		for _, service := range server.Services {
			if service.Manifest != "" {
				targets.add(service.Manifest)
			}
		}
		if server.Manifest != "" {
			targets.add(server.Manifest)
		}
	}
	return targets.list()
}

// servedServices maps each proto service a server hosts, PascalCased as `grpc_client`
// dependencies name it, to the name of the service manifest serving it. One manifest can serve
// several proto services, which is why the mapping runs this way round.
func servedServices(servers []serverConfig) (map[string]string, error) {
	result := map[string]string{}
	for _, server := range servers {
		for _, service := range server.Services {
			if service.Service == "" || service.Manifest == "" {
				continue
			}
			manifest, err := parseServiceManifest(service.Manifest)
			if err != nil {
				return nil, fmt.Errorf("reading service manifest %s: %w", service.Manifest, err)
			}
			if manifest.Name == "" {
				return nil, fmt.Errorf("service manifest %s declares no name", service.Manifest)
			}
			result[xstrings.ToPascalCase(service.Service)] = manifest.Name
		}
	}
	return result, nil
}

// render copies this node's manifest for the template, naming each service dependency and
// adding the level and roots the graph computed.
func (n *serviceNode) render() (map[string]any, error) {
	rendered := map[string]any{}
	for key, value := range n.raw {
		rendered[key] = value
	}
	rendered["level"] = n.level
	rendered["roots"] = n.roots.list()

	rawDependencies, _ := n.raw["dependencies"].([]any)
	if len(rawDependencies) != len(n.manifest.Dependencies) {
		return nil, fmt.Errorf("service %s: %d dependencies typed, %d raw", n.name(), len(n.manifest.Dependencies), len(rawDependencies))
	}
	renderedDependencies := make([]any, 0, len(rawDependencies))
	for i, rawDependency := range rawDependencies {
		dependency := n.manifest.Dependencies[i]
		rawDependencyMap, ok := rawDependency.(map[string]any)
		if !ok || dependency.Type != "service" {
			renderedDependencies = append(renderedDependencies, rawDependency)
			continue
		}
		name := dependency.Name
		if dependency.Manifest != "" {
			manifest, err := parseServiceManifest(dependency.Manifest)
			if err != nil {
				return nil, fmt.Errorf("service %s: reading dependency manifest %s: %w", n.name(), dependency.Manifest, err)
			}
			name = manifest.Name
		}
		renderedDependency := map[string]any{}
		for key, value := range rawDependencyMap {
			renderedDependency[key] = value
		}
		renderedDependency["name"] = name
		renderedDependencies = append(renderedDependencies, renderedDependency)
	}
	if len(rawDependencies) > 0 {
		rendered["dependencies"] = renderedDependencies
	}
	return rendered, nil
}

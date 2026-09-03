package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"text/template"
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

// serviceNode is one service in the dependency graph assembled by serviceGraph.
type serviceNode struct {
	manifest map[string]any
	name     string
	// deps holds, deduplicated, the names of the services this one declares as
	// `type: service` dependencies; depNames holds one name per declared dependency, in
	// declaration order, so the template can name each of them where they appear.
	deps     []string
	depNames []string
	// roots holds the names of the services reachable from a server that lead here, so that
	// a nested service's health checks can be attributed to the servers that ultimately use it.
	roots     []string
	rootSeen  map[string]bool
	depSeen   map[string]bool
	order     int
	level     int
	resolving bool
}

// serviceGraph resolves the services a main manifest's servers host, and everything those
// services depend on, into dependency-ordered levels. Level 0 holds the services that depend
// on no other service; a service in level N depends only on services in earlier levels, so a
// binary can bring up one level at a time and know that everything a service needs is already
// started. Returns an error on a dependency cycle.
//
// Each returned service is its manifest, with `type: service` dependencies resolved so they
// carry the depended-on service's name, plus two added keys: `level` and `roots`.
func serviceGraph(servers []any) ([][]map[string]any, error) {
	nameToNode := map[string]*serviceNode{}

	newNode := func(name string, manifest map[string]any) *serviceNode {
		node := &serviceNode{
			manifest: manifest,
			name:     name,
			rootSeen: map[string]bool{},
			depSeen:  map[string]bool{},
			order:    len(nameToNode),
			level:    -1,
		}
		nameToNode[name] = node
		return node
	}

	// collect registers the service at target and everything it depends on, returning its
	// name. root is the name of the service a server actually hosts; an empty root means
	// target is itself such a service. Cycles are left to the level assignment below, which
	// can report the path that forms them.
	visited := map[string]bool{}
	var collect func(target, root string) (string, error)
	collect = func(target, root string) (string, error) {
		manifest, err := parseYaml(target)
		if err != nil {
			return "", fmt.Errorf("reading service manifest %s (is it in the deps of the manifest rule referencing it?): %w", target, err)
		}
		name, _ := manifest["name"].(string)
		if name == "" {
			return "", fmt.Errorf("service manifest %s declares no name", target)
		}
		if root == "" {
			root = name
		}

		node, ok := nameToNode[name]
		if !ok {
			node = newNode(name, manifest)
		}
		if !node.rootSeen[root] {
			node.rootSeen[root] = true
			node.roots = append(node.roots, root)
		}

		// Walking a service once per root is enough: its own dependencies do not change,
		// only the roots we propagate into them.
		if visited[root+"\x00"+name] {
			return name, nil
		}
		visited[root+"\x00"+name] = true

		for _, dependency := range serviceDependencies(manifest) {
			depName, err := collectDependency(dependency, root, collect, nameToNode, newNode)
			if err != nil {
				return "", fmt.Errorf("service %s: %w", name, err)
			}
			node.depNames = append(node.depNames, depName)
			if !node.depSeen[depName] {
				node.depSeen[depName] = true
				node.deps = append(node.deps, depName)
			}
		}
		return name, nil
	}

	targets, err := serviceManifestTargets(servers)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		if _, err := collect(target, ""); err != nil {
			return nil, err
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
		for _, dep := range node.deps {
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
	nodes := make([]*serviceNode, 0, len(nameToNode))
	for _, node := range nameToNode {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].order < nodes[j].order })

	levels := make([][]map[string]any, maxLevel+1)
	for _, node := range nodes {
		levels[node.level] = append(levels[node.level], node.render())
	}
	return levels, nil
}

// collectDependency resolves a single `type: service` dependency to the name of the service
// it points at, registering that service in the graph.
func collectDependency(
	dependency map[string]any,
	root string,
	collect func(target, root string) (string, error),
	nameToNode map[string]*serviceNode,
	newNode func(name string, manifest map[string]any) *serviceNode,
) (string, error) {
	if target, _ := dependency["manifest"].(string); target != "" {
		return collect(target, root)
	}

	// A dependency given as a bare implementation has no manifest to read, so it is a leaf:
	// it takes no dependencies of its own and can always start in the first level.
	name, _ := dependency["name"].(string)
	implementation, _ := dependency["implementation"].(string)
	if name == "" {
		return "", fmt.Errorf("service dependency needs a manifest or a name")
	}
	node, ok := nameToNode[name]
	if !ok {
		node = newNode(name, map[string]any{"name": name, "implementation": implementation})
	}
	if !node.rootSeen[root] {
		node.rootSeen[root] = true
		node.roots = append(node.roots, root)
	}
	return name, nil
}

// serviceManifestTargets returns, in declaration order and without duplicates, the manifest
// target of every service a main manifest's servers host. A processor server carries its
// service on the server itself rather than in a services list.
func serviceManifestTargets(servers []any) ([]string, error) {
	var targets []string
	seen := map[string]bool{}
	add := func(target string) {
		if target != "" && !seen[target] {
			seen[target] = true
			targets = append(targets, target)
		}
	}
	for _, server := range servers {
		serverMap, ok := server.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("server is not a mapping: %v", server)
		}
		services, _ := serverMap["services"].([]any)
		for _, service := range services {
			serviceMap, ok := service.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("service is not a mapping: %v", service)
			}
			target, _ := serviceMap["manifest"].(string)
			add(target)
		}
		target, _ := serverMap["manifest"].(string)
		add(target)
	}
	return targets, nil
}

// serviceDependencies returns the `type: service` entries of a manifest's dependency list.
func serviceDependencies(manifest map[string]any) []map[string]any {
	var result []map[string]any
	dependencies, _ := manifest["dependencies"].([]any)
	for _, dependency := range dependencies {
		dependencyMap, ok := dependency.(map[string]any)
		if !ok {
			continue
		}
		if dependencyType, _ := dependencyMap["type"].(string); dependencyType == "service" {
			result = append(result, dependencyMap)
		}
	}
	return result
}

// render copies this node's manifest for the template, naming each service dependency and
// adding the level and roots the graph computed.
func (n *serviceNode) render() map[string]any {
	rendered := map[string]any{}
	for key, value := range n.manifest {
		rendered[key] = value
	}
	rendered["level"] = n.level
	rendered["roots"] = n.roots

	dependencies, _ := n.manifest["dependencies"].([]any)
	renderedDependencies := make([]any, 0, len(dependencies))
	serviceDependencyIndex := 0
	for _, dependency := range dependencies {
		dependencyMap, ok := dependency.(map[string]any)
		if !ok {
			renderedDependencies = append(renderedDependencies, dependency)
			continue
		}
		if dependencyType, _ := dependencyMap["type"].(string); dependencyType != "service" {
			renderedDependencies = append(renderedDependencies, dependency)
			continue
		}
		renderedDependency := map[string]any{}
		for key, value := range dependencyMap {
			renderedDependency[key] = value
		}
		renderedDependency["name"] = n.depNames[serviceDependencyIndex]
		serviceDependencyIndex++
		renderedDependencies = append(renderedDependencies, renderedDependency)
	}
	if len(dependencies) > 0 {
		rendered["dependencies"] = renderedDependencies
	}
	return rendered
}

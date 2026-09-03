package main

import (
	"fmt"
	"os"
	"path"
	"regexp"
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

// serviceNode is one service in the dependency graph assembled by serviceGraph.
type serviceNode struct {
	manifest map[string]any
	name     string
	// callServices holds the proto service names this service declares as `grpc_client`
	// dependencies, and callDeps the services in this binary that serve them, deduplicated.
	callServices []string
	callDeps     []string
	callSeen     map[string]bool
	order        int
	level        int
	levelKnown   bool
}

// serviceGraph resolves the services a main manifest's servers host into dependency-ordered
// levels. Level 0 holds the services that call no other service in this binary; a service in
// level N calls only services in earlier levels, so a binary can bring up one level at a time
// and know that everything a service reaches is already started.
//
// The edges come from `grpc_client` dependencies on a service this same binary serves. The
// caller reaches it over the wire, so it needs the callee started and the server serving, but
// the two can be constructed in either order — which is also why an edge that would close a
// cycle is dropped rather than rejected. Services that call each other are a legitimate
// arrangement; one of them simply has to start first.
//
// Each returned service is its manifest with one added key: `level`.
func serviceGraph(servers []any) ([][]map[string]any, error) {
	nameToNode := map[string]*serviceNode{}

	targets, err := serviceManifestTargets(servers)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		manifest, err := parseYaml(target)
		if err != nil {
			return nil, fmt.Errorf("reading service manifest %s (is it in the deps of the manifest rule referencing it?): %w", target, err)
		}
		name, _ := manifest["name"].(string)
		if name == "" {
			return nil, fmt.Errorf("service manifest %s declares no name", target)
		}
		if _, ok := nameToNode[name]; ok {
			continue
		}
		nameToNode[name] = &serviceNode{
			manifest:     manifest,
			name:         name,
			callServices: clientDependencies(manifest),
			callSeen:     map[string]bool{},
			order:        len(nameToNode),
		}
	}

	// Resolve `grpc_client` dependencies to the services in this binary that serve them. A
	// dependency served from outside the binary is nothing this binary can order, so it is
	// dropped.
	servedByName, err := servedServices(servers)
	if err != nil {
		return nil, err
	}
	for _, node := range nameToNode {
		for _, callService := range node.callServices {
			depName, ok := servedByName[xstrings.ToPascalCase(callService)]
			if !ok || depName == node.name || node.callSeen[depName] {
				continue
			}
			node.callSeen[depName] = true
			node.callDeps = append(node.callDeps, depName)
		}
	}

	// Drop the edges that would close a cycle, leaving a graph the levels below can be read
	// off. Removing every back edge a depth-first walk meets is what makes it acyclic.
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
		kept := node.callDeps[:0]
		for _, dep := range node.callDeps {
			if color[dep] == grey {
				continue // Closes a cycle: this service starts before the one it calls.
			}
			if color[dep] == white {
				prune(dep)
			}
			kept = append(kept, dep)
		}
		node.callDeps = kept
		color[name] = black
	}
	for _, node := range orderedNodes(nameToNode) {
		if color[node.name] == white {
			prune(node.name)
		}
	}

	// Assign each service the level after the deepest service it calls.
	var assign func(name string) int
	assign = func(name string) int {
		node := nameToNode[name]
		if node.levelKnown {
			return node.level
		}
		node.levelKnown = true
		for _, dep := range node.callDeps {
			if depLevel := assign(dep) + 1; depLevel > node.level {
				node.level = depLevel
			}
		}
		return node.level
	}
	maxLevel := 0
	for name := range nameToNode {
		if level := assign(name); level > maxLevel {
			maxLevel = level
		}
	}

	// Group by level, keeping services in the order the manifests declared them so that the
	// generated code does not churn between builds.
	levels := make([][]map[string]any, maxLevel+1)
	for _, node := range orderedNodes(nameToNode) {
		rendered := map[string]any{}
		for key, value := range node.manifest {
			rendered[key] = value
		}
		rendered["level"] = node.level
		levels[node.level] = append(levels[node.level], rendered)
	}
	return levels, nil
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

// servedServices maps each proto service a server hosts, PascalCased as `grpc_client`
// dependencies name it, to the name of the service manifest serving it. One manifest can serve
// several proto services, which is why the mapping runs this way round.
func servedServices(servers []any) (map[string]string, error) {
	result := map[string]string{}
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
			servedService, _ := serviceMap["service"].(string)
			target, _ := serviceMap["manifest"].(string)
			if servedService == "" || target == "" {
				continue
			}
			manifest, err := parseYaml(target)
			if err != nil {
				return nil, fmt.Errorf("reading service manifest %s: %w", target, err)
			}
			name, _ := manifest["name"].(string)
			if name == "" {
				return nil, fmt.Errorf("service manifest %s declares no name", target)
			}
			result[xstrings.ToPascalCase(servedService)] = name
		}
	}
	return result, nil
}

// clientDependencies returns the proto service names of a manifest's `grpc_client` entries.
func clientDependencies(manifest map[string]any) []string {
	var result []string
	dependencies, _ := manifest["dependencies"].([]any)
	for _, dependency := range dependencies {
		dependencyMap, ok := dependency.(map[string]any)
		if !ok {
			continue
		}
		if dependencyType, _ := dependencyMap["type"].(string); dependencyType != "grpc_client" {
			continue
		}
		if service, _ := dependencyMap["service"].(string); service != "" {
			result = append(result, service)
		}
	}
	return result
}

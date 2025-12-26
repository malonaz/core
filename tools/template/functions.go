package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"regexp"
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

		"parseYaml": parseYaml,
		"parseGRPC": parseGRPC,

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

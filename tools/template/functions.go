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
	pleaseFilenameRegex    = regexp.MustCompile(`[^(]+\(([^)]+)\)`)
	serviceRegex           = regexp.MustCompile(`service\s+([\w]+)\s+{`)
	publisherRegex         = regexp.MustCompile(`require_nats_publishers:\s*\[([\s\S]*?)\]`)
	goPackageRegex         = regexp.MustCompile(`(?m)^package\s+\w+\s*\n`)
	doOnceCache            = map[string]bool{}
	filepathToContent      = map[string][]byte{}
	labelToGrpcServiceName = map[string]string{}
	goImportPathToAlias    = map[string]string{}
	aliasToGoImportPath    = map[string]string{}
	customFuncMap          = template.FuncMap{
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

		"plzGoImport":        plzGoImport,
		"goImport":           goImport,
		"protoGoImportAlias": protoGoImportAlias,
		"grpcSvcName":        grpcSvcName,

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

func readFile(label string) ([]byte, error) {
	filepath, err := extractFilenameFromLabel(label)
	if err != nil {
		return nil, fmt.Errorf("extracing filename from label: %v", err)
	}
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

func goImport(in ...string) (string, error) {
	var importPath, requestedAlias string
	if len(in) == 1 {
		importPath = in[0]
		requestedAlias = path.Base(importPath)
	} else if len(in) == 2 {
		importPath = in[0]
		requestedAlias = in[1]
	} else {
		return "", fmt.Errorf("goImport: expected 1 or 2 arguments, got %d", len(in))
	}

	// Check if we've already processed this import path
	if existingAlias, exists := goImportPathToAlias[importPath]; exists {
		return existingAlias, nil
	}

	// Find an available alias
	finalAlias := findAvailableAlias(requestedAlias, importPath)

	// Store the mapping both ways
	goImportPathToAlias[importPath] = finalAlias
	aliasToGoImportPath[finalAlias] = importPath

	return finalAlias, nil
}

// Imports plz go labels like "//user/proto:api".
func plzGoImport(in ...string) (string, error) {
	label := in[0]

	// Remove trailing (filepath) if present
	if pleaseFilenameRegex.MatchString(label) {
		// Extract just the label part (everything before the parentheses)
		parts := strings.Split(label, "(")
		if len(parts) > 0 {
			label = parts[0]
		}
	}

	var importPath string

	// Check if label contains a colon
	if strings.Contains(label, ":") {
		parts := strings.Split(label, ":")
		if len(parts) == 2 {
			dirPath := strings.TrimPrefix(parts[0], "//")
			targetName := parts[1]

			// Get the last directory name
			lastDir := path.Base(dirPath)

			// If target name matches last directory, don't append it
			if lastDir == targetName {
				importPath = dirPath
			} else {
				importPath = dirPath + "/" + targetName
			}
		}
	} else {
		// No colon, just trim the prefix
		importPath = strings.TrimPrefix(label, "//")
	}

	in[0] = importPath
	return goImport(in...)
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

func protoGoImportAlias(filepath string) (string, error) {
	grpcSvcName, err := grpcSvcName(filepath)
	if err != nil {
		return "", err
	}
	return strings.ToLower(grpcSvcName) + "pb", nil
}

func grpcSvcName(label string) (string, error) {
	if serviceName, ok := labelToGrpcServiceName[label]; ok {
		return serviceName, nil
	}
	bytes, err := readFile(label)
	if err != nil {
		return "", err
	}
	content := string(bytes)

	// Find the service definition

	matches := serviceRegex.FindStringSubmatch(content)
	if len(matches) != 2 {
		return "", fmt.Errorf("no service found")
	}

	// Extract the service name
	serviceName := matches[1]
	if len(serviceName) == 0 {
		return "", fmt.Errorf("empty service name found")
	}
	labelToGrpcServiceName[label] = serviceName
	return serviceName, nil
}

func parseYaml(label string) (map[string]any, error) {
	bytes, err := readFile(label)
	if err != nil {
		return nil, err
	}
	data := map[string]any{}
	if err := yaml.Unmarshal(bytes, &data); err != nil {
		return nil, err
	}
	return data, nil
}

// Returns the filename and a boolean indicating if the pattern was found
func extractFilenameFromLabel(input string) (string, error) {
	matches := pleaseFilenameRegex.FindStringSubmatch(input)
	if len(matches) >= 2 {
		return matches[1], nil
	}
	return "", fmt.Errorf("no match found")
}

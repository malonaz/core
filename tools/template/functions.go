package main

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"text/template"
)

var (
	serviceRegex              = regexp.MustCompile(`service\s+([\w]+)\s+{`)
	publisherRegex            = regexp.MustCompile(`require_nats_publishers:\s*\[([\s\S]*?)\]`)
	goPackageRegex            = regexp.MustCompile(`(?m)^package\s+\w+\s*\n`)
	doOnceCache               = map[string]bool{}
	filepathToContent         = map[string]string{}
	filepathToGrpcServiceName = map[string]string{}
	goImportPathToAlias       = map[string]string{}
	aliasToGoImportPath       = map[string]string{}
	customFuncMap             = template.FuncMap{
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

		"readFile": readFile,

		// Imports plz go labels like "//user/proto:api".
		"plzGoImport": func(in ...string) (string, error) {
			label := in[0]
			importPath := strings.TrimPrefix(strings.ReplaceAll(label, ":", "/"), "//")
			in[0] = importPath
			return goImport(in...)
		},

		"goImport": goImport,

		"protoGoImportAlias": func(filepath string) (string, error) {
			grpcSvcName, err := grpcSvcName(filepath)
			if err != nil {
				return "", err
			}
			return strings.ToLower(grpcSvcName) + "pb", nil
		},

		"grpcSvcName": grpcSvcName,

		"grpcNatsPublishers": func(filepath string) ([]string, error) {
			sanitizedFilepath := strings.TrimPrefix(filepath, "//")
			sanitizedFilepath = strings.ReplaceAll(sanitizedFilepath, ":", "/")
			content, err := readFile(sanitizedFilepath + ".proto")
			if err != nil {
				return nil, err
			}

			// Find all matches
			matches := publisherRegex.FindStringSubmatch(content)
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

func readFile(filepath string) (string, error) {
	if content, ok := filepathToContent[filepath]; ok {
		return content, nil
	}
	bytes, err := os.ReadFile(filepath)
	if err != nil {
		return "", err
	}
	content := string(bytes)
	filepathToContent[filepath] = content
	return content, nil
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

func grpcSvcName(filepath string) (string, error) {
	if serviceName, ok := filepathToGrpcServiceName[filepath]; ok {
		return serviceName, nil
	}
	sanitizedFilepath := strings.TrimPrefix(filepath, "//")
	sanitizedFilepath = strings.ReplaceAll(sanitizedFilepath, ":", "/")
	content, err := readFile(sanitizedFilepath + ".proto")
	if err != nil {
		return "", err
	}

	// Find the service definition
	matches := serviceRegex.FindStringSubmatch(content)
	if len(matches) != 2 {
		return "", fmt.Errorf("no service found in %s", sanitizedFilepath)
	}

	// Extract the service name
	serviceName := matches[1]
	if len(serviceName) == 0 {
		return "", fmt.Errorf("empty service name found in %s", sanitizedFilepath)
	}
	filepathToGrpcServiceName[filepath] = serviceName
	return serviceName, nil
}

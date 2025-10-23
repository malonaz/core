package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"text/template"
)

var (
	serviceRegex              = regexp.MustCompile(`service\s+([\w]+)\s+{`)
	publisherRegex            = regexp.MustCompile(`require_nats_publishers:\s*\[([\s\S]*?)\]`)
	filepathToContent         = map[string]string{}
	filepathToGrpcServiceName = map[string]string{}
	customFuncMap             = template.FuncMap{
		"debug": func(v any) error {
			fmt.Printf("%+v\n", v)
			return nil
		},

		"readFile": readFile,

		"grpcSvcName": func(filepath string) (string, error) {
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
		},

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

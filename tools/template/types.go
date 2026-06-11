package main

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
)

const coreRepo = "github.com/malonaz/core/go"

var targetToGRPCs = map[string][]*GRPC{}

// Contains all the information about a grpc service.
type GRPC struct {
	target       string
	replacements map[string]string
}

// parseGRPCs resolves a proto target into one *GRPC per service.
// services filters which services to instantiate; nil or empty means all services in the library.
// It accepts `any` because template data (YAML lists, sprig's `list`) arrives as []any.
func parseGRPCs(target, depName string, services any) ([]*GRPC, error) {
	requestedServiceNames, err := toStringSlice(services)
	if err != nil {
		return nil, err
	}
	cacheKey := target + "|" + depName + "|" + strings.Join(requestedServiceNames, ",")
	if grpcs, ok := targetToGRPCs[cacheKey]; ok {
		return grpcs, nil
	}
	_, filenames, err := parseTarget(target)
	if err != nil {
		return nil, err
	}

	// Collect all service names defined across the target's files.
	var serviceNames []string
	for _, filename := range filenames {
		bytes, err := readFile(filename)
		if err != nil {
			return nil, err
		}
		for _, match := range serviceRegex.FindAllStringSubmatch(string(bytes), -1) {
			serviceNames = append(serviceNames, match[1])
		}
	}
	if len(serviceNames) == 0 {
		return nil, fmt.Errorf("no service found in %s", target)
	}

	// Filter down to the requested services, defaulting to all of them.
	selectedServiceNames := serviceNames
	if len(requestedServiceNames) > 0 {
		serviceNameSet := map[string]bool{}
		for _, serviceName := range serviceNames {
			serviceNameSet[serviceName] = true
		}
		for _, requestedServiceName := range requestedServiceNames {
			if !serviceNameSet[requestedServiceName] {
				return nil, fmt.Errorf("service %q not found in %s (available: [%s])", requestedServiceName, target, strings.Join(serviceNames, ", "))
			}
		}
		selectedServiceNames = requestedServiceNames
	}

	grpcs := make([]*GRPC, 0, len(selectedServiceNames))
	for _, serviceName := range selectedServiceNames {
		// Each service is fully independent (own opts, connection, client, health check)
		// because two services from one library may be served by different servers.
		// The dep name prefixes the service name when selecting multiple services so
		// that two deps pointing at the same library cannot collide.
		displayName := serviceName
		switch {
		case depName != "" && len(selectedServiceNames) > 1:
			displayName = xstrings.ToPascalCase(depName) + serviceName
		case depName != "":
			displayName = xstrings.ToPascalCase(depName)
		}
		grpcs = append(grpcs, newGRPC(target, serviceName, displayName))
	}
	targetToGRPCs[cacheKey] = grpcs
	return grpcs, nil
}

// parseGRPC retains the single-service contract for proto libraries defining exactly one service.
func parseGRPC(target, depName string) (*GRPC, error) {
	grpcs, err := parseGRPCs(target, depName, nil)
	if err != nil {
		return nil, err
	}
	if len(grpcs) != 1 {
		return nil, fmt.Errorf("found %d services in %s: use parseGRPCs", len(grpcs), target)
	}
	return grpcs[0], nil
}

func newGRPC(target, serviceName, displayName string) *GRPC {
	nameCamelCase := strings.ToLower(serviceName[:1]) + serviceName[1:]
	nameCamelCaseT := strings.Title(serviceName)
	nameCamelCaseUpper := strings.ToUpper(nameCamelCase)
	nameSnakeCase := xstrings.ToSnakeCase(nameCamelCase)
	nameSnakeCaseUpper := strings.ToUpper(nameSnakeCase)
	nameKebabCase := xstrings.ToKebabCase(serviceName)
	nameHumanCaseT := strings.Title(strings.ReplaceAll(nameKebabCase, "-", " "))

	displayNameCamelCase := strings.ToLower(displayName[:1]) + displayName[1:]
	displayNameCamelCaseT := strings.Title(displayName)
	displayNameCamelCaseUpper := strings.ToUpper(displayNameCamelCase)
	displayNameSnakeCase := xstrings.ToSnakeCase(displayNameCamelCase)
	displayNameSnakeCaseUpper := strings.ToUpper(displayNameSnakeCase)
	displayNameKebabCase := xstrings.ToKebabCase(displayName)
	displayNameHumanCaseT := strings.Title(strings.ReplaceAll(displayNameKebabCase, "-", " "))

	m := map[string]string{
		"packageImport":      strings.ToLower(nameCamelCase) + "pb",
		"nameCamelCase":      nameCamelCase,
		"nameCamelCaseT":     nameCamelCaseT,
		"nameCamelCaseUpper": nameCamelCaseUpper,
		"nameSnakeCaseUpper": nameSnakeCaseUpper,
		"nameSnakeCase":      nameSnakeCase,
		"nameKebabCase":      nameKebabCase,
		"nameHumanCaseT":     nameHumanCaseT,

		"displayNameCamelCase":      displayNameCamelCase,
		"displayNameCamelCaseT":     displayNameCamelCaseT,
		"displayNameCamelCaseUpper": displayNameCamelCaseUpper,
		"displayNameSnakeCaseUpper": displayNameSnakeCaseUpper,
		"displayNameSnakeCase":      displayNameSnakeCase,
		"displayNameKebabCase":      displayNameKebabCase,
		"displayNameHumanCaseT":     displayNameHumanCaseT,

		// Higher level functions.
		"optsFieldName": displayNameCamelCaseT + "GRPC",
		"connection":    displayNameCamelCase + "Connection",
		"healthCheck":   displayNameCamelCase + "HealthCheck",
		"client":        displayNameCamelCase + "Client",
	}
	return &GRPC{
		target:       target,
		replacements: m,
	}
}

func toStringSlice(v any) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	switch t := v.(type) {
	case []string:
		return t, nil
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("expected string, got %T", item)
			}
			out = append(out, s)
		}
		return out, nil
	}
	return nil, fmt.Errorf("expected list of strings, got %T", v)
}

func (t *GRPC) getReplacements(grpcImport, protoImport bool) (map[string]string, error) {
	if !grpcImport && !protoImport {
		return t.replacements, nil
	}
	m := make(map[string]string, len(t.replacements)+1)
	for k, v := range t.replacements {
		m[k] = v
	}
	if grpcImport {
		alias, err := goImport(coreRepo + "/grpc")
		if err != nil {
			return nil, err
		}
		m["grpcImport"] = alias
	}
	if protoImport {
		alias, err := plzGoImportAlias(t.target, m["packageImport"])
		if err != nil {
			return nil, err
		}
		m["protoImport"] = alias
	}
	return m, nil
}

// replaceTemplate replaces all {key} occurrences in template with corresponding values from the map
func (t *GRPC) template(template string, params ...any) (string, error) {
	template = fmt.Sprintf(template, params...)
	grpcImport := strings.Contains(template, "{grpcImport}")
	protoImport := strings.Contains(template, "{protoImport}")
	replacements, err := t.getReplacements(grpcImport, protoImport)
	if err != nil {
		return "", nil
	}
	result := template
	for key, value := range replacements {
		placeholder := fmt.Sprintf("{%s}", key)
		result = strings.ReplaceAll(result, placeholder, value)
	}
	return result, nil
}

// //////////////////////////// Methods to be used in templates are below ///////////////////////////
func (t *GRPC) ServiceDescriptionName() (string, error) {
	return t.template("{protoImport}.{nameCamelCaseT}_ServiceDesc.ServiceName")
}

func (t *GRPC) HumanName() string {
	return t.replacements["displayNameHumanCaseT"]
}

func (t *GRPC) OptsFieldName() string {
	return t.replacements["optsFieldName"]
}

func (t *GRPC) Opts() (string, error) {
	return t.template("{optsFieldName} *{grpcImport}.Opts `group:\"{displayNameHumanCaseT} GRPC (Client)\" namespace:\"{displayNameKebabCase}-grpc\" env-namespace:\"{displayNameSnakeCaseUpper}_GRPC\"`")
}

func (t *GRPC) Connection() string {
	return t.replacements["connection"]
}

func (t *GRPC) HealthCheck() string {
	return t.replacements["healthCheck"]
}

func (t *GRPC) Client() string {
	return t.replacements["client"]
}

func (t *GRPC) ClientInterface() (string, error) {
	return t.template("{protoImport}.{nameCamelCaseT}Client")
}

func (t *GRPC) NewConnection() (string, error) {
	return t.template("{grpcImport}.NewConnection(opts.{optsFieldName}, opts.Certs, opts.Prometheus)")
}

func (t *GRPC) NewClient() (string, error) {
	return t.template("{protoImport}.New{nameCamelCaseT}Client({connection}.Get())")
}

func (t *GRPC) Register(serviceName string) (string, error) {
	return t.template("{protoImport}.Register{nameCamelCaseT}Server(server.Raw, %s)", serviceName)
}

func (t *GRPC) RegisterHandlerFromEndpoint() (string, error) {
	return t.template("{protoImport}.Register{nameCamelCaseT}HandlerFromEndpoint")
}

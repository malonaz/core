package main

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
)

const coreRepo = "github.com/malonaz/core/go"

var targetToGRPC = map[string]*GRPC{}

type GRPC struct {
	target       string
	replacements map[string]string
}

func parseGRPC(target, service string, displayName ...string) (*GRPC, error) {
	cacheKey := target + ":" + service
	if grpc, ok := targetToGRPC[cacheKey]; ok {
		return grpc, nil
	}

	servicePascal := xstrings.ToPascalCase(service)

	_, filenames, err := parseTarget(target)
	if err != nil {
		return nil, err
	}

	var found bool
	for _, filename := range filenames {
		bytes, err := readFile(filename)
		if err != nil {
			return nil, err
		}
		for _, match := range serviceRegex.FindAllStringSubmatch(string(bytes), -1) {
			if len(match) == 2 && match[1] == servicePascal {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("service %q (%s) not found in target %s", service, servicePascal, target)
	}

	display := service
	if len(displayName) > 0 && displayName[0] != "" {
		display = displayName[0]
	}
	displayPascal := xstrings.ToPascalCase(display)

	nameCamelCase := strings.ToLower(servicePascal[:1]) + servicePascal[1:]
	nameCamelCaseT := strings.Title(servicePascal)
	nameCamelCaseUpper := strings.ToUpper(nameCamelCase)
	nameSnakeCase := xstrings.ToSnakeCase(nameCamelCase)
	nameSnakeCaseUpper := strings.ToUpper(nameSnakeCase)
	nameKebabCase := xstrings.ToKebabCase(servicePascal)
	nameHumanCaseT := strings.Title(strings.ReplaceAll(nameKebabCase, "-", " "))

	displayNameCamelCase := strings.ToLower(displayPascal[:1]) + displayPascal[1:]
	displayNameCamelCaseT := strings.Title(displayPascal)
	displayNameCamelCaseUpper := strings.ToUpper(displayNameCamelCase)
	displayNameSnakeCase := xstrings.ToSnakeCase(displayNameCamelCase)
	displayNameSnakeCaseUpper := strings.ToUpper(displayNameSnakeCase)
	displayNameKebabCase := xstrings.ToKebabCase(displayPascal)
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

		"optsFieldName": displayNameCamelCaseT + "GRPC",
		"connection":    displayNameCamelCase + "Connection",
		"healthCheck":   displayNameCamelCase + "HealthCheck",
		"client":        displayNameCamelCase + "Client",
	}
	grpc := &GRPC{
		target:       target,
		replacements: m,
	}
	targetToGRPC[cacheKey] = grpc
	return grpc, nil
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

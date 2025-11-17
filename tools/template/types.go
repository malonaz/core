package main

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
)

const coreRepo = "github.com/malonaz/core/go"

var inputToGRPC = map[string]*GRPC{}

// Contains all the information about a grpc server.
type GRPC struct {
	input        string
	replacements map[string]string
}

func parseGRPC(input, depName string) (*GRPC, error) {
	if grpc, ok := inputToGRPC[input]; ok {
		return grpc, nil
	}
	serviceName, err := grpcSvcName(input)
	if err != nil {
		return nil, err
	}
	displayName := serviceName
	if depName != "" {
		displayName = xstrings.ToPascalCase(depName)
	}

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
	grpc := &GRPC{
		input:        input,
		replacements: m,
	}
	inputToGRPC[input] = grpc
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
		alias, err := plzGoImport(t.input, m["packageImport"])
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

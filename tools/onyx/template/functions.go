package template

import (
	"text/template"

	"github.com/Masterminds/sprig/v3"
)

func getFuncMap() template.FuncMap {
	funcMap := sprig.TxtFuncMap()
	for functionName, function := range functionNameToFunction {
		funcMap[functionName] = function
	}
	return funcMap
}

var functionNameToFunction = map[string]any{}

package template

import (
	"embed"
	"fmt"
	"strings"
	"text/template"
)

//go:embed templates/buildify/*.tmpl
var templateFiles embed.FS

type Engine struct {
	tmpl *template.Template
}

func NewEngine() (*Engine, error) {
	funcMap := getFuncMap()
	tmpl, err := template.New("").Funcs(funcMap).
		Option("missingkey=error").
		ParseFS(templateFiles, "templates/buildify/*.tmpl")

	if err != nil {
		return nil, fmt.Errorf("parsing template: %v", err)
	}

	return &Engine{
		tmpl: tmpl,
	}, nil
}

func (e *Engine) EvaluateTemplate(templateName string, data any, opts ...EvaluationOpt) (string, error) {
	// Apply options.
	config := DefaultEvaluationConfig()
	for _, opt := range opts {
		opt(config)
	}

	// Execute template.
	var sb strings.Builder
	if err := e.tmpl.ExecuteTemplate(&sb, templateName, data); err != nil {
		return "", err
	}
	output := sb.String()
	return output, nil
}

// //////////////////////////// OPTIONS //////////////////////////////
type EvaluationConfig struct {
}

// EvaluationOpt is a function that modifies EvaluationConfig
type EvaluationOpt func(*EvaluationConfig)

// DefaultEvaluationConfig returns the default configuration
func DefaultEvaluationConfig() *EvaluationConfig {
	return &EvaluationConfig{}
}

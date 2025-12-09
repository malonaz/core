package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Masterminds/sprig/v3"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
	"text/template"

	"github.com/malonaz/core/go/flags"
	"github.com/malonaz/core/go/logging"
)

var opts struct {
	Logging      *logging.Opts
	Templates    []string `long:"template" description:"The template files to use" required:"true"`
	Data         string   `long:"data" description:"The data file to use"`
	DataFormat   string   `long:"data-format" description:"The data format to use (json or yaml)" default:"json"`
	Output       string   `long:"output" short:"o" description:"The output file to create" required:"true"`
	Delims       string   `long:"delims" description:"Template delimiters format (e.g., '[[.]]' or '{{.}}')" default:"{{.}}"`
	ExtraData    []string `long:"extra-data" description:"Extra data to pass in the format: key:value"`
	GoImportPath string   `long:"go-import-path" description:"The plz go plugin import path"`
}

func parseDelims(format string) (left, right string, err error) {
	dotIndex := strings.Index(format, ".")
	if dotIndex == -1 {
		return "", "", fmt.Errorf("delimiter format must contain a '.' (e.g., '[[.]]')")
	}
	left = format[:dotIndex]
	right = format[dotIndex+1:]
	if left == "" || right == "" {
		return "", "", fmt.Errorf("both left and right delimiters must be specified")
	}
	return left, right, nil
}

func main() {
	if err := flags.Parse(&opts); err != nil {
		panic(err)
	}
	if err := logging.Init(opts.Logging); err != nil {
		panic(err)
	}
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	if opts.Output == "" {
		return fmt.Errorf("--output is required")
	}
	if len(opts.Templates) == 0 {
		fmt.Errorf("--template is required")
	}

	// Parse delimiters
	leftDelim, rightDelim, err := parseDelims(opts.Delims)
	if err != nil {
		return fmt.Errorf("invalid delimiter format: %w", err)
	}

	// Read the template file
	funcMap := sprig.TxtFuncMap()
	for k, v := range customFuncMap {
		funcMap[k] = v
	}
	// Parse the template
	tmpl := template.New(opts.Templates[0]).Funcs(funcMap).Delims(leftDelim, rightDelim)
	for _, templatePath := range opts.Templates {
		bytes, err := os.ReadFile(templatePath)
		if err != nil {
			return fmt.Errorf("reading template file: %w", err)
		}
		tmpl, err = tmpl.Parse(string(bytes))
		if err != nil {
			return fmt.Errorf("parsing template: %w", err)
		}
	}

	// Read the data file
	data := map[string]any{}
	if opts.Data != "" {
		dataBytes, err := os.ReadFile(opts.Data)
		if err != nil {
			return fmt.Errorf("reading data file: %v", err)
		}
		fixedDataBytes := bytes.ReplaceAll(dataBytes, []byte("True"), []byte("true"))
		fixedDataBytes = bytes.ReplaceAll(fixedDataBytes, []byte("False"), []byte("false"))

		// Unmarshal the data into a map
		switch opts.DataFormat {
		case "json":
			if err := json.Unmarshal(fixedDataBytes, &data); err != nil {
				return fmt.Errorf("unmarshaling json data: %v", err)
			}
		case "yaml":
			if err := yaml.Unmarshal(fixedDataBytes, &data); err != nil {
				return fmt.Errorf("unmarshaling yaml data: %v", err)
			}
		default:
			return fmt.Errorf("unknown data format: %s", opts.DataFormat)
		}
	}

	// Process additional data.
	extraData := map[string]string{}
	if len(opts.ExtraData) > 0 {
		data["extra"] = extraData
	}
	for _, extra := range opts.ExtraData {
		split := strings.Split(extra, ":")
		if len(split) != 2 {
			return fmt.Errorf("invalid extra data: %s", extra)
		}
		extraData[split[0]] = split[1]
	}

	// Execute the template with the data
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("executing template: %v", err)
	}

	// Inject Go imports if any were collected
	finalContent := injectGoImports(buf.Bytes())

	// Write the result to the output file
	if err := os.WriteFile(opts.Output, finalContent, 0644); err != nil {
		return fmt.Errorf("writing output file: %v", err)
	}
	return nil
}

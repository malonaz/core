package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/malonaz/core/go/flags"
	"github.com/malonaz/core/go/logging"
	"gopkg.in/yaml.v3"
)

var (
	log = logging.NewPrettyLogger()
)

var opts struct {
	Templates  []string `long:"template" description:"The template files to use" required:"true"`
	Data       string   `long:"data" description:"The data file to use"`
	DataFormat string   `long:"data-format" description:"The data format to use (json or yaml)" default:"json"`
	Output     string   `long:"output" short:"o" description:"The output file to create" required:"true"`
	Delims     string   `long:"delims" description:"Template delimiters format (e.g., '[[.]]' or '{{.}}')" default:"{{.}}"`
	ExtraData  []string `long:"extra-data" description:"Extra data to pass in the format: key:value"`
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
	flags.MustParse(&opts)
	if opts.Output == "" {
		log.Fatal("--output is required")
	}
	if len(opts.Templates) == 0 {
		log.Fatal("--output is required")
	}

	// Parse delimiters
	leftDelim, rightDelim, err := parseDelims(opts.Delims)
	if err != nil {
		log.Fatalf("invalid delimiter format: %v", err)
	}

	// Use to do operations once and only once.
	cache := map[string]bool{}
	doOnce := func(key string) bool {
		if _, ok := cache[key]; ok {
			return false // Already done.
		}
		cache[key] = true
		return true
	}

	// Read the template file
	funcMap := sprig.TxtFuncMap()
	funcMap["doOnce"] = doOnce
	for k, v := range customFuncMap {
		funcMap[k] = v
	}
	// Parse the template
	tmpl := template.New("template").Funcs(funcMap).Delims(leftDelim, rightDelim)
	for _, templatePath := range opts.Templates {
		bytes, err := os.ReadFile(templatePath)
		if err != nil {
			log.Fatalf("reading template file: %v", err)
		}
		tmpl, err = tmpl.Parse(string(bytes))
		if err != nil {
			log.Fatalf("parsing template: %v", err)
		}
	}

	// Read the data file
	data := map[string]any{}
	if opts.Data != "" {
		dataBytes, err := os.ReadFile(opts.Data)
		if err != nil {
			log.Fatalf("reading data file: %v", err)
		}
		fixedDataBytes := bytes.ReplaceAll(dataBytes, []byte("True"), []byte("true"))
		fixedDataBytes = bytes.ReplaceAll(fixedDataBytes, []byte("False"), []byte("false"))

		// Unmarshal the data into a map
		switch opts.DataFormat {
		case "json":
			if err := json.Unmarshal(fixedDataBytes, &data); err != nil {
				log.Fatalf("unmarshaling json data: %v", err)
			}
		case "yaml":
			if err := yaml.Unmarshal(fixedDataBytes, &data); err != nil {
				log.Fatalf("unmarshaling yaml data: %v", err)
			}
		default:
			log.Fatalf("unknown data format: %s", opts.DataFormat)
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
			log.Fatalf("invalid extra data: %s", extra)
		}
		extraData[split[0]] = split[1]
	}

	// Execute the template with the data
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		log.Fatalf("executing template: %v", err)
	}
	// Write the result to the output file
	if err := os.WriteFile(opts.Output, buf.Bytes(), 0644); err != nil {
		log.Fatalf("writing output file: %v", err)
	}
	log.Printf("Successfully processed template and data")
}

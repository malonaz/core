package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/pluginpb"

	_ "github.com/malonaz/core/genproto/codegen/aip"
	_ "github.com/malonaz/core/genproto/codegen/gateway"
	_ "github.com/malonaz/core/genproto/codegen/model"
	_ "github.com/malonaz/core/genproto/codegen/rpc"
)

var (
	opts struct {
		Debug         *bool
		Template      *string
		Configuration *string
	}
)

type Input struct {
	File          *protogen.File
	Files         []*protogen.File
	GeneratedFile *protogen.GeneratedFile
	Configuration map[any]any
}

func main() {
	var flags flag.FlagSet
	opts.Debug = flags.Bool("debug", false, "verbose output")
	opts.Template = flags.String("template", "", "template file to compile")
	opts.Configuration = flags.String("configuration", "", "configuration to inject in context")
	options := protogen.Options{
		ParamFunc: flags.Set,
	}
	options.Run(func(gen *protogen.Plugin) error {
		*opts.Debug = false
		if *opts.Template == "" {
			return fmt.Errorf("template parameter is required")
		}

		var configuration map[any]any
		if *opts.Configuration != "" {
			configData, err := os.ReadFile(*opts.Configuration)
			if err != nil {
				return fmt.Errorf("reading configuration file: %w", err)
			}

			if err := json.Unmarshal(configData, &configuration); err != nil {
				return fmt.Errorf("parsing configuration file: %w", err)
			}
		}

		// Read template content (but don't parse yet)
		templateContent, err := readTemplateContent(*opts.Template)
		if err != nil {
			return fmt.Errorf("reading template %s: %w", *opts.Template, err)
		}

		// Get template name for output filename
		templateFilename := filepath.Base(*opts.Template)
		templateFilenameWithoutExtension := strings.TrimSuffix(templateFilename, filepath.Ext(templateFilename))

		// Let's grab other files.
		otherFiles := []*protogen.File{}
		for _, f := range gen.Files {
			if !f.Generate {
				otherFiles = append(otherFiles, f)
			}
		}

		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}
			generatedFilename := fmt.Sprintf(
				"%s_%s.pb.go", f.GeneratedFilenamePrefix, templateFilenameWithoutExtension,
			)
			generatedFile := gen.NewGeneratedFile(generatedFilename, "")
			scopedExecution := newScopedExecution(generatedFile)

			// Create template with custom functions first, then parse
			tmpl, err := template.New(templateFilename).
				Funcs(scopedExecution.FuncMap()).
				Parse(templateContent)
			if err != nil {
				return fmt.Errorf("parsing template with functions: %w", err)
			}

			input := &Input{
				File:          f,
				Files:         otherFiles,
				GeneratedFile: generatedFile,
				Configuration: configuration,
			}
			if err := tmpl.Execute(generatedFile, input); err != nil {
				return fmt.Errorf("executing template: %w", err)
			}
		}
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
		return nil
	})
}

func readTemplateContent(templatePath string) (string, error) {
	// Check if file exists
	if _, err := os.Stat(templatePath); os.IsNotExist(err) {
		return "", fmt.Errorf("template file does not exist: %s", templatePath)
	}

	// Read the template content from the file system
	templateContent, err := os.ReadFile(templatePath)
	if err != nil {
		return "", fmt.Errorf("reading template file: %w", err)
	}

	return string(templateContent), nil
}

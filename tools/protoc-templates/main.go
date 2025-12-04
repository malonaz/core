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

	_ "github.com/malonaz/core/genproto/codegen/aip/v1"
	_ "github.com/malonaz/core/genproto/codegen/gateway/v1"
	_ "github.com/malonaz/core/genproto/codegen/llm/v1"
	_ "github.com/malonaz/core/genproto/codegen/model/v1"
	_ "google.golang.org/genproto/googleapis/api/annotations"
)

var (
	opts struct {
		Debug                   *bool
		Templates               *[]string
		Configuration           *string
		ImportPath              *string
		PackageName             *string
		AdditionalGoImportPaths *[]string
	}
)

type Input struct {
	File                    *protogen.File
	Files                   []*protogen.File
	GeneratedFile           *protogen.GeneratedFile
	Configuration           map[any]any
	PackageName             protogen.GoPackageName
	AdditionalGoImportPaths map[string]protogen.GoImportPath
}

func main() {
	var flags flag.FlagSet
	opts.Debug = flags.Bool("debug", false, "verbose output")
	templates := []string{}
	opts.Templates = &templates
	flags.Func("template", "template file paths (can be specified multiple times)", func(s string) error {
		*opts.Templates = append(*opts.Templates, s)
		return nil
	})
	opts.Configuration = flags.String("configuration", "", "configuration to inject in context")
	opts.ImportPath = flags.String("import-path", "", "Override the import path of the generated code")
	opts.PackageName = flags.String("package-name", "", "Override the package name of the generated code")
	data := []string{}
	opts.AdditionalGoImportPaths = &data
	flags.Func("additional-go-import-paths", "Additional GoImportPaths in the form of k:v", func(s string) error {
		*opts.AdditionalGoImportPaths = append(*opts.AdditionalGoImportPaths, s)
		return nil
	})
	options := protogen.Options{
		ParamFunc: flags.Set,
	}

	options.Run(func(gen *protogen.Plugin) error {
		*opts.Debug = false
		if len(*opts.Templates) == 0 {
			return fmt.Errorf("at least one template parameter is required")
		}
		keyToGoImportPath := map[string]protogen.GoImportPath{}
		if len(*opts.AdditionalGoImportPaths) > 0 {
			for _, data := range *opts.AdditionalGoImportPaths {
				split := strings.Split(data, ":")
				if len(split) != 2 {
					return fmt.Errorf("invalid data argument: %s", data)
				}
				keyToGoImportPath[split[0]] = protogen.GoImportPath(split[1])
			}
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

		if err := registerAnnotations(gen.Files); err != nil {
			return fmt.Errorf("registering annotations: %v", err)
		}
		if err := registerAncestors(gen.Files); err != nil {
			return fmt.Errorf("registering ancestors: %v", err)
		}

		// Collect other files
		otherFiles := []*protogen.File{}
		for _, f := range gen.Files {
			if !f.Generate {
				otherFiles = append(otherFiles, f)
			}
		}

		// Process each template
		for _, templatePath := range *opts.Templates {
			// Read template content
			templateContent, err := readTemplateContent(templatePath)
			if err != nil {
				return fmt.Errorf("reading template %s: %w", templatePath, err)
			}

			// Get template name for output filename
			templateFilename := filepath.Base(templatePath)
			templateFilenameWithoutExtension := strings.TrimSuffix(templateFilename, filepath.Ext(templateFilename))

			// Process each file with this template
			for _, f := range gen.Files {
				if !f.Generate {
					continue
				}

				generatedFilename := fmt.Sprintf(
					"%s_%s.pb.go", f.GeneratedFilenamePrefix, templateFilenameWithoutExtension,
				)
				goImportPath := f.GoImportPath
				if *opts.ImportPath != "" {
					goImportPath = protogen.GoImportPath(*opts.ImportPath)
				}
				generatedFile := gen.NewGeneratedFile(generatedFilename, goImportPath)
				scopedExecution := newScopedExecution(generatedFile)
				funcMap := scopedExecution.FuncMap()

				// Create template with custom functions first, then parse
				tmpl, err := template.New(templateFilename).
					Funcs(funcMap).
					Parse(templateContent)
				if err != nil {
					return fmt.Errorf("parsing template %s with functions: %w", templateFilename, err)
				}

				packageName := f.GoPackageName
				if *opts.PackageName != "" {
					packageName = protogen.GoPackageName(*opts.PackageName)
				}
				input := &Input{
					File:                    f,
					Files:                   otherFiles,
					GeneratedFile:           generatedFile,
					Configuration:           configuration,
					PackageName:             packageName,
					AdditionalGoImportPaths: keyToGoImportPath,
				}
				if err := tmpl.Execute(generatedFile, input); err != nil {
					return fmt.Errorf("executing template %s: %w", templateFilename, err)
				}
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

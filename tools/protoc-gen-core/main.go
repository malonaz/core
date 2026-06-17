package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/pluginpb"

	_ "github.com/malonaz/core/genproto/codegen/aip/v1"
	_ "github.com/malonaz/core/genproto/codegen/gateway/v1"
	_ "github.com/malonaz/core/genproto/codegen/model/v1"
	_ "github.com/malonaz/core/genproto/codegen/nats/v1"
	"github.com/malonaz/core/tools/protoc-gen-core/aip"
	"github.com/malonaz/core/tools/protoc-gen-core/gateway"
	"github.com/malonaz/core/tools/protoc-gen-core/model"
	natsevents "github.com/malonaz/core/tools/protoc-gen-core/nats/events"
	natsstreams "github.com/malonaz/core/tools/protoc-gen-core/nats/streams"
	"github.com/malonaz/core/tools/protoc-gen-core/plugin"
	"github.com/malonaz/core/tools/protoc-gen-core/postgres"
	"github.com/malonaz/core/tools/protoc-gen-core/resource"
	"github.com/malonaz/core/tools/protoc-gen-core/rpc"
	_ "google.golang.org/genproto/googleapis/api/annotations"
)

var (
	opts struct {
		Debug                   *bool
		Plugin                  *string
		Configuration           *string
		ImportPath              *string
		PackageName             *string
		GoImportPath            *string
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
	opts.Plugin = flags.String("plugin", "", "the plugin to run")
	opts.Configuration = flags.String("configuration", "", "configuration to inject in context")
	opts.GoImportPath = flags.String("go-import-path", "", "The plz go plugin import path")
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
		if *opts.Plugin == "" {
			return fmt.Errorf("must specify plugin")
		}
		keyToGoImportPath := map[string]protogen.GoImportPath{}
		if len(*opts.AdditionalGoImportPaths) > 0 {
			for _, data := range *opts.AdditionalGoImportPaths {
				split := strings.Split(data, ":")
				if len(split) != 2 {
					return fmt.Errorf("invalid data argument: %s", data)
				}
				importPath := split[1]
				if *opts.GoImportPath != "" {
					importPath = *opts.GoImportPath + "/" + importPath
				}
				keyToGoImportPath[split[0]] = protogen.GoImportPath(importPath)
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

		if err := resource.RegisterAnnotations(gen.Files); err != nil {
			return fmt.Errorf("registering annotations: %v", err)
		}
		if err := resource.RegisterAncestors(gen.Files); err != nil {
			return fmt.Errorf("registering ancestors: %v", err)
		}

		otherFiles := []*protogen.File{}
		for _, f := range gen.Files {
			if !f.Generate {
				otherFiles = append(otherFiles, f)
			}
		}

		postgres.ResetState()

		pluginOpts := &plugin.Opts{
			Configuration:           configuration,
			AdditionalGoImportPaths: keyToGoImportPath,
		}
		var generateFunc plugin.GenerateFunc
		switch *opts.Plugin {
		case "model":
			generateFunc = model.Generate
		case "postgres":
			generateFunc = postgres.Generate
		case "gateway_handler":
			generateFunc = gateway.Generate
		case "aip_label":
			generateFunc = aip.Generate
		case "nats_event":
			generateFunc = natsevents.Generate
		case "nats":
			generateFunc = natsstreams.Generate
		case "rpc":
			generateFunc = rpc.Generate
		default:
			return fmt.Errorf("unknown plugin %q", *opts.Plugin)
		}

		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}
			generatedFilename := fmt.Sprintf(
				"%s_%s.pb.go", f.GeneratedFilenamePrefix, *opts.Plugin,
			)
			goImportPath := f.GoImportPath
			if *opts.ImportPath != "" {
				goImportPath = protogen.GoImportPath(*opts.ImportPath)
			}
			packageName := f.GoPackageName
			if *opts.PackageName != "" {
				packageName = protogen.GoPackageName(*opts.PackageName)
			}
			generatedFile := gen.NewGeneratedFile(generatedFilename, goImportPath)

			if err := generateFunc(f, generatedFile, packageName, pluginOpts); err != nil {
				return fmt.Errorf("generating %s for %s: %w", *opts.Plugin, f.Desc.Path(), err)
			}
		}

		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
		return nil
	})
}

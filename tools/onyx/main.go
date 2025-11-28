package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/malonaz/core/go/flags"
	"github.com/malonaz/core/go/logging"
	"github.com/malonaz/core/tools/onyx/buildify"
	"github.com/malonaz/core/tools/onyx/gen"
	"github.com/malonaz/core/tools/onyx/parse"
	"github.com/malonaz/core/tools/onyx/template"
	"github.com/malonaz/core/tools/onyx/write"
)

var opts struct {
	Logging  *logging.Opts
	Parse    *parse.Opts
	Buildify *buildify.Opts
}

var log *slog.Logger

func main() {
	if len(os.Args) < 2 {
		panic(fmt.Errorf("usage: onyx <command> [args...]\ncommands: buildify, gen"))
	}
	command := os.Args[1]
	args := append(os.Args[:1], os.Args[2:]...)

	if err := flags.ParseArgs(&opts, args); err != nil {
		panic(err)
	}
	if err := logging.Init(opts.Logging); err != nil {
		panic(err)
	}
	log = slog.Default()
	if err := run(command); err != nil {
		panic(err)
	}
}

func run(command string) error {
	// Parse messages.
	manifests, err := parse.Parse(opts.Parse)
	if err != nil {
		return fmt.Errorf("parsing manifests: %w", err)
	}

	// Instantiate template engine.
	templateEngine, err := template.NewEngine()
	if err != nil {
		return fmt.Errorf("instantiating template engine: %v", err)
	}

	switch command {
	case "buildify":
		// Create buildify instance
		request := &buildify.Request{
			Manifests: manifests,
		}
		response, err := buildify.New(opts.Buildify, templateEngine).Buildify(request)
		if err != nil {
			return fmt.Errorf("buildify: %w", err)
		}
		// Write generated files
		if err := write.WriteFiles(opts.Parse, response.GeneratedFiles); err != nil {
			return fmt.Errorf("failed to write files: %w", err)
		}
		log.Info("Successfully generated files", "count", len(response.GeneratedFiles))
	case "gen":
		return gen.Run()
	default:
		return fmt.Errorf("unknown command: %s\ncommands: buildify, gen", command)
	}

	return nil
}

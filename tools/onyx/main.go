// Onyx generates the Go wiring of services and binaries from their manifests.
package main

import (
	"fmt"
	"os"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
	"github.com/malonaz/core/go/flags"
	"github.com/malonaz/core/tools/onyx/binary"
	"github.com/malonaz/core/tools/onyx/manifest"
	"github.com/malonaz/core/tools/onyx/service"
)

var opts struct {
	Mode         string `long:"mode" description:"service | main" required:"true"`
	Manifest     string `long:"manifest" description:"The manifest to generate from." required:"true"`
	Output       string `long:"output" description:"The Go file to write." required:"true"`
	GoImportPath string `long:"go-import-path" description:"Root import path of the repository's Go packages."`
}

func main() {
	if err := flags.Parse(&opts); err != nil {
		os.Exit(1)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "onyx:", err)
		os.Exit(1)
	}
}

func run() error {
	var source []byte
	switch opts.Mode {
	case "service":
		m := &onyxpb.ServiceManifest{}
		if err := manifest.Load(opts.Manifest, m); err != nil {
			return err
		}
		var err error
		if source, err = service.Generate(m, opts.GoImportPath); err != nil {
			return err
		}
	case "main":
		m := &onyxpb.MainManifest{}
		if err := manifest.Load(opts.Manifest, m); err != nil {
			return err
		}
		b, err := binary.Load(m, opts.GoImportPath)
		if err != nil {
			return err
		}
		if source, err = binary.Generate(b); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown mode %q", opts.Mode)
	}
	return os.WriteFile(opts.Output, source, 0o644)
}

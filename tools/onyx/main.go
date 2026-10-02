// Onyx generates the Go wiring of services and binaries from their manifests.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
	"github.com/malonaz/core/go/flags"
	"github.com/malonaz/core/tools/onyx/binary"
	"github.com/malonaz/core/tools/onyx/k8s"
	"github.com/malonaz/core/tools/onyx/manifest"
	"github.com/malonaz/core/tools/onyx/service"
)

var opts struct {
	Mode         string `long:"mode" description:"service | main | k8s" required:"true"`
	Manifest     string `long:"manifest" description:"The manifest to generate from." required:"true"`
	Output       string `long:"output" description:"The Go file to write; for k8s, the directory." required:"true"`
	GoImportPath string `long:"go-import-path" description:"Root import path of the repository's Go packages."`
	Image        string `long:"image" description:"For k8s, the image the Deployment runs. Defaults to the binary's name."`
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
	case "k8s":
		m := &onyxpb.MainManifest{}
		if err := manifest.Load(opts.Manifest, m); err != nil {
			return err
		}
		image := opts.Image
		if image == "" {
			image = m.GetName()
		}
		files, err := k8s.Generate(m, image)
		if err != nil {
			return err
		}
		for filename, data := range files {
			if err := os.WriteFile(filepath.Join(opts.Output, filename), data, 0o644); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown mode %q", opts.Mode)
	}
	return os.WriteFile(opts.Output, source, 0o644)
}

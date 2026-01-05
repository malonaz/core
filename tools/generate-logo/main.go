package main

import (
	"context"
	_ "embed"
	"fmt"
	"image/png"
	"log/slog"
	"os"
	"strings"

	"github.com/malonaz/core/go/flags"
	"github.com/malonaz/core/go/logging"
)

//go:embed Arial_Bold.ttf
var ttfBytes []byte

var opts struct {
	Logging *logging.Opts `group:"Logging" namespace:"logging" env-namespace:"LOGGING"`

	Format      string  `long:"format" description:"png or svg or favicon" required:"true"`
	Output      string  `long:"output" description:"output file path" required:"true"`
	AppName     string  `long:"app-name" description:"Application name" required:"true"`
	Prefix      string  `long:"prefix" description:"Prefix portion of the app name for coloring" required:"true"`
	PrefixColor string  `long:"prefix-color" description:"Hex color for prefix" default:"#FFFFFF"`
	ThemeColor  string  `long:"theme-color" description:"Hex color for suffix/theme" required:"true"`
	SvgOffset   int     `long:"svg-offset" description:"SVG text offset" default:"0"`
	SvgWidth    int     `long:"svg-width" description:"SVG viewBox width" default:"357"`
	Resolution  float64 `long:"resolution" description:"Resolution multiplier for PNG output" default:"1"`
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		slog.ErrorContext(ctx, "running", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	if err := flags.Parse(&opts); err != nil {
		return err
	}
	if err := logging.Init(opts.Logging); err != nil {
		return err
	}

	if opts.Format != "svg" && opts.Format != "png" && opts.Format != "favicon" {
		return fmt.Errorf("unsupported format: %v", opts.Format)
	}

	switch opts.Format {
	case "svg":
		svgData, err := generateSVG()
		if err != nil {
			return fmt.Errorf("generating SVG: %v", err)
		}
		if err := os.WriteFile(opts.Output, svgData, 0644); err != nil {
			return fmt.Errorf("saving SVG file: %v", err)
		}

	case "png":
		rgba, err := generatePNG()
		if err != nil {
			return fmt.Errorf("generating PNG: %v", err)
		}
		pngFile, err := os.Create(opts.Output)
		if err != nil {
			return fmt.Errorf("creating PNG file: %v", err)
		}
		defer pngFile.Close()
		if err := png.Encode(pngFile, rgba); err != nil {
			return fmt.Errorf("saving PNG file: %v", err)
		}

	case "favicon":
		baseFile := strings.TrimSuffix(opts.Output, ".png")
		for _, size := range []int{16, 32, 64, 128, 192, 384} {
			fn := generateFaviconPngShort
			if size > 128 {
				fn = generateFaviconPng
			}
			rgba, err := fn(size)
			if err != nil {
				return fmt.Errorf("generating favicon PNG: %v", err)
			}
			outputFile := fmt.Sprintf("%s-%dx%d.png", baseFile, size, size)
			pngFile, err := os.Create(outputFile)
			if err != nil {
				return fmt.Errorf("creating favicon PNG file: %v", err)
			}
			if err := png.Encode(pngFile, rgba); err != nil {
				pngFile.Close()
				return fmt.Errorf("saving favicon PNG file: %v", err)
			}
			pngFile.Close()
		}
	}
	return nil
}

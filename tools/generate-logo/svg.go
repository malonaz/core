package main

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ajstarks/svgo"
)

func generateSVG() ([]byte, error) {
	buf := new(bytes.Buffer)
	height := 73
	canvas := svg.New(buf)
	canvas.Startpercent(100, 100, fmt.Sprintf(`viewBox="0 0 %d %d"`, opts.SvgWidth, height))

	canvas.Textspan(opts.SvgOffset, 57, "", "font-family:Arial,sans-serif;font-weight:bold;font-size:80px")
	canvas.Span(opts.Prefix, "fill:white")
	suffix := strings.TrimPrefix(opts.AppName, opts.Prefix)
	canvas.Span(suffix)
	canvas.TextEnd()

	canvas.End()
	return buf.Bytes(), nil
}

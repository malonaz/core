package main

import (
	"fmt"
	"image"
	"image/draw"
	"strings"

	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"golang.org/x/image/font"
)

func generatePNG() (*image.RGBA, error) {
	baseHeight := 30
	height := int(float64(baseHeight) * opts.Resolution)

	f, err := truetype.Parse(ttfBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing font: %v", err)
	}

	trueTypeOpts := truetype.Options{Size: float64(height), DPI: 72}
	face := truetype.NewFace(f, &trueTypeOpts)
	width := font.MeasureString(face, opts.AppName).Ceil()

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.Draw(img, img.Bounds(), image.Transparent, image.Point{}, draw.Src)

	c := freetype.NewContext()
	c.SetDPI(72)
	c.SetFont(f)
	c.SetFontSize(float64(height))
	c.SetClip(img.Bounds())
	c.SetDst(img)
	c.SetHinting(font.HintingFull)

	rgbaColor, err := hexToRGBA(opts.PrefixColor)
	if err != nil {
		return nil, fmt.Errorf("invalid prefix color: %v", err)
	}
	c.SetSrc(image.NewUniform(rgbaColor))

	pt := freetype.Pt(0, int(float64(height)*0.8))
	advance, err := c.DrawString(opts.Prefix, pt)
	if err != nil {
		return nil, fmt.Errorf("drawing prefix: %v", err)
	}

	themeColor, err := getThemeColor()
	if err != nil {
		return nil, fmt.Errorf("getting theme color: %v", err)
	}
	c.SetSrc(themeColor)

	suffix := strings.TrimPrefix(opts.AppName, opts.Prefix)
	if _, err := c.DrawString(suffix, advance); err != nil {
		return nil, fmt.Errorf("drawing suffix: %v", err)
	}

	return img, nil
}

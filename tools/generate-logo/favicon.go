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

func generateFaviconPngShort(size int) (*image.RGBA, error) {
	prefix := string(opts.Prefix[0])
	suffix := strings.TrimPrefix(opts.AppName, opts.Prefix)
	secondLetter := string(suffix[0])
	text := strings.ToUpper(prefix + secondLetter)

	f, err := truetype.Parse(ttfBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing font: %v", err)
	}

	img := image.NewRGBA(image.Rect(0, 0, size, size))
	draw.Draw(img, img.Bounds(), image.Transparent, image.Point{}, draw.Src)

	themeColor, err := getThemeColor()
	if err != nil {
		return nil, fmt.Errorf("getting theme color: %v", err)
	}

	c := freetype.NewContext()
	c.SetDPI(72)
	c.SetFont(f)
	fontSize := float64(size) * 0.75
	c.SetFontSize(fontSize)
	c.SetClip(img.Bounds())
	c.SetDst(img)
	c.SetHinting(font.HintingFull)

	trueTypeOpts := truetype.Options{Size: fontSize, DPI: 72}
	face := truetype.NewFace(f, &trueTypeOpts)
	firstLetterWidth := font.MeasureString(face, prefix).Ceil()
	totalWidth := font.MeasureString(face, text).Ceil()

	overlap := firstLetterWidth / 4
	x := (size - (totalWidth - overlap)) / 2
	y := int(float64(size) * 0.8)

	c.SetSrc(themeColor)
	pt := freetype.Pt(x+firstLetterWidth-overlap, y)
	if _, err := c.DrawString(strings.ToUpper(secondLetter), pt); err != nil {
		return nil, fmt.Errorf("drawing second letter: %v", err)
	}

	c.SetSrc(image.White)
	pt = freetype.Pt(x, y)
	if _, err := c.DrawString(prefix, pt); err != nil {
		return nil, fmt.Errorf("drawing first letter: %v", err)
	}

	return img, nil
}

func generateFaviconPng(size int) (*image.RGBA, error) {
	f, err := truetype.Parse(ttfBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing font: %v", err)
	}

	img := image.NewRGBA(image.Rect(0, 0, size, size))
	draw.Draw(img, img.Bounds(), image.Transparent, image.Point{}, draw.Src)

	trueTypeOpts := truetype.Options{Size: float64(size) * 0.4, DPI: 72}
	face := truetype.NewFace(f, &trueTypeOpts)
	textWidth := font.MeasureString(face, opts.AppName).Ceil()

	fontSize := float64(size) * 0.4
	if textWidth > size {
		fontSize *= float64(size) / float64(textWidth)
	}

	c := freetype.NewContext()
	c.SetDPI(72)
	c.SetFont(f)
	c.SetFontSize(fontSize)
	c.SetClip(img.Bounds())
	c.SetDst(img)
	c.SetSrc(image.White)
	c.SetHinting(font.HintingFull)

	trueTypeOpts.Size = fontSize
	face = truetype.NewFace(f, &trueTypeOpts)
	textWidth = font.MeasureString(face, opts.AppName).Ceil()

	x := (size - textWidth) / 2
	y := int(float64(size) * 0.57)

	pt := freetype.Pt(x, y)
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

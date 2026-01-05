package main

import (
	"fmt"
	"image"
	"image/color"
	"strconv"
	"strings"
)

func hexToRGBA(hex string) (color.Color, error) {
	hex = strings.TrimPrefix(hex, "#")
	r, err := strconv.ParseUint(hex[0:2], 16, 8)
	if err != nil {
		return color.RGBA{}, fmt.Errorf("parsing r: %v", err)
	}
	g, err := strconv.ParseUint(hex[2:4], 16, 8)
	if err != nil {
		return color.RGBA{}, fmt.Errorf("parsing g: %v", err)
	}
	b, err := strconv.ParseUint(hex[4:6], 16, 8)
	if err != nil {
		return color.RGBA{}, fmt.Errorf("parsing b: %v", err)
	}
	return color.RGBA{R: uint8(r), G: uint8(g), B: uint8(b), A: 255}, nil
}

func getThemeColor() (image.Image, error) {
	rgbaColor, err := hexToRGBA(opts.ThemeColor)
	if err != nil {
		return nil, fmt.Errorf("hex to rgba: %v", err)
	}
	return image.NewUniform(rgbaColor), nil
}

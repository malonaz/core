package pretty

import (
	"fmt"
	"log/slog"
	"strconv"
)

const (
	DefaultTimeFormat = "2006-01-02 15:04:05.000"

	reset = "\033[0m"

	black   = 30
	red     = 31
	green   = 32
	yellow  = 33
	blue    = 34
	magenta = 35
	cyan    = 36

	darkGray = 90

	lightGray    = 37
	lightRed     = 91
	lightGreen   = 92
	lightYellow  = 93
	lightBlue    = 94
	lightMagenta = 95
	lightCyan    = 96

	white = 97
)

type Options struct {
	Level      slog.Level
	AddSource  bool
	Colorful   bool
	Multiline  bool
	TimeFormat string
}

func DefaultOptions() *Options {
	return &Options{
		Level:      slog.LevelInfo,
		Colorful:   true,
		TimeFormat: DefaultTimeFormat,
	}
}

func colorize(colorCode int, v string) string {
	return fmt.Sprintf("\033[%sm%s%s", strconv.Itoa(colorCode), v, reset)
}

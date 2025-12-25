package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/malonaz/core/go/logging/pretty"
)

const (
	// Level types
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"

	// Format types
	FormatJSON   = "json"
	FormatText   = "text"
	FormatRaw    = "raw"
	FormatPretty = "pretty"
)

// Opts holds logging configuration options.
type Opts struct {
	Fields []string `long:"field" env:"FIELD" description:"Inject fields at the topline level, using k:v"`
	Level  string   `long:"level" env:"LEVEL" description:"Log level: debug, info, warn, error" default:"info"`
	Format string   `long:"format" env:"FORMAT" description:"Log format: json, text, raw, pretty" default:"json"`
}

// Init initializes the default slog logger based on the provided options.
func Init(opts *Opts) error {
	logger, err := NewLogger(opts)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	return nil
}

func NewLogger(opts *Opts) (*slog.Logger, error) {
	handler, err := getHandler(opts)
	if err != nil {
		return nil, err
	}
	logger := slog.New(handler)
	for _, field := range opts.Fields {
		split := strings.Split(field, ":")
		if len(split) != 2 {
			return nil, fmt.Errorf("invalid field format: %s", field)
		}
		logger = logger.With(split[0], split[1])
	}
	return logger, nil
}

func getHandler(opts *Opts) (slog.Handler, error) {
	level := parseLevel(opts.Level)
	handlerOpts := &slog.HandlerOptions{
		Level: level,
	}
	var handler slog.Handler
	switch opts.Format {
	case FormatJSON:
		handler = slog.NewJSONHandler(os.Stderr, handlerOpts)
	case FormatText:
		handler = slog.NewTextHandler(os.Stderr, handlerOpts)
	case FormatRaw:
		handler = NewRawHandler(os.Stderr, handlerOpts)
	case FormatPretty:
		handler = pretty.New(os.Stdout, &pretty.Options{
			Level:     level,
			AddSource: true, // Show file location
			Colorful:  true, // Enable colors. Default is true
			Multiline: true, // Pretty print for complex data
			//TimeFormat: pretty.DefaultTimeFormat,    // Custom format (e.g., time.Kitchen)
		})
	default:
		return nil, fmt.Errorf("unrecognized format: %s", opts.Format)
	}
	return handler, nil
}

var levelToSlogLevel = map[string]slog.Level{
	LevelDebug: slog.LevelDebug,
	LevelInfo:  slog.LevelInfo,
	LevelWarn:  slog.LevelWarn,
	LevelError: slog.LevelError,
}

func parseLevel(level string) slog.Level {
	if l, ok := levelToSlogLevel[level]; ok {
		return l
	}
	return slog.LevelInfo
}

package flags

import (
	"fmt"
	"os"

	goflags "github.com/jessevdk/go-flags"
)

// Options represents parser options.
type Options = goflags.Options

// Option constants re-exported from go-flags.
const (
	// None indicates no options.
	None Options = goflags.None

	// HelpFlag adds a default Help Options group to the parser containing
	// -h and --help options.
	HelpFlag = goflags.HelpFlag

	// PassDoubleDash passes all arguments after a double dash, --, as
	// remaining command line arguments.
	PassDoubleDash = goflags.PassDoubleDash

	// IgnoreUnknown ignores any unknown options and passes them as
	// remaining command line arguments instead of generating an error.
	IgnoreUnknown = goflags.IgnoreUnknown

	// PrintErrors prints any errors which occurred during parsing to
	// os.Stderr.
	PrintErrors = goflags.PrintErrors

	// PassAfterNonOption passes all arguments after the first non option
	// as remaining command line arguments.
	PassAfterNonOption = goflags.PassAfterNonOption

	// AllowBoolValues allows a user to assign true/false to a boolean value.
	AllowBoolValues = goflags.AllowBoolValues

	// Default is a convenient default set of options.
	Default = goflags.Default
)

func Parse(opts any, options ...Options) error {
	return ParseArgs(opts, os.Args, options...)
}

// ParseArgs parses the given args into opts.
func ParseArgs(opts any, args []string, options ...Options) error {
	combinedOptions := None
	if len(options) == 0 {
		options = append(options, Default)
	}
	for _, option := range options {
		combinedOptions |= option
	}
	parser := goflags.NewParser(opts, combinedOptions)
	_, err := parser.ParseArgs(args)
	return err
}

// ParseWithDefaults parses os.Args into opts, first replacing the default of each flag in
// longNameToDefault, keyed by namespaced long name (`a-service-grpc.port`). Env still wins over them.
func ParseWithDefaults(opts any, longNameToDefault map[string]string, options ...Options) error {
	parser := goflags.NewParser(opts, combineOptions(options...))
	for longName, value := range longNameToDefault {
		option := parser.FindOptionByLongName(longName)
		if option == nil {
			return fmt.Errorf("no flag --%s to default", longName)
		}
		option.Default = []string{value}
	}
	_, err := parser.ParseArgs(os.Args)
	return err
}

// combineOptions combines multiple Options using bitwise OR.
// If no options are provided, returns Default.
func combineOptions(options ...Options) Options {
	if len(options) == 0 {
		return Default
	}

	var combined Options
	for _, opt := range options {
		combined |= opt
	}
	return combined
}

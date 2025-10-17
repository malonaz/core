package flags

import (
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
)

// MustParse parses os.Args and env into opts.
func MustParse(opts any) {
	MustParseArgs(opts, os.Args)
}

// MustParseArgs parses the given flags into opts.
func MustParseArgs(opts any, args []string) {
	if err := ParseArgs(opts, args); err != nil {
		panic("parsing flags: " + err.Error())
	}
}

// ParseArgs parses the given args into opts.
func ParseArgs(opts any, args []string) error {
	if _, err := flags.ParseArgs(opts, args); err != nil {
		return fmt.Errorf("parsing flags: %w", err)
	}
	return nil
}

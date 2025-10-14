package flags

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"github.com/jessevdk/go-flags"
	"github.com/malonaz/core/go/logging"
)

var log = logging.NewLogger()

// MustParse parses os.Args and env into opts.
func MustParse(opts any) {
	MustParseArgs(opts, os.Args)
}

// MustParseArgs parses the given flags into opts.
func MustParseArgs(opts any, args []string) {
	if err := ParseArgs(opts, args); err != nil {
		log.Panicf("parsing args: %v", err)
	}
}

// ParseArgs parses the given args into opts.
func ParseArgs(opts any, args []string) error {
	if err := parseSecrets(opts); err != nil {
		return fmt.Errorf("parsing secrets: %w", err)
	}
	if _, err := flags.ParseArgs(opts, args); err != nil {
		return fmt.Errorf("parsing flags: %w", err)
	}
	return nil
}

// Parses secrets into any field which uses the `secret` tag.
// TODO(malon): make recursion work...
func parseSecrets(obj any) error {
	v := reflect.Indirect(reflect.ValueOf(obj))
	t := reflect.TypeOf(obj)

	for i := 0; i < v.NumField(); i++ {
		field := t.Elem().Field(i)
		value := v.Field(i)
		if !field.IsExported() {
			continue
		}
		secretFilepath, ok := field.Tag.Lookup("secret")
		if !ok {
			if value.Kind() != reflect.Ptr {
				continue
			}
			value.Set(reflect.New(value.Type().Elem()))
			if err := parseSecrets(value.Interface()); err != nil {
				return fmt.Errorf(field.Name+": %w", err)
			}
			continue
		}
		bytes, err := os.ReadFile(secretFilepath)
		if err != nil {
			return fmt.Errorf("reading secret @%s: %w", secretFilepath, err)
		}
		var vaultSecret struct{ Data any }
		vaultSecret.Data = value.Addr().Interface()
		if err := json.Unmarshal(bytes, &vaultSecret); err != nil {
			return fmt.Errorf("unmarshaling secret @%s: %w", secretFilepath, err)
		}
	}
	return nil
}

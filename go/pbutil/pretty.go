package pbutil

import (
	"fmt"

	"github.com/tidwall/pretty"
	"google.golang.org/protobuf/proto"
)

var prettyOptions = &pretty.Options{
	Width:    120,
	Prefix:   "",
	Indent:   "  ",
	SortKeys: true,
}

// PrettyPrint pretty prints a proto message to the given writer with colored JSON output.
func MarshalPretty(m proto.Message) ([]byte, error) {
	jsonBytes, err := JSONMarshalPretty(m)
	if err != nil {
		return nil, fmt.Errorf("marshaling proto to JSON: %w", err)
	}

	// Pretty print with colors and indentation
	output := pretty.Color(pretty.Pretty(jsonBytes), pretty.TerminalStyle)
	return output, err
}

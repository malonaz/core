package ai

import (
	"bytes"
	"encoding/json"

	"google.golang.org/protobuf/proto"

	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/pbutil"
)

// JSONMarshal renders m as JSON for a model's input. protojson's whitespace changes from one
// binary to the next, and providers bind thinking signatures and prompt caches to exact bytes, so
// the output is compacted to be the same on every build.
func JSONMarshal(m proto.Message) ([]byte, error) {
	b, err := pbutil.JSONMarshal(m)
	if err != nil {
		return nil, err
	}
	return compact(b)
}

// JSONSchemaMarshal renders a tool's schema for a provider, its keywords spelled as JSON Schema
// spells them (`additionalProperties`, `maxLength`): a provider ignores the proto's field names,
// and the model then never sees what a map holds or what bounds a value.
func JSONSchemaMarshal(schema *jsonpb.Schema) ([]byte, error) {
	b, err := pbutil.JSONCamelCaseMarshal(schema)
	if err != nil {
		return nil, err
	}
	return compact(b)
}

func compact(b []byte) ([]byte, error) {
	var buffer bytes.Buffer
	if err := json.Compact(&buffer, b); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

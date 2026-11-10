package ai

import (
	"bytes"
	"encoding/json"

	"google.golang.org/protobuf/proto"

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
	var buffer bytes.Buffer
	if err := json.Compact(&buffer, b); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

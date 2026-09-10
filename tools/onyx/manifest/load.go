package manifest

import (
	"encoding/json"
	"fmt"
	"os"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Load reads a YAML manifest into the message and validates it.
func Load[T proto.Message](filename string, message T) error {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	var data any
	if err := yaml.Unmarshal(bytes, &data); err != nil {
		return fmt.Errorf("%s: %w", filename, err)
	}
	// Through JSON so protojson enforces the schema: unknown fields are errors.
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("%s: %w", filename, err)
	}
	if err := protojson.Unmarshal(jsonBytes, message); err != nil {
		return fmt.Errorf("%s: %w", filename, err)
	}
	if err := protovalidate.Validate(message); err != nil {
		return fmt.Errorf("%s: %w", filename, err)
	}
	return nil
}

// LoadLabel loads the manifest a `//pkg:name(file)` label names.
func LoadLabel[T proto.Message](label string, message T) error {
	parsed, err := ParseLabel(label)
	if err != nil {
		return err
	}
	filename, err := parsed.File()
	if err != nil {
		return err
	}
	return Load(filename, message)
}

package authentication

import (
	"fmt"

	"buf.build/go/protovalidate"
	"github.com/malonaz/core/go/jsonnet"
	"github.com/malonaz/core/go/pbutil"
	"google.golang.org/protobuf/proto"
)

var (
	validator = func() protovalidate.Validator {
		validator, err := protovalidate.New()
		if err != nil {
			panic(fmt.Errorf("instantiating proto validator: %w", err))
		}
		return validator
	}()
)

func parseConfig(path string, config proto.Message) error {
	bytes, err := jsonnet.EvaluateFile(path)
	if err != nil {
		return fmt.Errorf("failed to evaluate config file %s: %w", path, err)
	}
	if err := pbutil.JSONUnmarshalStrict(bytes, config); err != nil {
		return fmt.Errorf("failed to parse config file %s: %w", path, err)
	}
	if err := validator.Validate(config); err != nil {
		return fmt.Errorf("validating config: %v", err)
	}
	return nil
}

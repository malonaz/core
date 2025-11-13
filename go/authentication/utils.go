package authentication

import (
	"fmt"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/jsonnet"
	"github.com/malonaz/core/go/pbutil"
	"os"
)

func parseAuthenticationConfig(path string) (*authenticationpb.Configuration, error) {
	// Parse the configuration file
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}
	bytes, err = jsonnet.EvaluateSnippet(string(bytes))
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate config file %s: %w", path, err)
	}
	configuration := &authenticationpb.Configuration{}
	if err := pbutil.JSONUnmarshal(bytes, configuration); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}
	return configuration, nil
}

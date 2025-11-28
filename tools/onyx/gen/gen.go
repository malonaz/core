package gen

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
	"github.com/malonaz/core/go/flags"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"
)

var opts struct {
	Input      string `long:"input" description:"The onyx manifest file to generate code from" required:"true"`
	WorkingDir string `long:"working-dir" description:"The working directory to use as root for file resolution (defaults to current directory)"`
}

var log *slog.Logger

// Run executes the gen command
func Run() error {
	if err := flags.Parse(&opts); err != nil {
		return err
	}
	log = slog.Default()
	return run()
}

// kindWrapper is used to determine the type before unmarshaling
type kindWrapper struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
}

func run() error {
	// Set working directory if specified
	workingDir := opts.WorkingDir
	if workingDir == "" {
		var err error
		workingDir, err = os.Getwd()
		if err != nil {
			return fmt.Errorf("failed to get current directory: %w", err)
		}
	}

	// Resolve input file path relative to working directory
	inputPath := opts.Input
	if !filepath.IsAbs(inputPath) {
		inputPath = filepath.Join(workingDir, inputPath)
	}

	// Read the input file
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("failed to read input file: %w", err)
	}

	// First pass: determine the kind
	var wrapper kindWrapper
	if err := yaml.Unmarshal(data, &wrapper); err != nil {
		return fmt.Errorf("failed to unmarshal kind wrapper: %w", err)
	}

	// Switch on kind to determine which proto message to unmarshal into
	switch wrapper.Kind {
	case "Service":
		return handleService(data)
	default:
		return fmt.Errorf("unsupported kind: %s", wrapper.Kind)
	}
}

func handleService(data []byte) error {
	// Convert YAML to JSON (protojson expects JSON)
	var yamlData interface{}
	if err := yaml.Unmarshal(data, &yamlData); err != nil {
		return fmt.Errorf("failed to unmarshal yaml: %w", err)
	}

	jsonData, err := json.Marshal(yamlData)
	if err != nil {
		return fmt.Errorf("failed to convert to json: %w", err)
	}

	// Unmarshal into Service proto
	service := &onyxpb.Service{}
	if err := protojson.Unmarshal(jsonData, service); err != nil {
		return fmt.Errorf("failed to unmarshal service: %w", err)
	}

	// TODO: Process the service and generate code
	log.Info("Successfully parsed Service", "name", service.Metadata.Name)

	return nil
}

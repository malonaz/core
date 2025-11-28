package parse

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	onyxpb "github.com/malonaz/core/proto/onyx/v1"
	"github.com/malonaz/core/tools/onyx/types"
)

type Opts struct {
	WorkingDir string   `long:"working-dir" description:"The working directory to use as root for file resolution" default:"./"`
	Input      []string `long:"input" description:"The onyx manifest to parse. If unset, we traverse working directory"`
	Ignore     []string `long:"ignore" description:"ignore file using .gitignore style"`
}

// kindWrapper is used to determine the type before unmarshaling
type kindWrapper struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
}

func Parse(opts *Opts) ([]*types.Manifest, error) {
	var manifests []*types.Manifest

	// If no inputs specified, traverse working directory
	if len(opts.Input) == 0 {
		onyxFiles, err := findOnyxFiles(opts, opts.WorkingDir)
		if err != nil {
			return nil, fmt.Errorf("failed to find onyx files: %w", err)
		}

		if len(onyxFiles) == 0 {
			return nil, fmt.Errorf("no .onyx files found in %s", opts.WorkingDir)
		}

		opts.Input = onyxFiles
	}

	// Parse each input file
	for _, input := range opts.Input {
		message, err := ParseFile(opts, input)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", input, err)
		}
		manifest := &types.Manifest{
			Filepath: input,
			Message:  message,
		}
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}

// findOnyxFiles recursively traverses the directory and returns all .onyx files
// respecting .gitignore patterns
func findOnyxFiles(opts *Opts, rootDir string) ([]string, error) {
	// Load .gitignore patterns if it exists
	gitignorePath := filepath.Join(rootDir, ".gitignore")
	patterns, err := loadGitignorePatterns(gitignorePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load .gitignore: %w", err)
	}
	patterns = append(patterns, ".git")
	for _, ignore := range opts.Ignore {
		patterns = append(patterns, ignore)
	}

	var onyxFiles []string
	err = filepath.WalkDir(rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip the root directory itself
		if path == rootDir {
			return nil
		}

		// Check if this path should be ignored
		if len(patterns) > 0 && shouldIgnore(path, rootDir, patterns) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Skip other directories
		if d.IsDir() {
			return nil
		}

		// Check if file has .onyx extension
		if filepath.Ext(path) == ".onyx" {
			onyxFiles = append(onyxFiles, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return onyxFiles, nil
}

// ParseFile parses an onyx manifest file and returns the appropriate proto message
func ParseFile(opts *Opts, inputPath string) (proto.Message, error) {
	// Resolve input file path relative to working directory
	if !filepath.IsAbs(inputPath) {
		inputPath = filepath.Join(opts.WorkingDir, inputPath)
	}

	// Read the input file
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read input file: %w", err)
	}

	// First pass: determine the kind
	var wrapper kindWrapper
	if err := yaml.Unmarshal(data, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to unmarshal kind wrapper: %w", err)
	}

	// Switch on kind to determine which proto message to unmarshal into
	switch wrapper.Kind {
	case "Service":
		return parseService(data)
	default:
		return nil, fmt.Errorf("unsupported kind: %s", wrapper.Kind)
	}
}

func parseService(data []byte) (*onyxpb.Service, error) {
	// Convert YAML to JSON (protojson expects JSON)
	var yamlData interface{}
	if err := yaml.Unmarshal(data, &yamlData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal yaml: %w", err)
	}

	jsonData, err := json.Marshal(yamlData)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to json: %w", err)
	}

	// Unmarshal into Service proto
	service := &onyxpb.Service{}
	if err := protojson.Unmarshal(jsonData, service); err != nil {
		return nil, fmt.Errorf("failed to unmarshal service: %w", err)
	}

	if service.Metadata == nil || service.Metadata.Name == "" {
		return nil, fmt.Errorf("service metadata.name is required")
	}

	return service, nil
}

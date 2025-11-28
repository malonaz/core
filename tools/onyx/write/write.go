package write

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/malonaz/core/tools/onyx/parse"
	"github.com/malonaz/core/tools/onyx/types"
)

// WriteFiles writes all generated files to disk
func WriteFiles(opts *parse.Opts, files []*types.GeneratedFile) error {
	for _, file := range files {
		if err := writeFile(opts, file); err != nil {
			return fmt.Errorf("failed to write %s: %w", file.Output, err)
		}
	}
	return nil
}

func writeFile(opts *parse.Opts, file *types.GeneratedFile) error {
	// Resolve output path relative to working directory
	outputPath := filepath.Join(opts.WorkingDir, file.Output)

	// Create parent directories if they don't exist
	outputDir := filepath.Dir(outputPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write the file
	if err := os.WriteFile(outputPath, file.Content, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

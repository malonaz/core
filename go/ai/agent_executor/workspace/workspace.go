// Package workspace provides coding tools operating on a local working directory.
package workspace

import (
	"path/filepath"

	"github.com/malonaz/core/go/ai/agent_executor"
)

// NewTools returns every workspace tool, operating on the given directory.
func NewTools(directory string) []agent_executor.Tool {
	return []agent_executor.Tool{
		NewExecShellTool(directory),
		NewReadFilesTool(directory),
		NewWriteFileTool(directory),
		NewEditFileTool(directory),
	}
}

// resolvePath resolves a path against the directory, unless absolute. It
// confines nothing: an agent holding a shell can reach anywhere anyway.
func resolvePath(directory, path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(directory, path)
}

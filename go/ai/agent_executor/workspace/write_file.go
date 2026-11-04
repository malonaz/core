package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/agent_executor"
)

const writeFileToolName = "write_file"

// WriteFileTool creates or overwrites files.
type WriteFileTool struct {
	directory string
}

// NewWriteFileTool returns a tool writing files, resolving relative paths
// against the directory.
func NewWriteFileTool(directory string) *WriteFileTool {
	return &WriteFileTool{directory: directory}
}

type writeFileArguments struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

func (t *WriteFileTool) Definition() *aipb.Tool {
	return &aipb.Tool{
		Name:        writeFileToolName,
		Description: "Create a file, or overwrite it whole, creating missing parent directories. Prefer edit_file to change part of an existing file.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"path": {
					Type:        "string",
					Description: "The file to write, absolute or relative to the workspace.",
				},
				"content": {
					Type:        "string",
					Description: "The file's full content.",
				},
			},
			Required: []string{"path", "content"},
		},
	}
}

func (t *WriteFileTool) Execute(_ context.Context, _ *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	arguments := &writeFileArguments{}
	if err := agent_executor.DecodeArguments(toolCall, arguments); err != nil {
		return nil, err
	}
	if arguments.Path == "" {
		return nil, errors.New("path is required")
	}
	path := resolvePath(t.directory, arguments.Path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	// An existing file keeps its mode.
	if err := os.WriteFile(path, []byte(arguments.Content), 0o644); err != nil {
		return nil, err
	}
	content := fmt.Sprintf("Wrote %d bytes to %s.", len(arguments.Content), arguments.Path)
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), content), nil
}

package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/agent_executor"
)

const editFileToolName = "edit_file"

// EditFileTool replaces exact text in files.
type EditFileTool struct {
	directory string
}

// NewEditFileTool returns a tool editing files, resolving relative paths
// against the directory.
func NewEditFileTool(directory string) *EditFileTool {
	return &EditFileTool{directory: directory}
}

type editFileArguments struct {
	Path       string `json:"path"`
	OldString  string `json:"old_string"`
	NewString  string `json:"new_string"`
	ReplaceAll bool   `json:"replace_all"`
}

func (t *EditFileTool) Definition() *aipb.Tool {
	return &aipb.Tool{
		Name: editFileToolName,
		Description: "Replace exact text in a file. old_string must match the file's content exactly, whitespace included, " +
			"and occur exactly once unless replace_all is set: include enough surrounding lines to make it unique.",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"path": {
					Type:        "string",
					Description: "The file to edit, absolute or relative to the workspace.",
				},
				"old_string": {
					Type:        "string",
					Description: "The exact text to replace.",
				},
				"new_string": {
					Type:        "string",
					Description: "The text to replace it with.",
				},
				"replace_all": {
					Type:        "boolean",
					Description: "Replace every occurrence of old_string instead of exactly one.",
				},
			},
			Required: []string{"path", "old_string", "new_string"},
		},
	}
}

func (t *EditFileTool) Execute(_ context.Context, _ *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	arguments := &editFileArguments{}
	if err := agent_executor.DecodeArguments(toolCall, arguments); err != nil {
		return nil, err
	}
	switch {
	case arguments.Path == "":
		return nil, errors.New("path is required")
	case arguments.OldString == "":
		return nil, errors.New("old_string is required: create files with write_file")
	case arguments.OldString == arguments.NewString:
		return nil, errors.New("old_string and new_string are identical")
	}
	path := resolvePath(t.directory, arguments.Path)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	count := strings.Count(string(content), arguments.OldString)
	switch {
	case count == 0:
		return nil, fmt.Errorf("old_string not found in %s", arguments.Path)
	case count > 1 && !arguments.ReplaceAll:
		return nil, fmt.Errorf("old_string occurs %d times in %s: include more context to make it unique, or set replace_all", count, arguments.Path)
	}
	edited := strings.ReplaceAll(string(content), arguments.OldString, arguments.NewString)
	if err := os.WriteFile(path, []byte(edited), info.Mode()); err != nil {
		return nil, err
	}
	result := fmt.Sprintf("Replaced %d occurrence(s) in %s.", count, arguments.Path)
	return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), result), nil
}

package workspace

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/agent_executor"
)

// execute calls a tool with the given arguments, returning its result content.
func execute(t *testing.T, ctx context.Context, tool agent_executor.Tool, arguments map[string]any) (string, error) {
	t.Helper()
	argumentsStruct, err := structpb.NewStruct(arguments)
	require.NoError(t, err)
	toolCall := &aipb.ToolCall{Id: "c1", Name: tool.Definition().GetName(), Arguments: argumentsStruct}
	toolResult, err := tool.Execute(ctx, nil, toolCall)
	if err != nil {
		return "", err
	}
	require.Equal(t, "c1", toolResult.GetToolCallId())
	return toolResult.GetContent(), nil
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(content)
}

func TestNewToolsHaveUniqueNames(t *testing.T) {
	toolNameSet := map[string]struct{}{}
	for _, tool := range NewTools(t.TempDir()) {
		definition := tool.Definition()
		require.NotEmpty(t, definition.GetDescription())
		require.Equal(t, "object", definition.GetJsonSchema().GetType())
		toolNameSet[definition.GetName()] = struct{}{}
	}
	require.Equal(t, map[string]struct{}{"exec_shell": {}, "read_files": {}, "write_file": {}, "edit_file": {}}, toolNameSet)
}

func TestResolvePath(t *testing.T) {
	require.Equal(t, "/work/a/b", resolvePath("/work", "a/b"))
	require.Equal(t, "/work", resolvePath("/work", ""))
	require.Equal(t, "/other", resolvePath("/work", "/other/./"))
	require.Equal(t, "/sibling", resolvePath("/work", "../sibling"))
}

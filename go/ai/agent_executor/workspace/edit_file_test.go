package workspace

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteFile(t *testing.T) {
	directory := t.TempDir()
	tool := NewWriteFileTool(directory)

	content, err := execute(t, context.Background(), tool, map[string]any{"path": "new/dir/file.txt", "content": "hello"})
	require.NoError(t, err)
	require.Equal(t, "Wrote 5 bytes to new/dir/file.txt.", content)
	require.Equal(t, "hello", readTestFile(t, filepath.Join(directory, "new", "dir", "file.txt")))

	// Overwriting keeps the file's mode.
	script := filepath.Join(directory, "script.sh")
	writeTestFile(t, script, "old")
	require.NoError(t, os.Chmod(script, 0o755))
	_, err = execute(t, context.Background(), tool, map[string]any{"path": script, "content": "new"})
	require.NoError(t, err)
	require.Equal(t, "new", readTestFile(t, script))
	info, err := os.Stat(script)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())

	_, err = execute(t, context.Background(), tool, map[string]any{"path": "", "content": "x"})
	require.Error(t, err)
}

func TestEditFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "file.go")
	writeTestFile(t, path, "a := 1\nb := 1\nc := 2\n")
	tool := NewEditFileTool(directory)

	content, err := execute(t, context.Background(), tool, map[string]any{"path": "file.go", "old_string": "c := 2", "new_string": "c := 3"})
	require.NoError(t, err)
	require.Equal(t, "Replaced 1 occurrence(s) in file.go.", content)
	require.Equal(t, "a := 1\nb := 1\nc := 3\n", readTestFile(t, path))

	_, err = execute(t, context.Background(), tool, map[string]any{"path": "file.go", "old_string": ":= 1", "new_string": ":= 0"})
	require.ErrorContains(t, err, "old_string occurs 2 times in file.go")
	require.Equal(t, "a := 1\nb := 1\nc := 3\n", readTestFile(t, path))

	content, err = execute(t, context.Background(), tool, map[string]any{"path": "file.go", "old_string": ":= 1", "new_string": ":= 0", "replace_all": true})
	require.NoError(t, err)
	require.Equal(t, "Replaced 2 occurrence(s) in file.go.", content)
	require.Equal(t, "a := 0\nb := 0\nc := 3\n", readTestFile(t, path))

	for name, arguments := range map[string]map[string]any{
		"not found":    {"path": "file.go", "old_string": "d := 4", "new_string": "d := 5"},
		"missing file": {"path": "missing.go", "old_string": "a", "new_string": "b"},
		"empty old":    {"path": "file.go", "old_string": "", "new_string": "b"},
		"identical":    {"path": "file.go", "old_string": "a", "new_string": "a"},
	} {
		_, err := execute(t, context.Background(), tool, arguments)
		require.Error(t, err, name)
	}
	require.Equal(t, "a := 0\nb := 0\nc := 3\n", readTestFile(t, path))
}

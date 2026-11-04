package workspace

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadFiles(t *testing.T) {
	directory := t.TempDir()
	writeTestFile(t, filepath.Join(directory, "a.txt"), "one\ntwo\n")
	writeTestFile(t, filepath.Join(directory, "b.txt"), "no newline")
	writeTestFile(t, filepath.Join(directory, "empty.txt"), "")
	writeTestFile(t, filepath.Join(directory, "binary"), "a\x00b")
	absolutePath := filepath.Join(directory, "b.txt")
	tool := NewReadFilesTool(directory)

	content, err := execute(t, context.Background(), tool, map[string]any{
		"paths": []any{"a.txt", absolutePath, "empty.txt", "missing.txt", "binary", "."},
	})
	require.NoError(t, err)
	want := "==> a.txt <==\none\ntwo\n" +
		"\n==> " + absolutePath + " <==\nno newline\n" +
		"\n==> empty.txt <==\n(empty file)\n" +
		"\n==> missing.txt <==\nError: stat " + filepath.Join(directory, "missing.txt") + ": no such file or directory\n" +
		"\n==> binary <==\nError: not a text file\n" +
		"\n==> . <==\nError: is a directory\n"
	require.Equal(t, want, content)

	_, err = execute(t, context.Background(), tool, map[string]any{"paths": []any{}})
	require.Error(t, err)
}

func TestReadFilesWindow(t *testing.T) {
	directory := t.TempDir()
	var lines []string
	for i := 1; i <= 2500; i++ {
		lines = append(lines, fmt.Sprintf("line %d", i))
	}
	writeTestFile(t, filepath.Join(directory, "long.txt"), strings.Join(lines, "\n")+"\n")
	tool := NewReadFilesTool(directory)

	content, err := execute(t, context.Background(), tool, map[string]any{"paths": []any{"long.txt"}})
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(content, "line 2000\n[lines 1-2000 of 2500: read on with offset 2001]\n"))

	content, err = execute(t, context.Background(), tool, map[string]any{"paths": []any{"long.txt"}, "offset": 2001})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(content, "==> long.txt <==\nline 2001\n"))
	require.True(t, strings.HasSuffix(content, "line 2500\n[lines 2001-2500 of 2500]\n"))

	content, err = execute(t, context.Background(), tool, map[string]any{"paths": []any{"long.txt"}, "offset": 10, "limit": 2})
	require.NoError(t, err)
	require.Equal(t, "==> long.txt <==\nline 10\nline 11\n[lines 10-11 of 2500: read on with offset 12]\n", content)

	content, err = execute(t, context.Background(), tool, map[string]any{"paths": []any{"long.txt"}, "offset": 2501})
	require.NoError(t, err)
	require.Equal(t, "==> long.txt <==\nError: offset 2501 is past the end of the file (2500 lines)\n", content)
}

func TestReadFilesCapsSize(t *testing.T) {
	directory := t.TempDir()
	line := strings.Repeat("x", 1023) + "\n"
	writeTestFile(t, filepath.Join(directory, "wide.txt"), strings.Repeat(line, 200))
	writeTestFile(t, filepath.Join(directory, "one-line.txt"), strings.Repeat("y", maxReadSize+10))
	writeTestFile(t, filepath.Join(directory, "huge.txt"), strings.Repeat("z", maxFileSize+1))
	tool := NewReadFilesTool(directory)

	content, err := execute(t, context.Background(), tool, map[string]any{"paths": []any{"wide.txt"}})
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(content, "[lines 1-128 of 200: read on with offset 129]\n"))

	content, err = execute(t, context.Background(), tool, map[string]any{"paths": []any{"one-line.txt"}})
	require.NoError(t, err)
	require.Equal(t, "==> one-line.txt <==\n"+strings.Repeat("y", maxReadSize)+"\n", content)

	content, err = execute(t, context.Background(), tool, map[string]any{"paths": []any{"huge.txt"}})
	require.NoError(t, err)
	require.Contains(t, content, "Error: 8388609 bytes, over the 8388608 byte limit")
}

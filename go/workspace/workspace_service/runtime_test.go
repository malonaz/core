package workspace_service

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// newTestService returns a service over a fresh workspace, and its directory.
func newTestService(t *testing.T) (*Service, string) {
	t.Helper()
	directory := t.TempDir()
	service, err := New(&Opts{Directory: directory})
	require.NoError(t, err)
	return service, directory
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

func TestNewResolvesDirectory(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	service, err := New(&Opts{Directory: "."})
	require.NoError(t, err)
	require.Equal(t, directory, service.directory)

	writeTestFile(t, filepath.Join(directory, "file"), "")
	for _, invalidDirectory := range []string{"missing", "file"} {
		_, err := New(&Opts{Directory: invalidDirectory})
		require.Error(t, err, invalidDirectory)
	}
}

func TestResolvePath(t *testing.T) {
	runtime := &runtime{directory: "/work"}
	require.Equal(t, "/work/a/b", runtime.resolvePath("a/b"))
	require.Equal(t, "/work", runtime.resolvePath(""))
	require.Equal(t, "/other", runtime.resolvePath("/other/./"))
	require.Equal(t, "/sibling", runtime.resolvePath("../sibling"))
}

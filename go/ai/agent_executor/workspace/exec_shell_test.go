package workspace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecShell(t *testing.T) {
	directory := t.TempDir()
	writeTestFile(t, filepath.Join(directory, "sub", "file.txt"), "hello")
	tool := NewExecShellTool(directory)

	content, err := execute(t, context.Background(), tool, map[string]any{"command": "cat sub/file.txt; echo ' world' >&2"})
	require.NoError(t, err)
	require.Equal(t, "Exit code: 0\nhello world\n", content)

	content, err = execute(t, context.Background(), tool, map[string]any{"command": "pwd; exit 3", "working_directory": "sub"})
	require.NoError(t, err)
	require.Equal(t, "Exit code: 3\n"+filepath.Join(directory, "sub")+"\n", content)

	_, err = execute(t, context.Background(), tool, map[string]any{"command": "true", "working_directory": "missing"})
	require.Error(t, err)
	_, err = execute(t, context.Background(), tool, map[string]any{"command": " "})
	require.Error(t, err)
}

func TestExecShellTimeoutKillsProcessGroup(t *testing.T) {
	directory := t.TempDir()
	tool := NewExecShellTool(directory)
	marker := filepath.Join(directory, "marker")
	// The background child would outlive a kill of bash alone.
	command := "(sleep 2; touch " + marker + ") & echo started; wait"
	start := time.Now()
	content, err := execute(t, context.Background(), tool, map[string]any{"command": command, "timeout_seconds": 1})
	require.NoError(t, err)
	require.Less(t, time.Since(start), 2*time.Second)
	require.Equal(t, "Timed out after 1s: killed.\nstarted\n", content)

	time.Sleep(1500 * time.Millisecond)
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "the background child survived the timeout")
}

func TestExecShellCancelled(t *testing.T) {
	tool := NewExecShellTool(t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := execute(t, ctx, tool, map[string]any{"command": "sleep 10"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 2*time.Second)
}

func TestExecShellBackgroundProcessDoesNotBlock(t *testing.T) {
	tool := NewExecShellTool(t.TempDir())
	start := time.Now()
	// The background sleep inherits the output pipe.
	content, err := execute(t, context.Background(), tool, map[string]any{"command": "sleep 10 & echo done"})
	require.NoError(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
	require.Equal(t, "Exit code: 0\ndone\n", content)
}

func TestExecShellTerminatedBySignal(t *testing.T) {
	tool := NewExecShellTool(t.TempDir())
	content, err := execute(t, context.Background(), tool, map[string]any{"command": "kill -TERM $$"})
	require.NoError(t, err)
	require.Equal(t, "Terminated: signal: terminated\n", content)
}

func TestExecShellTruncatesOutput(t *testing.T) {
	tool := NewExecShellTool(t.TempDir())
	command := "printf 'HEAD'; head -c 100000 /dev/zero | tr '\\0' x; printf 'TAIL'"
	content, err := execute(t, context.Background(), tool, map[string]any{"command": command})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(content, "Exit code: 0\nHEAD"))
	require.True(t, strings.HasSuffix(content, "TAIL"))
	// Booleans keep a failure from printing 32 KiB of output.
	require.True(t, strings.Contains(content, fmt.Sprintf("\n[... %d bytes truncated ...]\n", 100008-outputLimit)))
	require.Less(t, len(content), outputLimit+100)
}

func TestCappedBuffer(t *testing.T) {
	buffer := newCappedBuffer(4)
	for _, chunk := range []string{"ab", "cdef", "ghijklmnop", "q"} {
		_, err := buffer.Write([]byte(chunk))
		require.NoError(t, err)
	}
	require.Equal(t, "abcd\n[... 9 bytes truncated ...]\nnopq", buffer.String())

	buffer = newCappedBuffer(4)
	_, err := buffer.Write([]byte("abcdefgh"))
	require.NoError(t, err)
	require.Equal(t, "abcdefgh", buffer.String())

	// Invalid UTF-8 is replaced.
	buffer = newCappedBuffer(4)
	_, err = buffer.Write([]byte{'a', 0xff})
	require.NoError(t, err)
	require.Equal(t, "a\uFFFD", buffer.String())
}

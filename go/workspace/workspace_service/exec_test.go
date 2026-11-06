package workspace_service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func TestExec(t *testing.T) {
	service, directory := newTestService(t)
	writeTestFile(t, filepath.Join(directory, "sub", "file.txt"), "hello")

	execRequest := &pb.ExecRequest{Command: "cat sub/file.txt; echo ' world' >&2"}
	execResponse, err := service.Exec(context.Background(), execRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.ExecResponse{Output: "hello world\n"}, execResponse)

	execRequest = &pb.ExecRequest{Command: "pwd; exit 3", WorkingDirectory: "sub"}
	execResponse, err = service.Exec(context.Background(), execRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.ExecResponse{ExitCode: 3, Output: filepath.Join(directory, "sub") + "\n"}, execResponse)

	execRequest = &pb.ExecRequest{Command: "true", WorkingDirectory: "missing"}
	_, err = service.Exec(context.Background(), execRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestExecTimeoutKillsProcessGroup(t *testing.T) {
	service, directory := newTestService(t)
	marker := filepath.Join(directory, "marker")
	// The background child would outlive a kill of bash alone.
	execRequest := &pb.ExecRequest{Command: "(sleep 2; touch " + marker + ") & echo started; wait", TimeoutSeconds: 1}
	start := time.Now()
	execResponse, err := service.Exec(context.Background(), execRequest)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 2*time.Second)
	grpcrequire.Equal(t, &pb.ExecResponse{ExitCode: -1, Output: "started\n", TimedOut: true}, execResponse)

	time.Sleep(1500 * time.Millisecond)
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "the background child survived the timeout")
}

func TestExecCancelled(t *testing.T) {
	service, _ := newTestService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	execRequest := &pb.ExecRequest{Command: "sleep 10"}
	_, err := service.Exec(ctx, execRequest)
	grpcrequire.Error(t, codes.DeadlineExceeded, err)
	require.Less(t, time.Since(start), 2*time.Second)
}

func TestExecBackgroundProcessDoesNotBlock(t *testing.T) {
	service, _ := newTestService(t)
	start := time.Now()
	// The background sleep inherits the output pipe.
	execRequest := &pb.ExecRequest{Command: "sleep 10 & echo done"}
	execResponse, err := service.Exec(context.Background(), execRequest)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
	grpcrequire.Equal(t, &pb.ExecResponse{Output: "done\n"}, execResponse)
}

func TestExecTerminatedBySignal(t *testing.T) {
	service, _ := newTestService(t)
	execRequest := &pb.ExecRequest{Command: "echo dying; kill -TERM $$"}
	execResponse, err := service.Exec(context.Background(), execRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.ExecResponse{ExitCode: -1, Output: "dying\n"}, execResponse)
}

func TestExecTruncatesOutput(t *testing.T) {
	service, _ := newTestService(t)
	execRequest := &pb.ExecRequest{Command: "printf 'HEAD'; head -c 100000 /dev/zero | tr '\\0' x; printf 'TAIL'"}
	execResponse, err := service.Exec(context.Background(), execRequest)
	require.NoError(t, err)
	output := execResponse.GetOutput()
	require.True(t, strings.HasPrefix(output, "HEAD"))
	require.True(t, strings.HasSuffix(output, "TAIL"))
	// Booleans keep a failure from printing 32 KiB of output.
	require.True(t, strings.Contains(output, fmt.Sprintf("\n[... %d bytes truncated ...]\n", 100008-outputLimit)))
	require.Less(t, len(output), outputLimit+100)
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

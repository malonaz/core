package workspace_service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func TestWriteFile(t *testing.T) {
	service, directory := newTestService(t)

	writeFileRequest := &pb.WriteFileRequest{Path: "new/dir/file.txt", Content: "hello"}
	_, err := service.WriteFile(context.Background(), writeFileRequest)
	require.NoError(t, err)
	require.Equal(t, "hello", readTestFile(t, filepath.Join(directory, "new", "dir", "file.txt")))

	// Overwriting keeps the file's mode.
	script := filepath.Join(directory, "script.sh")
	writeTestFile(t, script, "old")
	require.NoError(t, os.Chmod(script, 0o755))
	writeFileRequest = &pb.WriteFileRequest{Path: script, Content: "new"}
	_, err = service.WriteFile(context.Background(), writeFileRequest)
	require.NoError(t, err)
	require.Equal(t, "new", readTestFile(t, script))
	info, err := os.Stat(script)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())

	// A file in the way of a parent directory.
	writeFileRequest = &pb.WriteFileRequest{Path: "script.sh/file.txt", Content: "x"}
	_, err = service.WriteFile(context.Background(), writeFileRequest)
	grpcrequire.Error(t, codes.FailedPrecondition, err)

	writeFileRequest = &pb.WriteFileRequest{Path: "new", Content: "x"}
	_, err = service.WriteFile(context.Background(), writeFileRequest)
	grpcrequire.Error(t, codes.FailedPrecondition, err)
}

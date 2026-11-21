package workspace_service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func TestEditFile(t *testing.T) {
	service, directory := newTestService(t)
	path := filepath.Join(directory, "file.go")
	writeTestFile(t, path, "a := 1\nb := 1\nc := 2\n")

	editFileRequest := &pb.EditFileRequest{Path: "file.go", OldString: "c := 2", NewString: "c := 3"}
	editFileResponse, err := service.EditFile(context.Background(), editFileRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.EditFileResponse{Replacements: 1, StartLines: []int32{3}}, editFileResponse)
	require.Equal(t, "a := 1\nb := 1\nc := 3\n", readTestFile(t, path))

	editFileRequest = &pb.EditFileRequest{Path: "file.go", OldString: ":= 1", NewString: ":= 0"}
	_, err = service.EditFile(context.Background(), editFileRequest)
	grpcrequire.Error(t, codes.FailedPrecondition, err)
	require.ErrorContains(t, err, "old_string occurs 2 times in file.go")
	require.Equal(t, "a := 1\nb := 1\nc := 3\n", readTestFile(t, path))

	editFileRequest = &pb.EditFileRequest{Path: path, OldString: ":= 1", NewString: ":= 0", ReplaceAll: true}
	editFileResponse, err = service.EditFile(context.Background(), editFileRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.EditFileResponse{Replacements: 2, StartLines: []int32{1, 2}}, editFileResponse)
	require.Equal(t, "a := 0\nb := 0\nc := 3\n", readTestFile(t, path))

	for name, testCase := range map[string]struct {
		request *pb.EditFileRequest
		code    codes.Code
	}{
		"no match":     {&pb.EditFileRequest{Path: "file.go", OldString: "d := 4", NewString: "d := 5"}, codes.FailedPrecondition},
		"missing file": {&pb.EditFileRequest{Path: "missing.go", OldString: "a", NewString: "b"}, codes.NotFound},
		"directory":    {&pb.EditFileRequest{Path: ".", OldString: "a", NewString: "b"}, codes.FailedPrecondition},
		"identical":    {&pb.EditFileRequest{Path: "file.go", OldString: "a", NewString: "a"}, codes.InvalidArgument},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := service.EditFile(context.Background(), testCase.request)
			grpcrequire.Error(t, testCase.code, err)
		})
	}
	require.Equal(t, "a := 0\nb := 0\nc := 3\n", readTestFile(t, path))
}

func TestEditFileStartLines(t *testing.T) {
	service, directory := newTestService(t)
	path := filepath.Join(directory, "file.txt")
	// Multi-line occurrences: the second starts after the first's own newlines.
	writeTestFile(t, path, "x\ny\nz\nx\ny\n")

	editFileRequest := &pb.EditFileRequest{Path: "file.txt", OldString: "x\ny\n", NewString: "w\n", ReplaceAll: true}
	editFileResponse, err := service.EditFile(context.Background(), editFileRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.EditFileResponse{Replacements: 2, StartLines: []int32{1, 4}}, editFileResponse)
	require.Equal(t, "w\nz\nw\n", readTestFile(t, path))

	// Mid-line occurrences report the line they begin on.
	editFileRequest = &pb.EditFileRequest{Path: "file.txt", OldString: "\nw", NewString: "\nv"}
	editFileResponse, err = service.EditFile(context.Background(), editFileRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.EditFileResponse{Replacements: 1, StartLines: []int32{2}}, editFileResponse)
}

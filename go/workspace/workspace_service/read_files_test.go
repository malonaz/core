package workspace_service

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func TestReadFiles(t *testing.T) {
	service, directory := newTestService(t)
	writeTestFile(t, filepath.Join(directory, "a.txt"), "one\ntwo\n")
	writeTestFile(t, filepath.Join(directory, "b.txt"), "no newline")
	writeTestFile(t, filepath.Join(directory, "empty.txt"), "")
	writeTestFile(t, filepath.Join(directory, "binary"), "a\x00b")
	absolutePath := filepath.Join(directory, "b.txt")

	readFilesRequest := &pb.ReadFilesRequest{Paths: []string{"a.txt", absolutePath, "empty.txt", "missing.txt", "binary", "."}}
	readFilesResponse, err := service.ReadFiles(context.Background(), readFilesRequest)
	require.NoError(t, err)
	expected := &pb.ReadFilesResponse{
		Files: []*pb.ReadFilesResponse_File{
			{Path: "a.txt", Content: "one\ntwo\n"},
			{Path: absolutePath, Content: "no newline"},
			{Path: "empty.txt"},
			{Path: "missing.txt", Error: "stat " + filepath.Join(directory, "missing.txt") + ": no such file or directory"},
			{Path: "binary", Error: "not a text file"},
			{Path: ".", Error: "is a directory"},
		},
	}
	grpcrequire.Equal(t, expected, readFilesResponse)
}

func TestReadFilesWindow(t *testing.T) {
	service, directory := newTestService(t)
	var lines []string
	for i := 1; i <= 2500; i++ {
		lines = append(lines, fmt.Sprintf("line %d", i))
	}
	writeTestFile(t, filepath.Join(directory, "long.txt"), strings.Join(lines, "\n")+"\n")
	readFile := func(offset, limit int32) *pb.ReadFilesResponse_File {
		t.Helper()
		readFilesRequest := &pb.ReadFilesRequest{Paths: []string{"long.txt"}, Offset: offset, Limit: limit}
		readFilesResponse, err := service.ReadFiles(context.Background(), readFilesRequest)
		require.NoError(t, err)
		require.Len(t, readFilesResponse.GetFiles(), 1)
		return readFilesResponse.GetFiles()[0]
	}

	file := readFile(0, 0)
	require.Equal(t, strings.Join(lines[:2000], "\n")+"\n", file.GetContent())
	require.Equal(t, int32(2001), file.GetNextOffset())

	file = readFile(2001, 0)
	require.Equal(t, strings.Join(lines[2000:], "\n")+"\n", file.GetContent())
	require.Zero(t, file.GetNextOffset())

	grpcrequire.Equal(t, &pb.ReadFilesResponse_File{Path: "long.txt", Content: "line 10\nline 11\n", NextOffset: 12}, readFile(10, 2))
	grpcrequire.Equal(t, &pb.ReadFilesResponse_File{Path: "long.txt", Error: "offset 2501 is past the end of the file (2500 lines)"}, readFile(2501, 0))
}

func TestReadFilesCapsSize(t *testing.T) {
	service, directory := newTestService(t)
	line := strings.Repeat("x", 1023) + "\n"
	writeTestFile(t, filepath.Join(directory, "wide.txt"), strings.Repeat(line, 200))
	writeTestFile(t, filepath.Join(directory, "one-line.txt"), strings.Repeat("y", maxReadSize+10)+"\nnext\n")
	writeTestFile(t, filepath.Join(directory, "huge.txt"), strings.Repeat("z", maxFileSize+1))

	readFilesRequest := &pb.ReadFilesRequest{Paths: []string{"wide.txt", "one-line.txt", "huge.txt"}}
	readFilesResponse, err := service.ReadFiles(context.Background(), readFilesRequest)
	require.NoError(t, err)
	files := readFilesResponse.GetFiles()
	require.Len(t, files, 3)

	require.Equal(t, strings.Repeat(line, 128), files[0].GetContent())
	require.Equal(t, int32(129), files[0].GetNextOffset())

	// A single line over the cap shows its beginning, and reading on skips its rest.
	require.Equal(t, strings.Repeat("y", maxReadSize), files[1].GetContent())
	require.Equal(t, int32(2), files[1].GetNextOffset())

	require.Contains(t, files[2].GetError(), "8388609 bytes, over the 8388608 byte limit")
}

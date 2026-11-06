package workspace_service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"unicode/utf8"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
)

const (
	// maxFileSize bounds the files read whole into memory.
	maxFileSize = 8 << 20
	// maxReadSize caps the content returned per file.
	maxReadSize      = 128 << 10
	defaultLineLimit = 2000
)

// ReadFiles implements the interface.
func (s *Service) ReadFiles(ctx context.Context, request *pb.ReadFilesRequest) (*pb.ReadFilesResponse, error) {
	offset := max(int(request.GetOffset()), 1)
	limit := defaultLineLimit
	if request.GetLimit() > 0 {
		limit = int(request.GetLimit())
	}
	response := &pb.ReadFilesResponse{}
	for _, path := range request.GetPaths() {
		file := &pb.ReadFilesResponse_File{Path: path}
		// One unreadable file never sinks the others.
		content, nextOffset, err := readFile(s.resolvePath(path), offset, limit)
		if err != nil {
			file.Error = err.Error()
		} else {
			file.Content = content
			file.NextOffset = int32(nextOffset)
		}
		response.Files = append(response.Files, file)
	}
	return response, nil
}

// readFile returns a window of a text file's lines, and the line to read on
// from when the window ends before the file does.
func readFile(path string, offset, limit int) (string, int, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", 0, err
	}
	if info.IsDir() {
		return "", 0, errors.New("is a directory")
	}
	if info.Size() > maxFileSize {
		return "", 0, fmt.Errorf("%d bytes, over the %d byte limit: read parts of it with Exec", info.Size(), maxFileSize)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", 0, err
	}
	if bytes.IndexByte(content, 0) != -1 || !utf8.Valid(content) {
		return "", 0, errors.New("not a text file")
	}
	if len(content) == 0 {
		return "", 0, nil
	}
	lines := strings.SplitAfter(string(content), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if offset > len(lines) {
		return "", 0, fmt.Errorf("offset %d is past the end of the file (%d lines)", offset, len(lines))
	}

	start, end := offset-1, min(offset-1+limit, len(lines))
	var builder strings.Builder
	for i := start; i < end; i++ {
		if builder.Len()+len(lines[i]) > maxReadSize {
			if i == start {
				// A single line over the cap still shows its beginning.
				builder.WriteString(strings.ToValidUTF8(lines[i][:maxReadSize], ""))
				i++
			}
			end = i
			break
		}
		builder.WriteString(lines[i])
	}
	if end < len(lines) {
		return builder.String(), end + 1, nil
	}
	return builder.String(), 0, nil
}

package workspace_service

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
)

// WriteFile implements the interface.
func (s *Service) WriteFile(ctx context.Context, request *pb.WriteFileRequest) (*pb.WriteFileResponse, error) {
	path := s.resolvePath(request.GetPath())
	_, err := os.Stat(path)
	created := errors.Is(err, fs.ErrNotExist)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fileError(err)
	}
	// An existing file keeps its mode.
	if err := os.WriteFile(path, []byte(request.GetContent()), 0o644); err != nil {
		return nil, fileError(err)
	}
	return &pb.WriteFileResponse{Created: created, LineCount: int32(lineCount(request.GetContent()))}, nil
}

// lineCount counts lines as ReadFiles does: a final line without a newline still counts.
func lineCount(content string) int {
	count := strings.Count(content, "\n")
	if content != "" && !strings.HasSuffix(content, "\n") {
		count++
	}
	return count
}

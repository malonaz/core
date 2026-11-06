package workspace_service

import (
	"context"
	"os"
	"path/filepath"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
)

// WriteFile implements the interface.
func (s *Service) WriteFile(ctx context.Context, request *pb.WriteFileRequest) (*pb.WriteFileResponse, error) {
	path := s.resolvePath(request.GetPath())
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fileError(err)
	}
	// An existing file keeps its mode.
	if err := os.WriteFile(path, []byte(request.GetContent()), 0o644); err != nil {
		return nil, fileError(err)
	}
	return &pb.WriteFileResponse{}, nil
}

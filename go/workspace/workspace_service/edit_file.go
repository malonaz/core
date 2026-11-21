package workspace_service

import (
	"context"
	"os"
	"strings"

	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// EditFile implements the interface.
func (s *Service) EditFile(ctx context.Context, request *pb.EditFileRequest) (*pb.EditFileResponse, error) {
	if request.GetOldString() == request.GetNewString() {
		return nil, status.Errorf(codes.InvalidArgument, "old_string and new_string are identical").Err()
	}
	path := s.resolvePath(request.GetPath())
	info, err := os.Stat(path)
	if err != nil {
		return nil, fileError(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fileError(err)
	}
	startLines := occurrenceLines(string(content), request.GetOldString())
	switch count := len(startLines); {
	case count == 0:
		return nil, status.Errorf(codes.FailedPrecondition, "old_string not found in %s", request.GetPath()).Err()
	case count > 1 && !request.GetReplaceAll():
		return nil, status.Errorf(codes.FailedPrecondition, "old_string occurs %d times in %s: include more context to make it unique, or set replace_all", count, request.GetPath()).Err()
	}
	edited := strings.ReplaceAll(string(content), request.GetOldString(), request.GetNewString())
	if err := os.WriteFile(path, []byte(edited), info.Mode()); err != nil {
		return nil, fileError(err)
	}
	return &pb.EditFileResponse{Replacements: int32(len(startLines)), StartLines: startLines}, nil
}

// occurrenceLines returns the 1-based line each non-overlapping occurrence of
// substr begins on, matching the occurrences strings.ReplaceAll replaces.
func occurrenceLines(content, substr string) []int32 {
	var lines []int32
	line, scanned := 1, 0
	for {
		index := strings.Index(content[scanned:], substr)
		if index == -1 {
			return lines
		}
		line += strings.Count(content[scanned:scanned+index], "\n")
		lines = append(lines, int32(line))
		// Count the newlines inside the occurrence as we skip past it.
		line += strings.Count(substr, "\n")
		scanned += index + len(substr)
	}
}

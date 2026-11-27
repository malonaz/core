package filesystem_service

import (
	"context"
	"errors"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/malonaz/core/gengo/workspace/model"
	filesystemservicepb "github.com/malonaz/core/genproto/workspace/filesystem_service/v1"
	nodeservicepb "github.com/malonaz/core/genproto/workspace/node_service/v1"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler/longrunning"
)

// DeleteFilesystem implements the interface: a missing filesystem, a stale etag or an attached
// filesystem is refused before the operation starts, so its run acts on state alone.
func (s *Service) DeleteFilesystem(ctx context.Context, request *filesystemservicepb.DeleteFilesystemRequest) (*longrunningpb.Operation, error) {
	if longrunning.IsRun(ctx) {
		return s.FilesystemServiceServer.DeleteFilesystem(ctx, request)
	}
	getFilesystemRequest := &filesystemservicepb.GetFilesystemRequest{Name: request.GetName()}
	filesystem, err := s.GetFilesystem(ctx, getFilesystemRequest)
	if err != nil {
		// With allow_missing the run finds nothing to do.
		if !request.GetAllowMissing() || !status.HasCode(err, codes.NotFound) {
			return nil, err
		}
	} else {
		if request.GetEtag() != "" && request.GetEtag() != filesystem.GetEtag() {
			return nil, status.Errorf(codes.Aborted, "ETag changed").Err()
		}
		if len(filesystem.GetAttachments()) > 0 {
			return nil, status.Errorf(codes.FailedPrecondition, "filesystem is attached to workspace %s", filesystem.GetAttachments()[0].GetWorkspace()).Err()
		}
	}
	return s.FilesystemServiceServer.DeleteFilesystem(ctx, request)
}

// RunDeleteFilesystem implements the interface: the agent purges the files and history, then the
// filesystem and its snapshots go. A retried job whose first attempt deleted the rows is done.
func (s *Service) RunDeleteFilesystem(ctx context.Context, request *filesystemservicepb.DeleteFilesystemRequest) (*emptypb.Empty, error) {
	filesystemRn, err := pb.ParseFilesystemRn(request.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing name: %v", err).Err()
	}
	getFilesystemRequest := &filesystemservicepb.GetFilesystemRequest{Name: request.GetName()}
	filesystem, err := s.GetFilesystem(ctx, getFilesystemRequest)
	if err != nil {
		if status.HasCode(err, codes.NotFound) {
			return &emptypb.Empty{}, nil
		}
		return nil, err
	}
	// Attached since the operation started.
	if len(filesystem.GetAttachments()) > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "filesystem is attached to workspace %s", filesystem.GetAttachments()[0].GetWorkspace()).Err()
	}

	client, closeClient, err := s.nodes.Client(ctx, "")
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "reaching an agent: %v", err).Err()
	}
	defer closeClient()
	purgeFilesystemRequest := &nodeservicepb.PurgeFilesystemRequest{Filesystem: request.GetName()}
	if _, err := client.PurgeFilesystem(ctx, purgeFilesystemRequest); err != nil {
		return nil, status.FromError(err, "purging filesystem").Err()
	}

	// The etag keeps a filesystem attached since the check.
	if _, err := s.workspacePostgresStore.DeleteFilesystem(ctx, filesystemRn.Organization, filesystemRn.User, filesystemRn.Filesystem, filesystem.GetEtag(), true); err != nil {
		switch {
		case errors.Is(err, model.ErrFilesystemNotExist):
			return &emptypb.Empty{}, nil
		case errors.Is(err, model.ErrFilesystemETagChanged):
			return nil, status.Errorf(codes.Aborted, "filesystem changed while deleting").Err()
		}
		return nil, status.FromError(err, "deleting filesystem").Err()
	}
	return &emptypb.Empty{}, nil
}

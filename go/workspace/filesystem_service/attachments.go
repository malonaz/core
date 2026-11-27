package filesystem_service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/workspace/model"
	filesystemservicepb "github.com/malonaz/core/genproto/workspace/filesystem_service/v1"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
)

// AttachFilesystem implements the interface.
func (s *Service) AttachFilesystem(ctx context.Context, request *filesystemservicepb.AttachFilesystemRequest) (*pb.Filesystem, error) {
	attach := func(filesystem *pb.Filesystem) (bool, error) {
		index := slices.IndexFunc(filesystem.GetAttachments(), func(attachment *pb.Attachment) bool {
			return attachment.GetWorkspace() == request.GetWorkspace()
		})
		if index >= 0 && filesystem.GetAttachments()[index].GetMode() == request.GetMode() {
			return false, nil
		}
		if filesystem.GetKind() == pb.Filesystem_KIND_LOCAL && request.GetMode() == pb.Mount_MODE_READ_WRITE {
			for _, attachment := range filesystem.GetAttachments() {
				if attachment.GetMode() == pb.Mount_MODE_READ_WRITE && attachment.GetWorkspace() != request.GetWorkspace() {
					return false, status.Errorf(codes.FailedPrecondition, "LOCAL filesystem %s is written by workspace %s", filesystem.GetName(), attachment.GetWorkspace()).Err()
				}
			}
		}
		if index >= 0 {
			filesystem.Attachments[index].Mode = request.GetMode()
			return true, nil
		}
		filesystem.Attachments = append(filesystem.Attachments, &pb.Attachment{Workspace: request.GetWorkspace(), Mode: request.GetMode()})
		return true, nil
	}
	return s.updateFilesystem(ctx, request.GetName(), attach, "attachments")
}

// DetachFilesystem implements the interface.
func (s *Service) DetachFilesystem(ctx context.Context, request *filesystemservicepb.DetachFilesystemRequest) (*pb.Filesystem, error) {
	detach := func(filesystem *pb.Filesystem) (bool, error) {
		attachments := slices.DeleteFunc(filesystem.GetAttachments(), func(attachment *pb.Attachment) bool {
			return attachment.GetWorkspace() == request.GetWorkspace()
		})
		changed := len(attachments) != len(filesystem.GetAttachments())
		filesystem.Attachments = attachments
		return changed, nil
	}
	return s.updateFilesystem(ctx, request.GetName(), detach, "attachments")
}

// updateFilesystem applies a change to a filesystem's columns, retrying when a concurrent write
// changes its etag first. A change reporting false leaves the filesystem as is.
func (s *Service) updateFilesystem(ctx context.Context, name string, change func(*pb.Filesystem) (bool, error), columns ...string) (*pb.Filesystem, error) {
	columns = append(columns, "update_time", "etag")
	setClauses := make([]string, len(columns))
	for i, column := range columns {
		setClauses[i] = fmt.Sprintf("%s = $%d", column, i+1)
	}
	updateClause := strings.Join(setClauses, ", ")
	for {
		getFilesystemRequest := &filesystemservicepb.GetFilesystemRequest{Name: name}
		filesystem, err := s.GetFilesystem(ctx, getFilesystemRequest)
		if err != nil {
			return nil, err
		}
		patchedFilesystem := proto.CloneOf(filesystem)
		changed, err := change(patchedFilesystem)
		if err != nil || !changed {
			return filesystem, err
		}
		patchedFilesystem.UpdateTime = timestamppb.Now()
		if patchedFilesystem.Etag, err = aip.ComputeETag(patchedFilesystem); err != nil {
			return nil, status.Errorf(codes.Internal, "computing etag: %v", err).Err()
		}
		filesystemModel, err := model.FilesystemFromPb(patchedFilesystem)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting filesystem from pb to model: %v", err).Err()
		}
		updatedFilesystemModel, err := s.workspacePostgresStore.UpdateFilesystem(ctx, filesystemModel, updateClause, columns, filesystem.GetEtag())
		if err != nil {
			if errors.Is(err, model.ErrFilesystemETagChanged) {
				if ctx.Err() != nil {
					return nil, status.FromError(ctx.Err(), "updating filesystem").Err()
				}
				continue
			}
			if errors.Is(err, model.ErrFilesystemNotExist) {
				return nil, status.Errorf(codes.NotFound, "filesystem does not exist").Err()
			}
			return nil, status.FromError(err, "updating filesystem").Err()
		}
		updatedFilesystem, err := updatedFilesystemModel.ToPb()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting filesystem from model to pb: %v", err).Err()
		}
		return updatedFilesystem, nil
	}
}

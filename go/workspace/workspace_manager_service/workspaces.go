package workspace_manager_service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/workspace/model"
	filesystemservicepb "github.com/malonaz/core/genproto/workspace/filesystem_service/v1"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	workspacemanagerservicepb "github.com/malonaz/core/genproto/workspace/workspace_manager_service/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/workspace/pod"
)

// CreateWorkspace implements the interface: the workspace is created STOPPED, then each mount's
// filesystem attached. A failed attachment undoes the create, so a retry starts clean.
func (s *Service) CreateWorkspace(ctx context.Context, request *workspacemanagerservicepb.CreateWorkspaceRequest) (*pb.Workspace, error) {
	request.Workspace.State = pb.Workspace_STATE_STOPPED
	workspace, err := s.WorkspaceManagerServiceServer.CreateWorkspace(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := s.attachMounts(ctx, workspace.GetName(), workspace.GetMounts()); err != nil {
		if rollbackErr := s.detachMounts(ctx, workspace.GetName(), workspace.GetMounts()); rollbackErr != nil {
			return nil, status.FromError(rollbackErr, "detaching after %v", err).Err()
		}
		deleteWorkspaceRequest := &workspacemanagerservicepb.DeleteWorkspaceRequest{Name: workspace.GetName(), AllowMissing: true}
		if _, rollbackErr := s.WorkspaceManagerServiceServer.DeleteWorkspace(ctx, deleteWorkspaceRequest); rollbackErr != nil {
			return nil, status.FromError(rollbackErr, "deleting after %v", err).Err()
		}
		return nil, err
	}
	return workspace, nil
}

// UpdateWorkspace implements the interface. A mounts change attaches the new mounts first, so a
// refused writer fails the update before it applies, and detaches the dropped ones after. A
// running pod is deleted and started again with the new mounts.
func (s *Service) UpdateWorkspace(ctx context.Context, request *workspacemanagerservicepb.UpdateWorkspaceRequest) (*pb.Workspace, error) {
	paths := request.GetUpdateMask().GetPaths()
	if !slices.Contains(paths, "mounts") && !slices.Contains(paths, "*") {
		return s.WorkspaceManagerServiceServer.UpdateWorkspace(ctx, request)
	}
	getWorkspaceRequest := &workspacemanagerservicepb.GetWorkspaceRequest{Name: request.GetWorkspace().GetName()}
	existing, err := s.GetWorkspace(ctx, getWorkspaceRequest)
	if err != nil {
		return nil, err
	}
	mounts := request.GetWorkspace().GetMounts()
	if err := s.attachMounts(ctx, existing.GetName(), mounts); err != nil {
		return nil, err
	}
	workspace, err := s.WorkspaceManagerServiceServer.UpdateWorkspace(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := s.detachMounts(ctx, workspace.GetName(), droppedMounts(existing.GetMounts(), workspace.GetMounts())); err != nil {
		return nil, err
	}
	if proto.Equal(&pb.Workspace{Mounts: existing.GetMounts()}, &pb.Workspace{Mounts: workspace.GetMounts()}) {
		return workspace, nil
	}
	switch workspace.GetState() {
	case pb.Workspace_STATE_STARTING, pb.Workspace_STATE_RUNNING:
	default:
		return workspace, nil
	}
	// STARTING before the pod goes: the reconciler restarts a RUNNING workspace without one.
	restarting := &pb.Workspace{State: pb.Workspace_STATE_STARTING}
	if workspace, err = s.updateWorkspace(ctx, workspace.GetName(), nil, restarting, "state", "node", "start_time"); err != nil {
		return nil, err
	}
	if err := s.pods.Delete(ctx, pod.Name(workspace.GetName())); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting pod: %v", err).Err()
	}
	startWorkspaceRequest := &workspacemanagerservicepb.StartWorkspaceRequest{Name: workspace.GetName()}
	if _, err := s.StartWorkspace(ctx, startWorkspaceRequest); err != nil {
		return nil, status.FromError(err, "starting workspace again").Err()
	}
	return workspace, nil
}

// DeleteWorkspace implements the interface: its pod goes, then its attachments, then the
// workspace, so a failed delete is retried with nothing left mounted.
func (s *Service) DeleteWorkspace(ctx context.Context, request *workspacemanagerservicepb.DeleteWorkspaceRequest) (*emptypb.Empty, error) {
	getWorkspaceRequest := &workspacemanagerservicepb.GetWorkspaceRequest{Name: request.GetName()}
	workspace, err := s.GetWorkspace(ctx, getWorkspaceRequest)
	if err != nil {
		if request.GetAllowMissing() && status.HasCode(err, codes.NotFound) {
			return &emptypb.Empty{}, nil
		}
		return nil, err
	}
	if request.GetEtag() != "" && request.GetEtag() != workspace.GetEtag() {
		return nil, status.Errorf(codes.Aborted, "ETag changed").Err()
	}
	if err := s.pods.Delete(ctx, pod.Name(workspace.GetName())); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting pod: %v", err).Err()
	}
	if err := s.detachMounts(ctx, workspace.GetName(), workspace.GetMounts()); err != nil {
		return nil, err
	}
	return s.WorkspaceManagerServiceServer.DeleteWorkspace(ctx, request)
}

// attachMounts attaches each mount's filesystem to a workspace, in its mode.
func (s *Service) attachMounts(ctx context.Context, workspace string, mounts []*pb.Mount) error {
	for _, mount := range mounts {
		attachFilesystemRequest := &filesystemservicepb.AttachFilesystemRequest{Name: mount.GetFilesystem(), Workspace: workspace, Mode: mount.GetMode()}
		if _, err := s.filesystemServiceClient.AttachFilesystem(ctx, attachFilesystemRequest); err != nil {
			return status.FromError(err, "attaching filesystem %s", mount.GetFilesystem()).Err()
		}
	}
	return nil
}

// detachMounts detaches each mount's filesystem from a workspace; one already gone is not an error.
func (s *Service) detachMounts(ctx context.Context, workspace string, mounts []*pb.Mount) error {
	for _, mount := range mounts {
		detachFilesystemRequest := &filesystemservicepb.DetachFilesystemRequest{Name: mount.GetFilesystem(), Workspace: workspace}
		if _, err := s.filesystemServiceClient.DetachFilesystem(ctx, detachFilesystemRequest); err != nil && !status.HasCode(err, codes.NotFound) {
			return status.FromError(err, "detaching filesystem %s", mount.GetFilesystem()).Err()
		}
	}
	return nil
}

// droppedMounts returns the mounts whose filesystem the new mounts no longer hold.
func droppedMounts(oldMounts, newMounts []*pb.Mount) []*pb.Mount {
	kept := map[string]bool{}
	for _, newMount := range newMounts {
		kept[newMount.GetFilesystem()] = true
	}
	var dropped []*pb.Mount
	for _, oldMount := range oldMounts {
		if !kept[oldMount.GetFilesystem()] {
			dropped = append(dropped, oldMount)
		}
	}
	return dropped
}

// updateWorkspace sets a workspace's columns to patch's values, retrying when a concurrent write
// changes its etag first. A workspace in none of the states from fails; nil allows any.
func (s *Service) updateWorkspace(ctx context.Context, name string, from []pb.Workspace_State, patch *pb.Workspace, patchColumns ...string) (*pb.Workspace, error) {
	mask := pbfieldmask.FromPaths(patchColumns...)
	columns := slices.Concat(patchColumns, []string{"update_time", "etag"})
	setClauses := make([]string, len(columns))
	for i, column := range columns {
		setClauses[i] = fmt.Sprintf("%s = $%d", column, i+1)
	}
	updateClause := strings.Join(setClauses, ", ")
	for {
		getWorkspaceRequest := &workspacemanagerservicepb.GetWorkspaceRequest{Name: name}
		workspace, err := s.GetWorkspace(ctx, getWorkspaceRequest)
		if err != nil {
			return nil, err
		}
		if from != nil && !slices.Contains(from, workspace.GetState()) {
			return nil, status.Errorf(codes.FailedPrecondition, "workspace is %s", workspace.GetState()).Err()
		}
		patchedWorkspace := proto.CloneOf(workspace)
		mask.Update(patchedWorkspace, patch)
		if proto.Equal(patchedWorkspace, workspace) {
			return workspace, nil
		}
		patchedWorkspace.UpdateTime = timestamppb.Now()
		if patchedWorkspace.Etag, err = aip.ComputeETag(patchedWorkspace); err != nil {
			return nil, status.Errorf(codes.Internal, "computing etag: %v", err).Err()
		}
		workspaceModel, err := model.WorkspaceFromPb(patchedWorkspace)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting workspace from pb to model: %v", err).Err()
		}
		updatedWorkspaceModel, err := s.workspacePostgresStore.UpdateWorkspace(ctx, workspaceModel, updateClause, columns, workspace.GetEtag())
		if err != nil {
			if errors.Is(err, model.ErrWorkspaceETagChanged) {
				if ctx.Err() != nil {
					return nil, status.FromError(ctx.Err(), "updating workspace").Err()
				}
				continue
			}
			if errors.Is(err, model.ErrWorkspaceNotExist) {
				return nil, status.Errorf(codes.NotFound, "workspace does not exist").Err()
			}
			return nil, status.FromError(err, "updating workspace").Err()
		}
		updatedWorkspace, err := updatedWorkspaceModel.ToPb()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting workspace from model to pb: %v", err).Err()
		}
		return updatedWorkspace, nil
	}
}

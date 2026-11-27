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
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/malonaz/core/gengo/workspace/model"
	filesystemservicepb "github.com/malonaz/core/genproto/workspace/filesystem_service/v1"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	workspacemanagerservicepb "github.com/malonaz/core/genproto/workspace/workspace_manager_service/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"github.com/malonaz/core/go/uuid"
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
		if rollbackErr := s.detachUnmounted(ctx, workspace.GetName(), nil); rollbackErr != nil {
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
// refused writer fails the update before it applies. It then converges on the stored mounts, so a
// retry finishes what a failed call left: it detaches every filesystem they no longer hold, and
// restarts a pod built with other mounts.
func (s *Service) UpdateWorkspace(ctx context.Context, request *workspacemanagerservicepb.UpdateWorkspaceRequest) (*pb.Workspace, error) {
	paths := request.GetUpdateMask().GetPaths()
	if !slices.Contains(paths, "mounts") && !slices.Contains(paths, "*") {
		return s.WorkspaceManagerServiceServer.UpdateWorkspace(ctx, request)
	}
	if err := s.attachMounts(ctx, request.GetWorkspace().GetName(), request.GetWorkspace().GetMounts()); err != nil {
		return nil, err
	}
	workspace, err := s.WorkspaceManagerServiceServer.UpdateWorkspace(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := s.detachUnmounted(ctx, workspace.GetName(), workspace.GetMounts()); err != nil {
		return nil, err
	}
	switch workspace.GetState() {
	case pb.Workspace_STATE_STARTING, pb.Workspace_STATE_RUNNING:
	default:
		return workspace, nil
	}
	workspacePod, err := s.pods.Get(ctx, pod.Name(workspace.GetName()))
	if err != nil {
		// A STARTING workspace creates its pod from the stored mounts; the reconciler restarts a
		// RUNNING one without a pod.
		if apierrors.IsNotFound(err) {
			return workspace, nil
		}
		return nil, status.Errorf(codes.Internal, "getting pod: %v", err).Err()
	}
	if isTerminating(workspacePod) || pod.HasMounts(workspacePod, workspace.GetMounts()) {
		return workspace, nil
	}
	return s.restartWorkspace(ctx, workspace.GetName())
}

// restartWorkspace replaces a workspace's pod with one built from its stored mounts. It goes
// STARTING before the pod goes, so the reconciler never sees it RUNNING without one.
func (s *Service) restartWorkspace(ctx context.Context, name string) (*pb.Workspace, error) {
	restartable := []pb.Workspace_State{pb.Workspace_STATE_STARTING, pb.Workspace_STATE_RUNNING}
	restarting := &pb.Workspace{State: pb.Workspace_STATE_STARTING}
	workspace, err := s.updateWorkspace(ctx, name, restartable, restarting, "state", "node", "start_time")
	if err != nil {
		return nil, err
	}
	if err := s.pods.Delete(ctx, pod.Name(name)); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting pod: %v", err).Err()
	}
	// Derived from the etag going STARTING changed: a retry starts the same operation.
	startWorkspaceRequest := &workspacemanagerservicepb.StartWorkspaceRequest{
		Name:      name,
		RequestId: uuid.NewV5(workspaceNamespace, name+"/"+workspace.GetEtag()).String(),
	}
	if _, err := s.StartWorkspace(ctx, startWorkspaceRequest); err != nil {
		return nil, status.FromError(err, "starting workspace again").Err()
	}
	return workspace, nil
}

// DeleteWorkspace implements the interface: it goes STOPPING, then its pod goes, then its
// attachments, then the workspace, so a failed delete is retried with nothing left mounted.
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
	// STOPPING first: the reconciler would start a RUNNING workspace whose pod is gone.
	if workspace.GetState() == pb.Workspace_STATE_STARTING || workspace.GetState() == pb.Workspace_STATE_RUNNING {
		stopping := &pb.Workspace{State: pb.Workspace_STATE_STOPPING}
		if _, err := s.updateWorkspace(ctx, workspace.GetName(), nil, stopping, "state"); err != nil {
			return nil, err
		}
	}
	if err := s.pods.Delete(ctx, pod.Name(workspace.GetName())); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting pod: %v", err).Err()
	}
	if err := s.detachUnmounted(ctx, workspace.GetName(), nil); err != nil {
		return nil, err
	}
	// The etag was checked above; going STOPPING changed it.
	deleteWorkspaceRequest := &workspacemanagerservicepb.DeleteWorkspaceRequest{Name: request.GetName(), AllowMissing: request.GetAllowMissing()}
	return s.WorkspaceManagerServiceServer.DeleteWorkspace(ctx, deleteWorkspaceRequest)
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

// detachUnmounted detaches a workspace from every filesystem it is attached to that its mounts no
// longer hold, found from the filesystems' attachments rather than from what it mounted before.
// Workspaces mount only their user's filesystems; a filesystem already gone is not an error.
func (s *Service) detachUnmounted(ctx context.Context, workspace string, mounts []*pb.Mount) error {
	workspaceRn, err := pb.ParseWorkspaceRn(workspace)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "parsing workspace: %v", err).Err()
	}
	mounted := map[string]bool{}
	for _, mount := range mounts {
		mounted[mount.GetFilesystem()] = true
	}
	// Listed in full first: each detach drops a filesystem from the filter's results.
	var unmounted []string
	listFilesystemsRequest := &filesystemservicepb.ListFilesystemsRequest{
		Parent: workspaceRn.UserRn().String(),
		Filter: fmt.Sprintf("attachments.workspace:%q", workspace),
	}
	for {
		listFilesystemsResponse, err := s.filesystemServiceClient.ListFilesystems(ctx, listFilesystemsRequest)
		if err != nil {
			return status.FromError(err, "listing attached filesystems").Err()
		}
		for _, filesystem := range listFilesystemsResponse.GetFilesystems() {
			if !mounted[filesystem.GetName()] {
				unmounted = append(unmounted, filesystem.GetName())
			}
		}
		if listFilesystemsResponse.GetNextPageToken() == "" {
			break
		}
		listFilesystemsRequest.PageToken = listFilesystemsResponse.GetNextPageToken()
	}
	for _, filesystem := range unmounted {
		detachFilesystemRequest := &filesystemservicepb.DetachFilesystemRequest{Name: filesystem, Workspace: workspace}
		if _, err := s.filesystemServiceClient.DetachFilesystem(ctx, detachFilesystemRequest); err != nil && !status.HasCode(err, codes.NotFound) {
			return status.FromError(err, "detaching filesystem %s", filesystem).Err()
		}
	}
	return nil
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

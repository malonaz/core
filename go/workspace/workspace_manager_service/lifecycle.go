package workspace_manager_service

import (
	"context"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	filesystemservicepb "github.com/malonaz/core/genproto/workspace/filesystem_service/v1"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	workspacemanagerservicepb "github.com/malonaz/core/genproto/workspace/workspace_manager_service/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
	"github.com/malonaz/core/go/workspace/pod"
)

var snapshotNamespace = aip.MustGetUUIDNamespace(&pb.Snapshot{})

// RunStartWorkspace implements the interface: it creates the pod, waits for it to serve and
// records it. A RUNNING workspace whose pod is gone, its node lost, is started again.
func (s *Service) RunStartWorkspace(ctx context.Context, request *workspacemanagerservicepb.StartWorkspaceRequest) (*pb.Workspace, error) {
	getWorkspaceRequest := &workspacemanagerservicepb.GetWorkspaceRequest{Name: request.GetName()}
	workspace, err := s.GetWorkspace(ctx, getWorkspaceRequest)
	if err != nil {
		return nil, err
	}
	podName := pod.Name(workspace.GetName())
	switch workspace.GetState() {
	case pb.Workspace_STATE_STOPPING:
		return nil, status.Errorf(codes.FailedPrecondition, "workspace is stopping").Err()
	case pb.Workspace_STATE_RUNNING:
		existing, err := s.pods.Get(ctx, podName)
		if err == nil && !isTerminating(existing) {
			return workspace, nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.Internal, "getting pod: %v", err).Err()
		}
	}
	startable := []pb.Workspace_State{pb.Workspace_STATE_STOPPED, pb.Workspace_STATE_STARTING, pb.Workspace_STATE_RUNNING}
	starting := &pb.Workspace{State: pb.Workspace_STATE_STARTING}
	if workspace, err = s.updateWorkspace(ctx, workspace.GetName(), startable, starting, "state"); err != nil {
		return nil, err
	}

	filesystemNameToKind := map[string]pb.Filesystem_Kind{}
	for _, mount := range workspace.GetMounts() {
		filesystem, err := s.getFilesystem(ctx, mount.GetFilesystem())
		if err != nil {
			return nil, err
		}
		filesystemNameToKind[filesystem.GetName()] = filesystem.GetKind()
	}
	spec, err := pod.Spec(s.podConfig, workspace, filesystemNameToKind)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "building pod: %v", err).Err()
	}
	// The pod of a recreate or a lost node may still be terminating under the same name.
	if existing, err := s.pods.Get(ctx, podName); err == nil && isTerminating(existing) {
		if err := s.pods.WaitDeleted(ctx, podName); err != nil {
			return nil, status.Errorf(codes.DeadlineExceeded, "%v", err).Err()
		}
	}
	if _, err := s.pods.GetOrCreate(ctx, spec); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}
	ready, err := s.pods.WaitRunning(ctx, podName)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "%v", err).Err()
	}
	// A start counts as use: the reconciler gives the workspace its idle timeout from now.
	now := time.Now()
	if ready, err = s.pods.Touch(ctx, ready, now); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}

	running := &pb.Workspace{LastActiveTime: timestamppb.New(now)}
	pod.RecordStart(running, ready)
	return s.updateWorkspace(ctx, workspace.GetName(), []pb.Workspace_State{pb.Workspace_STATE_STARTING}, running, "state", "node", "start_time", "last_active_time")
}

// RunStopWorkspace implements the interface: it snapshots the LOCAL read-write mounts, then
// deletes the pod, keeping when the pod was last used.
func (s *Service) RunStopWorkspace(ctx context.Context, request *workspacemanagerservicepb.StopWorkspaceRequest) (*pb.Workspace, error) {
	job, ok := scheduler.JobFromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "StopWorkspace runs only as a scheduler job").Err()
	}
	getWorkspaceRequest := &workspacemanagerservicepb.GetWorkspaceRequest{Name: request.GetName()}
	workspace, err := s.GetWorkspace(ctx, getWorkspaceRequest)
	if err != nil {
		return nil, err
	}
	if request.GetEtag() != "" && request.GetEtag() != workspace.GetEtag() {
		return nil, status.Errorf(codes.Aborted, "ETag changed").Err()
	}
	if workspace.GetState() == pb.Workspace_STATE_STOPPED {
		return workspace, nil
	}
	stopping := &pb.Workspace{State: pb.Workspace_STATE_STOPPING}
	if workspace, err = s.updateWorkspace(ctx, workspace.GetName(), nil, stopping, "state"); err != nil {
		return nil, err
	}

	podName := pod.Name(workspace.GetName())
	// The annotation keeps seconds: it only moves the time forward.
	stopped := &pb.Workspace{State: pb.Workspace_STATE_STOPPED, LastActiveTime: workspace.GetLastActiveTime()}
	if existing, err := s.pods.Get(ctx, podName); err == nil {
		if lastActive, ok := pod.LastActiveTime(existing); ok && lastActive.After(workspace.GetLastActiveTime().AsTime()) {
			stopped.LastActiveTime = timestamppb.New(lastActive)
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.Internal, "getting pod: %v", err).Err()
	}
	if _, err := s.snapshotMounts(ctx, workspace, pb.Snapshot_TRIGGER_STOP, "", job); err != nil {
		return nil, err
	}
	if err := s.pods.Delete(ctx, podName); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err).Err()
	}

	return s.updateWorkspace(ctx, workspace.GetName(), nil, stopped, "state", "node", "start_time", "last_active_time")
}

// RunSnapshotWorkspace implements the interface.
func (s *Service) RunSnapshotWorkspace(ctx context.Context, request *workspacemanagerservicepb.SnapshotWorkspaceRequest) (*workspacemanagerservicepb.SnapshotWorkspaceResponse, error) {
	job, ok := scheduler.JobFromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "SnapshotWorkspace runs only as a scheduler job").Err()
	}
	getWorkspaceRequest := &workspacemanagerservicepb.GetWorkspaceRequest{Name: request.GetName()}
	workspace, err := s.GetWorkspace(ctx, getWorkspaceRequest)
	if err != nil {
		return nil, err
	}
	trigger := pb.Snapshot_TRIGGER_IDLE
	if request.GetTurn() != "" {
		trigger = pb.Snapshot_TRIGGER_TURN
	}
	snapshots, err := s.snapshotMounts(ctx, workspace, trigger, request.GetTurn(), job)
	if err != nil {
		return nil, err
	}
	return &workspacemanagerservicepb.SnapshotWorkspaceResponse{Snapshots: snapshots}, nil
}

// snapshotMounts snapshots each LOCAL read-write mount of a workspace, in mount order, and waits
// for every snapshot.
func (s *Service) snapshotMounts(ctx context.Context, workspace *pb.Workspace, trigger pb.Snapshot_Trigger, turn, job string) ([]string, error) {
	var snapshots []string
	for _, mount := range workspace.GetMounts() {
		if mount.GetMode() != pb.Mount_MODE_READ_WRITE {
			continue
		}
		filesystem, err := s.getFilesystem(ctx, mount.GetFilesystem())
		if err != nil {
			return nil, err
		}
		if filesystem.GetKind() != pb.Filesystem_KIND_LOCAL {
			continue
		}
		createSnapshotRequest := &filesystemservicepb.CreateSnapshotRequest{
			Parent:    filesystem.GetName(),
			Snapshot:  &pb.Snapshot{Trigger: trigger, Turn: turn},
			RequestId: uuid.NewV5(snapshotNamespace, job+"/"+filesystem.GetName()).String(),
		}
		operation, err := s.filesystemServiceClient.CreateSnapshot(ctx, createSnapshotRequest)
		if err != nil {
			return nil, status.FromError(err, "snapshotting filesystem %s", filesystem.GetName()).Err()
		}
		snapshot := &pb.Snapshot{}
		if err := s.waitOperation(ctx, operation, snapshot); err != nil {
			return nil, status.FromError(err, "snapshotting filesystem %s", filesystem.GetName()).Err()
		}
		snapshots = append(snapshots, snapshot.GetName())
	}
	return snapshots, nil
}

// waitOperation waits for an operation to be done, and unpacks its response.
func (s *Service) waitOperation(ctx context.Context, operation *longrunningpb.Operation, response *pb.Snapshot) error {
	for !operation.GetDone() {
		waitOperationRequest := &longrunningpb.WaitOperationRequest{Name: operation.GetName(), Timeout: durationpb.New(time.Minute)}
		var err error
		if operation, err = s.operations.WaitOperation(ctx, waitOperationRequest); err != nil {
			return err
		}
	}
	if operation.GetError() != nil {
		return grpcstatus.FromProto(operation.GetError()).Err()
	}
	if err := operation.GetResponse().UnmarshalTo(response); err != nil {
		return status.Errorf(codes.Internal, "unpacking operation response: %v", err).Err()
	}
	return nil
}

// getFilesystem gets a mounted filesystem.
func (s *Service) getFilesystem(ctx context.Context, name string) (*pb.Filesystem, error) {
	getFilesystemRequest := &filesystemservicepb.GetFilesystemRequest{Name: name}
	filesystem, err := s.filesystemServiceClient.GetFilesystem(ctx, getFilesystemRequest)
	if err != nil {
		return nil, status.FromError(err, "getting filesystem %s", name).Err()
	}
	return filesystem, nil
}

// isTerminating reports whether a pod is being deleted.
func isTerminating(workspacePod *corev1.Pod) bool {
	return workspacePod.DeletionTimestamp != nil
}

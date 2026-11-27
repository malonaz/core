package filesystem_service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/workspace/model"
	filesystemservicepb "github.com/malonaz/core/genproto/workspace/filesystem_service/v1"
	nodeservicepb "github.com/malonaz/core/genproto/workspace/node_service/v1"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

// snapshotNamespace derives a snapshot's request id from the job creating it: a retried job reaches
// the agent and the store with the same one.
var snapshotNamespace = uuid.MustParse("0b6f5f4e-3d0a-4a5c-9f63-7c1e2d8a4b90")

// RunCreateSnapshot implements the interface. The agent on the node of the workspace writing the
// filesystem uploads what changed; with no running writer nothing can have changed since the
// last snapshot, so an empty one is recorded without the agent.
func (s *Service) RunCreateSnapshot(ctx context.Context, request *filesystemservicepb.CreateSnapshotRequest) (*pb.Snapshot, error) {
	job, ok := scheduler.JobFromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "CreateSnapshot runs only as a scheduler job").Err()
	}
	filesystemRn, err := pb.ParseFilesystemRn(request.GetParent())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	getFilesystemRequest := &filesystemservicepb.GetFilesystemRequest{Name: request.GetParent()}
	filesystem, err := s.GetFilesystem(ctx, getFilesystemRequest)
	if err != nil {
		return nil, err
	}
	if filesystem.GetKind() != pb.Filesystem_KIND_LOCAL {
		return nil, status.Errorf(codes.FailedPrecondition, "only a LOCAL filesystem has snapshots").Err()
	}

	requestID := uuid.NewV5(snapshotNamespace, job).String()
	snapshot := proto.CloneOf(request.GetSnapshot())
	snapshot.Name = filesystemRn.SnapshotRn(aip.NewSystemGeneratedBase32ResourceID()).String()
	node, err := s.writerNode(ctx, filesystem)
	if err != nil {
		return nil, err
	}
	if node != "" {
		client, closeClient, err := s.nodes.Client(ctx, node)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "reaching the agent on node %s: %v", node, err).Err()
		}
		defer closeClient()
		snapshotFilesystemRequest := &nodeservicepb.SnapshotFilesystemRequest{Snapshot: snapshot, RequestId: requestID}
		snapshot, err = client.SnapshotFilesystem(ctx, snapshotFilesystemRequest)
		if err != nil {
			return nil, status.FromError(err, "snapshotting filesystem").Err()
		}
	}
	snapshot.CreateTime = timestamppb.Now()

	snapshotModel, err := model.SnapshotFromPb(snapshot)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting snapshot from pb to model: %v", err).Err()
	}
	// A retry finds the snapshot its first attempt inserted.
	snapshotModels, err := s.workspacePostgresStore.BatchInsertSnapshots(ctx, []string{requestID}, []*model.Snapshot{snapshotModel})
	if err != nil {
		return nil, status.FromError(err, "inserting snapshot").Err()
	}
	if len(snapshotModels) != 1 {
		return nil, status.Errorf(codes.Internal, "expected 1 inserted snapshot, got %d", len(snapshotModels)).Err()
	}
	if snapshot, err = snapshotModels[0].ToPb(); err != nil {
		return nil, status.Errorf(codes.Internal, "converting snapshot from model to pb: %v", err).Err()
	}

	setLatestSnapshot := func(filesystem *pb.Filesystem) (bool, error) {
		changed := filesystem.GetLatestSnapshot() != snapshot.GetName()
		filesystem.LatestSnapshot = snapshot.GetName()
		return changed, nil
	}
	if _, err := s.updateFilesystem(ctx, request.GetParent(), setLatestSnapshot, "latest_snapshot"); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// writerNode returns the node of the running workspace writing a filesystem, or empty if none is.
func (s *Service) writerNode(ctx context.Context, filesystem *pb.Filesystem) (string, error) {
	for _, attachment := range filesystem.GetAttachments() {
		if attachment.GetMode() != pb.Mount_MODE_READ_WRITE {
			continue
		}
		workspaceRn, err := pb.ParseWorkspaceRn(attachment.GetWorkspace())
		if err != nil {
			return "", status.Errorf(codes.Internal, "parsing attachment workspace: %v", err).Err()
		}
		workspaceModel, err := s.workspacePostgresStore.GetWorkspace(ctx, workspaceRn.Organization, workspaceRn.User, workspaceRn.Workspace)
		if err != nil {
			if errors.Is(err, model.ErrWorkspaceNotExist) {
				return "", nil
			}
			return "", status.FromError(err, "getting writing workspace").Err()
		}
		workspace, err := workspaceModel.ToPb()
		if err != nil {
			return "", status.Errorf(codes.Internal, "converting workspace from model to pb: %v", err).Err()
		}
		switch workspace.GetState() {
		case pb.Workspace_STATE_RUNNING, pb.Workspace_STATE_STOPPING:
			return workspace.GetNode(), nil
		}
		return "", nil
	}
	return "", nil
}

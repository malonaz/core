package filesystem_service

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/workspace/model"
	"github.com/malonaz/core/gengo/workspace/store"
	filesystemservicepb "github.com/malonaz/core/genproto/workspace/filesystem_service/v1"
	nodeservicepb "github.com/malonaz/core/genproto/workspace/node_service/v1"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/postgres"
	testserver "github.com/malonaz/core/go/postgres/test_server"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
)

// migrationsDirectory holds the workspace database's migrations, provided to the test as data.
const migrationsDirectory = "go/workspace/migrations"

// testPostgresClient is shared by every test: isolate tests by user.
var testPostgresClient *postgres.Client

func TestMain(m *testing.M) {
	testserver.RunWithPostgres(context.Background(), m, slog.Default(), &testPostgresClient, nil, "", os.ReadFile, migrationsDirectory)
}

// fakeNodes is every node's agent: it records the calls, and returns its snapshot's paths.
type fakeNodes struct {
	nodeservicepb.NodeServiceClient
	mutex     sync.Mutex
	nodes     []string
	snapshots []*nodeservicepb.SnapshotFilesystemRequest
	purges    []*nodeservicepb.PurgeFilesystemRequest
}

func (f *fakeNodes) Client(ctx context.Context, node string) (nodeservicepb.NodeServiceClient, func(), error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.nodes = append(f.nodes, node)
	return f, func() {}, nil
}

func (f *fakeNodes) SnapshotFilesystem(ctx context.Context, request *nodeservicepb.SnapshotFilesystemRequest, _ ...grpc.CallOption) (*pb.Snapshot, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.snapshots = append(f.snapshots, request)
	snapshot := proto.CloneOf(request.GetSnapshot())
	snapshot.SizeBytes = 42
	snapshot.Added = []string{"main.go"}
	return snapshot, nil
}

func (f *fakeNodes) PurgeFilesystem(ctx context.Context, request *nodeservicepb.PurgeFilesystemRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.purges = append(f.purges, request)
	return &emptypb.Empty{}, nil
}

// newTestService returns a service over the test database, with fake agents, and a user of its own.
func newTestService(t *testing.T) (*Service, *fakeNodes, string) {
	t.Helper()
	service, err := New(&Opts{}, store.New(testPostgresClient), nil)
	require.NoError(t, err)
	nodes := &fakeNodes{}
	service.nodes = nodes
	user := "organizations/o/users/" + uuid.MustNewV7().String()
	return service, nodes, user
}

func createFilesystem(t *testing.T, service *Service, user, id string, kind pb.Filesystem_Kind) *pb.Filesystem {
	t.Helper()
	createFilesystemRequest := &filesystemservicepb.CreateFilesystemRequest{
		Parent:       user,
		FilesystemId: id,
		Filesystem:   &pb.Filesystem{Kind: kind},
	}
	filesystem, err := service.CreateFilesystem(context.Background(), createFilesystemRequest)
	require.NoError(t, err)
	return filesystem
}

// createWorkspace inserts a workspace in a state, on a node.
func createWorkspace(t *testing.T, service *Service, user, id string, state pb.Workspace_State, node string) string {
	t.Helper()
	workspace := &pb.Workspace{
		Name:       user + "/workspaces/" + id,
		CreateTime: timestamppb.Now(),
		UpdateTime: timestamppb.Now(),
		Etag:       "etag",
		State:      state,
		Node:       node,
	}
	workspaceModel, err := model.WorkspaceFromPb(workspace)
	require.NoError(t, err)
	_, err = service.workspacePostgresStore.BatchInsertWorkspaces(context.Background(), []string{uuid.MustNewV7().String()}, []*model.Workspace{workspaceModel})
	require.NoError(t, err)
	return workspace.GetName()
}

func attach(t *testing.T, service *Service, filesystem, workspace string, mode pb.Mount_Mode) (*pb.Filesystem, error) {
	t.Helper()
	attachFilesystemRequest := &filesystemservicepb.AttachFilesystemRequest{Name: filesystem, Workspace: workspace, Mode: mode}
	return service.AttachFilesystem(context.Background(), attachFilesystemRequest)
}

func detach(t *testing.T, service *Service, filesystem, workspace string) *pb.Filesystem {
	t.Helper()
	detachFilesystemRequest := &filesystemservicepb.DetachFilesystemRequest{Name: filesystem, Workspace: workspace}
	detached, err := service.DetachFilesystem(context.Background(), detachFilesystemRequest)
	require.NoError(t, err)
	return detached
}

// requireProtosEqual compares slices of protos.
func requireProtosEqual(t *testing.T, expected, actual any) {
	t.Helper()
	diff := cmp.Diff(expected, actual, protocmp.Transform())
	require.Empty(t, diff, diff)
}

// requireAttachments checks a filesystem's attachments, and that it is stored with them.
func requireAttachments(t *testing.T, service *Service, filesystem *pb.Filesystem, expected ...*pb.Attachment) {
	t.Helper()
	getFilesystemRequest := &filesystemservicepb.GetFilesystemRequest{Name: filesystem.GetName()}
	stored, err := service.GetFilesystem(context.Background(), getFilesystemRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, stored, filesystem)
	grpcrequire.Equal(t, &pb.Filesystem{Attachments: expected}, &pb.Filesystem{Attachments: filesystem.GetAttachments()})
}

func TestAttachFilesystem(t *testing.T) {
	t.Run("LOCAL takes one writer", func(t *testing.T) {
		service, _, user := newTestService(t)
		filesystem := createFilesystem(t, service, user, "repo", pb.Filesystem_KIND_LOCAL).GetName()
		workspace1, workspace2 := user+"/workspaces/w1", user+"/workspaces/w2"

		attached, err := attach(t, service, filesystem, workspace1, pb.Mount_MODE_READ_WRITE)
		require.NoError(t, err)
		requireAttachments(t, service, attached, &pb.Attachment{Workspace: workspace1, Mode: pb.Mount_MODE_READ_WRITE})

		// Attaching again as is changes nothing.
		again, err := attach(t, service, filesystem, workspace1, pb.Mount_MODE_READ_WRITE)
		require.NoError(t, err)
		grpcrequire.Equal(t, attached, again)

		attached, err = attach(t, service, filesystem, workspace2, pb.Mount_MODE_READ_ONLY)
		require.NoError(t, err)
		_, err = attach(t, service, filesystem, workspace2, pb.Mount_MODE_READ_WRITE)
		grpcrequire.Error(t, codes.FailedPrecondition, err)

		// The writer turning reader frees the filesystem for another.
		_, err = attach(t, service, filesystem, workspace1, pb.Mount_MODE_READ_ONLY)
		require.NoError(t, err)
		attached, err = attach(t, service, filesystem, workspace2, pb.Mount_MODE_READ_WRITE)
		require.NoError(t, err)
		requireAttachments(t, service, attached,
			&pb.Attachment{Workspace: workspace1, Mode: pb.Mount_MODE_READ_ONLY},
			&pb.Attachment{Workspace: workspace2, Mode: pb.Mount_MODE_READ_WRITE},
		)
	})

	t.Run("SHARED takes many writers", func(t *testing.T) {
		service, _, user := newTestService(t)
		filesystem := createFilesystem(t, service, user, "shared", pb.Filesystem_KIND_SHARED).GetName()
		workspace1, workspace2 := user+"/workspaces/w1", user+"/workspaces/w2"
		_, err := attach(t, service, filesystem, workspace1, pb.Mount_MODE_READ_WRITE)
		require.NoError(t, err)
		attached, err := attach(t, service, filesystem, workspace2, pb.Mount_MODE_READ_WRITE)
		require.NoError(t, err)
		requireAttachments(t, service, attached,
			&pb.Attachment{Workspace: workspace1, Mode: pb.Mount_MODE_READ_WRITE},
			&pb.Attachment{Workspace: workspace2, Mode: pb.Mount_MODE_READ_WRITE},
		)
	})

	t.Run("concurrent writers: one wins", func(t *testing.T) {
		service, _, user := newTestService(t)
		filesystem := createFilesystem(t, service, user, "repo", pb.Filesystem_KIND_LOCAL).GetName()
		const workspaces = 8
		errs := make([]error, workspaces)
		var waitGroup sync.WaitGroup
		for i := range workspaces {
			waitGroup.Go(func() {
				_, errs[i] = attach(t, service, filesystem, user+"/workspaces/w"+string(rune('a'+i)), pb.Mount_MODE_READ_WRITE)
			})
		}
		waitGroup.Wait()
		succeeded := 0
		for _, err := range errs {
			if err == nil {
				succeeded++
				continue
			}
			grpcrequire.Error(t, codes.FailedPrecondition, err)
		}
		require.Equal(t, 1, succeeded)
	})

	t.Run("missing filesystem", func(t *testing.T) {
		service, _, user := newTestService(t)
		_, err := attach(t, service, user+"/filesystems/missing", user+"/workspaces/w1", pb.Mount_MODE_READ_ONLY)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestDetachFilesystem(t *testing.T) {
	service, _, user := newTestService(t)
	filesystem := createFilesystem(t, service, user, "repo", pb.Filesystem_KIND_LOCAL).GetName()
	workspace1, workspace2 := user+"/workspaces/w1", user+"/workspaces/w2"
	_, err := attach(t, service, filesystem, workspace1, pb.Mount_MODE_READ_WRITE)
	require.NoError(t, err)
	_, err = attach(t, service, filesystem, workspace2, pb.Mount_MODE_READ_ONLY)
	require.NoError(t, err)

	detached := detach(t, service, filesystem, workspace1)
	requireAttachments(t, service, detached, &pb.Attachment{Workspace: workspace2, Mode: pb.Mount_MODE_READ_ONLY})
	// Idempotent.
	grpcrequire.Equal(t, detached, detach(t, service, filesystem, workspace1))
}

func TestRunDeleteFilesystem(t *testing.T) {
	service, nodes, user := newTestService(t)
	filesystem := createFilesystem(t, service, user, "repo", pb.Filesystem_KIND_LOCAL).GetName()
	workspace := user + "/workspaces/w1"
	_, err := attach(t, service, filesystem, workspace, pb.Mount_MODE_READ_WRITE)
	require.NoError(t, err)

	deleteFilesystemRequest := &filesystemservicepb.DeleteFilesystemRequest{Name: filesystem}
	_, err = service.RunDeleteFilesystem(context.Background(), deleteFilesystemRequest)
	grpcrequire.Error(t, codes.FailedPrecondition, err)
	require.Empty(t, nodes.purges)

	detach(t, service, filesystem, workspace)
	createSnapshotRequest := &filesystemservicepb.CreateSnapshotRequest{Parent: filesystem, Snapshot: &pb.Snapshot{Trigger: pb.Snapshot_TRIGGER_IDLE}}
	_, err = service.RunCreateSnapshot(jobContext(newJob()), createSnapshotRequest)
	require.NoError(t, err)

	_, err = service.RunDeleteFilesystem(context.Background(), deleteFilesystemRequest)
	require.NoError(t, err)
	requireProtosEqual(t, []*nodeservicepb.PurgeFilesystemRequest{{Filesystem: filesystem}}, nodes.purges)
	require.Equal(t, []string{""}, nodes.nodes)
	getFilesystemRequest := &filesystemservicepb.GetFilesystemRequest{Name: filesystem}
	_, err = service.GetFilesystem(context.Background(), getFilesystemRequest)
	grpcrequire.Error(t, codes.NotFound, err)

	_, err = service.RunDeleteFilesystem(context.Background(), deleteFilesystemRequest)
	grpcrequire.Error(t, codes.NotFound, err)
	deleteFilesystemRequest.AllowMissing = true
	_, err = service.RunDeleteFilesystem(context.Background(), deleteFilesystemRequest)
	require.NoError(t, err)
}

func TestRunCreateSnapshot(t *testing.T) {
	t.Run("running writer: the agent on its node snapshots", func(t *testing.T) {
		service, nodes, user := newTestService(t)
		filesystem := createFilesystem(t, service, user, "repo", pb.Filesystem_KIND_LOCAL).GetName()
		workspace := createWorkspace(t, service, user, "w1", pb.Workspace_STATE_RUNNING, "node-a")
		_, err := attach(t, service, filesystem, workspace, pb.Mount_MODE_READ_WRITE)
		require.NoError(t, err)

		createSnapshotRequest := &filesystemservicepb.CreateSnapshotRequest{
			Parent:   filesystem,
			Snapshot: &pb.Snapshot{Trigger: pb.Snapshot_TRIGGER_TURN, Turn: "turn-1"},
		}
		job := newJob()
		snapshot, err := service.RunCreateSnapshot(jobContext(job), createSnapshotRequest)
		require.NoError(t, err)
		expected := &pb.Snapshot{
			Name:       snapshot.GetName(),
			CreateTime: snapshot.GetCreateTime(),
			Trigger:    pb.Snapshot_TRIGGER_TURN,
			Turn:       "turn-1",
			SizeBytes:  42,
			Added:      []string{"main.go"},
		}
		grpcrequire.Equal(t, expected, snapshot)
		require.Equal(t, []string{"node-a"}, nodes.nodes)
		requireProtosEqual(t, []*nodeservicepb.SnapshotFilesystemRequest{{
			Snapshot:  &pb.Snapshot{Name: snapshot.GetName(), Trigger: pb.Snapshot_TRIGGER_TURN, Turn: "turn-1"},
			RequestId: uuid.NewV5(snapshotNamespace, job).String(),
		}}, nodes.snapshots)

		getFilesystemRequest := &filesystemservicepb.GetFilesystemRequest{Name: filesystem}
		stored, err := service.GetFilesystem(context.Background(), getFilesystemRequest)
		require.NoError(t, err)
		require.Equal(t, snapshot.GetName(), stored.GetLatestSnapshot())

		// A retry of the job returns its first attempt's snapshot.
		retried, err := service.RunCreateSnapshot(jobContext(job), createSnapshotRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, snapshot, retried)
	})

	t.Run("no running writer: empty, without the agent", func(t *testing.T) {
		service, nodes, user := newTestService(t)
		filesystem := createFilesystem(t, service, user, "repo", pb.Filesystem_KIND_LOCAL).GetName()
		workspace := createWorkspace(t, service, user, "w1", pb.Workspace_STATE_STOPPED, "")
		_, err := attach(t, service, filesystem, workspace, pb.Mount_MODE_READ_WRITE)
		require.NoError(t, err)

		createSnapshotRequest := &filesystemservicepb.CreateSnapshotRequest{Parent: filesystem, Snapshot: &pb.Snapshot{Trigger: pb.Snapshot_TRIGGER_IDLE}}
		snapshot, err := service.RunCreateSnapshot(jobContext(newJob()), createSnapshotRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, &pb.Snapshot{Name: snapshot.GetName(), CreateTime: snapshot.GetCreateTime(), Trigger: pb.Snapshot_TRIGGER_IDLE}, snapshot)
		require.Empty(t, nodes.nodes)
	})

	t.Run("SHARED has no snapshots", func(t *testing.T) {
		service, _, user := newTestService(t)
		filesystem := createFilesystem(t, service, user, "shared", pb.Filesystem_KIND_SHARED).GetName()
		createSnapshotRequest := &filesystemservicepb.CreateSnapshotRequest{Parent: filesystem, Snapshot: &pb.Snapshot{Trigger: pb.Snapshot_TRIGGER_IDLE}}
		_, err := service.RunCreateSnapshot(jobContext(newJob()), createSnapshotRequest)
		grpcrequire.Error(t, codes.FailedPrecondition, err)
	})
}

func TestRunCreateSnapshotOutsideJob(t *testing.T) {
	service, _, user := newTestService(t)
	filesystem := createFilesystem(t, service, user, "repo", pb.Filesystem_KIND_LOCAL).GetName()
	createSnapshotRequest := &filesystemservicepb.CreateSnapshotRequest{Parent: filesystem, Snapshot: &pb.Snapshot{Trigger: pb.Snapshot_TRIGGER_IDLE}}
	_, err := service.RunCreateSnapshot(context.Background(), createSnapshotRequest)
	grpcrequire.Error(t, codes.FailedPrecondition, err)
}

// newJob names a job of its own.
func newJob() string {
	return "jobs/" + uuid.MustNewV7().String()
}

// jobContext is the context the scheduler runs a job in.
func jobContext(job string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(scheduler.JobMetadataKey, job))
}

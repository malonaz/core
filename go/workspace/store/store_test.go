package store

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/workspace/model"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	"github.com/malonaz/core/go/postgres"
	testserver "github.com/malonaz/core/go/postgres/test_server"
	"github.com/malonaz/core/go/uuid"
)

// migrationsDirectory holds the workspace database's migrations, provided to the test as data.
const migrationsDirectory = "go/workspace/migrations"

// testPostgresClient is shared by every test: isolate tests by organization.
var testPostgresClient *postgres.Client

func TestMain(m *testing.M) {
	testserver.RunWithPostgres(context.Background(), m, slog.Default(), &testPostgresClient, nil, "", os.ReadFile, migrationsDirectory)
}

// testTime is a time Postgres's TIMESTAMP keeps exactly: microseconds, UTC.
func testTime(minute int) *timestamppb.Timestamp {
	return timestamppb.New(time.Date(2026, 11, 27, 10, minute, 0, 0, time.UTC))
}

// requireProtoEqual compares protos rather than models: JSONB reformats the json bytes it stores.
func requireProtoEqual(t *testing.T, expected, actual any) {
	t.Helper()
	diff := cmp.Diff(expected, actual, protocmp.Transform())
	require.Empty(t, diff, diff)
}

func newWorkspace(t *testing.T, name string, minute int) *pb.Workspace {
	t.Helper()
	return &pb.Workspace{
		Name:       name,
		CreateTime: testTime(minute),
		UpdateTime: testTime(minute),
		Etag:       "etag-1",
		Labels:     map[string]string{"root-agent": "a1"},
		Mounts: []*pb.Mount{
			{Filesystem: "organizations/o/users/u/filesystems/repo", Path: "/workspace", Mode: pb.Mount_MODE_READ_WRITE},
			{Filesystem: "organizations/o/users/u/filesystems/shared", Path: "/shared/me", Mode: pb.Mount_MODE_READ_ONLY},
		},
		State: pb.Workspace_STATE_STOPPED,
	}
}

func insertWorkspaces(t *testing.T, store *Store, workspaces ...*pb.Workspace) []*pb.Workspace {
	t.Helper()
	requestIDs := make([]string, len(workspaces))
	models := make([]*model.Workspace, len(workspaces))
	for i, workspace := range workspaces {
		requestIDs[i] = uuid.MustNewV7().String()
		workspaceModel, err := model.WorkspaceFromPb(workspace)
		require.NoError(t, err)
		models[i] = workspaceModel
	}
	inserted, err := store.BatchInsertWorkspaces(context.Background(), requestIDs, models)
	require.NoError(t, err)
	return workspacesToPb(t, inserted)
}

func workspacesToPb(t *testing.T, workspaces []*model.Workspace) []*pb.Workspace {
	t.Helper()
	pbs := make([]*pb.Workspace, len(workspaces))
	for i, workspace := range workspaces {
		workspacePb, err := workspace.ToPb()
		require.NoError(t, err)
		pbs[i] = workspacePb
	}
	return pbs
}

func TestWorkspace(t *testing.T) {
	ctx := context.Background()
	store := New(testPostgresClient)
	first := newWorkspace(t, "organizations/workspace/users/u/workspaces/first", 1)
	// No labels and no mounts: nullable labels and an empty mounts array.
	second := &pb.Workspace{
		Name:       "organizations/workspace/users/u/workspaces/second",
		CreateTime: testTime(2),
		UpdateTime: testTime(2),
		Etag:       "etag-1",
		State:      pb.Workspace_STATE_STOPPED,
	}
	inserted := insertWorkspaces(t, store, first, second)
	requireProtoEqual(t, []*pb.Workspace{first, second}, inserted)

	t.Run("get", func(t *testing.T) {
		got, err := store.GetWorkspace(ctx, "workspace", "u", "first")
		require.NoError(t, err)
		requireProtoEqual(t, []*pb.Workspace{first}, workspacesToPb(t, []*model.Workspace{got}))

		_, err = store.GetWorkspace(ctx, "workspace", "u", "missing")
		require.ErrorIs(t, err, model.ErrWorkspaceNotExist)
	})

	t.Run("insert existing", func(t *testing.T) {
		workspaceModel, err := model.WorkspaceFromPb(first)
		require.NoError(t, err)
		_, err = store.BatchInsertWorkspaces(ctx, []string{uuid.MustNewV7().String()}, []*model.Workspace{workspaceModel})
		require.ErrorIs(t, err, model.ErrWorkspaceAlreadyExists)
	})

	t.Run("list", func(t *testing.T) {
		listed, err := store.ListWorkspaces(ctx, "workspace", "u", "", "ORDER BY create_time DESC", "", nil)
		require.NoError(t, err)
		requireProtoEqual(t, []*pb.Workspace{second, first}, workspacesToPb(t, listed))
	})

	t.Run("update", func(t *testing.T) {
		running := newWorkspace(t, first.Name, 1)
		running.UpdateTime = testTime(3)
		running.Etag = "etag-2"
		running.State = pb.Workspace_STATE_RUNNING
		running.Node = "node-1"
		running.StartTime = testTime(3)
		running.LastActiveTime = testTime(4)
		runningModel, err := model.WorkspaceFromPb(running)
		require.NoError(t, err)
		columns := []string{"update_time", "etag", "state", "node", "start_time", "last_active_time"}
		clause := "update_time = $1, etag = $2, state = $3, node = $4, start_time = $5, last_active_time = $6"

		_, err = store.UpdateWorkspace(ctx, runningModel, clause, columns, "stale")
		require.ErrorIs(t, err, model.ErrWorkspaceETagChanged)

		updated, err := store.UpdateWorkspace(ctx, runningModel, clause, columns, "etag-1")
		require.NoError(t, err)
		requireProtoEqual(t, []*pb.Workspace{running}, workspacesToPb(t, []*model.Workspace{updated}))

		// The idle reaper's scan: running workspaces idle since before a cutoff.
		idle, err := store.ListWorkspaces(ctx, "workspace", "-", "WHERE state = $1 AND last_active_time < $2", "ORDER BY last_active_time", "", nil, int16(pb.Workspace_STATE_RUNNING), testTime(5).AsTime())
		require.NoError(t, err)
		requireProtoEqual(t, []*pb.Workspace{running}, workspacesToPb(t, idle))
	})

	t.Run("delete", func(t *testing.T) {
		_, err := store.DeleteWorkspace(ctx, "workspace", "u", "second", "stale")
		require.ErrorIs(t, err, model.ErrWorkspaceETagChanged)

		deleted, err := store.DeleteWorkspace(ctx, "workspace", "u", "second", "etag-1")
		require.NoError(t, err)
		requireProtoEqual(t, []*pb.Workspace{second}, workspacesToPb(t, []*model.Workspace{deleted}))

		_, err = store.GetWorkspace(ctx, "workspace", "u", "second")
		require.ErrorIs(t, err, model.ErrWorkspaceNotExist)
	})
}

func filesystemToPb(t *testing.T, filesystem *model.Filesystem) *pb.Filesystem {
	t.Helper()
	filesystemPb, err := filesystem.ToPb()
	require.NoError(t, err)
	return filesystemPb
}

func snapshotsToPb(t *testing.T, snapshots []*model.Snapshot) []*pb.Snapshot {
	t.Helper()
	pbs := make([]*pb.Snapshot, len(snapshots))
	for i, snapshot := range snapshots {
		snapshotPb, err := snapshot.ToPb()
		require.NoError(t, err)
		pbs[i] = snapshotPb
	}
	return pbs
}

func TestFilesystemAndSnapshots(t *testing.T) {
	ctx := context.Background()
	store := New(testPostgresClient)
	filesystem := &pb.Filesystem{
		Name:       "organizations/filesystem/users/u/filesystems/repo",
		CreateTime: testTime(1),
		UpdateTime: testTime(1),
		Etag:       "etag-1",
		Labels:     map[string]string{"root-agent": "a1"},
		Kind:       pb.Filesystem_KIND_LOCAL,
		QuotaBytes: 10 << 30,
		Attachments: []*pb.Attachment{
			{Workspace: "organizations/filesystem/users/u/workspaces/w", Mode: pb.Mount_MODE_READ_WRITE},
		},
	}
	filesystemModel, err := model.FilesystemFromPb(filesystem)
	require.NoError(t, err)
	inserted, err := store.BatchInsertFilesystems(ctx, []string{uuid.MustNewV7().String()}, []*model.Filesystem{filesystemModel})
	require.NoError(t, err)
	requireProtoEqual(t, filesystem, filesystemToPb(t, inserted[0]))

	// One snapshot with changed files, one with none: empty arrays, not NULL.
	turnSnapshot := &pb.Snapshot{
		Name:       filesystem.Name + "/snapshots/s1",
		CreateTime: testTime(2),
		Trigger:    pb.Snapshot_TRIGGER_TURN,
		Turn:       "turn-1",
		SizeBytes:  1024,
		Added:      []string{"README.md", "go/main.go"},
		Changed:    []string{"BUILD"},
		Deleted:    []string{"old.txt"},
	}
	idleSnapshot := &pb.Snapshot{
		Name:       filesystem.Name + "/snapshots/s2",
		CreateTime: testTime(3),
		Trigger:    pb.Snapshot_TRIGGER_IDLE,
		Added:      []string{},
		Changed:    []string{},
		Deleted:    []string{},
	}
	snapshotModels := make([]*model.Snapshot, 0, 2)
	for _, snapshot := range []*pb.Snapshot{turnSnapshot, idleSnapshot} {
		snapshotModel, err := model.SnapshotFromPb(snapshot)
		require.NoError(t, err)
		snapshotModels = append(snapshotModels, snapshotModel)
	}
	insertedSnapshots, err := store.BatchInsertSnapshots(ctx, []string{uuid.MustNewV7().String(), uuid.MustNewV7().String()}, snapshotModels)
	require.NoError(t, err)
	requireProtoEqual(t, []*pb.Snapshot{turnSnapshot, idleSnapshot}, snapshotsToPb(t, insertedSnapshots))

	listed, err := store.ListSnapshots(ctx, "filesystem", "u", "repo", "", "ORDER BY create_time DESC", "", nil)
	require.NoError(t, err)
	requireProtoEqual(t, []*pb.Snapshot{idleSnapshot, turnSnapshot}, snapshotsToPb(t, listed))

	// The latest snapshot and usage, as filesystem-manager records them.
	updatedFilesystem := filesystemToPb(t, inserted[0])
	updatedFilesystem.UpdateTime = testTime(3)
	updatedFilesystem.Etag = "etag-2"
	updatedFilesystem.UsedBytes = 2048
	updatedFilesystem.LatestSnapshot = idleSnapshot.Name
	updatedFilesystemModel, err := model.FilesystemFromPb(updatedFilesystem)
	require.NoError(t, err)
	updated, err := store.UpdateFilesystem(ctx, updatedFilesystemModel,
		"update_time = $1, etag = $2, used_bytes = $3, latest_snapshot = $4",
		[]string{"update_time", "etag", "used_bytes", "latest_snapshot"}, "etag-1")
	require.NoError(t, err)
	requireProtoEqual(t, updatedFilesystem, filesystemToPb(t, updated))

	// Deleting a filesystem with snapshots needs force, which deletes them with it.
	_, err = store.DeleteFilesystem(ctx, "filesystem", "u", "repo", "", false)
	require.ErrorIs(t, err, model.ErrFilesystemHasChildren)
	_, err = store.DeleteFilesystem(ctx, "filesystem", "u", "repo", "", true)
	require.NoError(t, err)
	_, err = store.GetSnapshot(ctx, "filesystem", "u", "repo", "s1")
	require.ErrorIs(t, err, model.ErrSnapshotNotExist)
}

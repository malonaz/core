package workspace_manager_service

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/malonaz/core/gengo/workspace/store"
	schedulerservicepb "github.com/malonaz/core/genproto/scheduler/scheduler_service/v1"
	schedulerpb "github.com/malonaz/core/genproto/scheduler/v1"
	filesystemservicepb "github.com/malonaz/core/genproto/workspace/filesystem_service/v1"
	pb "github.com/malonaz/core/genproto/workspace/v1"
	workspacemanagerservicepb "github.com/malonaz/core/genproto/workspace/workspace_manager_service/v1"
	workspaceservicepb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/postgres"
	testserver "github.com/malonaz/core/go/postgres/test_server"
	"github.com/malonaz/core/go/scheduler"
	"github.com/malonaz/core/go/uuid"
	"github.com/malonaz/core/go/workspace/pod"
)

const (
	// migrationsDirectory holds the workspace database's migrations, provided to the test as data.
	migrationsDirectory = "go/workspace/migrations"
	testNamespace       = "workspaces"
	testNode            = "node-a"
)

// testPostgresClient is shared by every test: isolate tests by user.
var testPostgresClient *postgres.Client

func TestMain(m *testing.M) {
	testserver.RunWithPostgres(context.Background(), m, slog.Default(), &testPostgresClient, nil, "", os.ReadFile, migrationsDirectory)
}

// call is a call a fake recorded.
type call struct {
	method    string
	name      string
	workspace string
	mode      pb.Mount_Mode
}

// fakeFilesystems is the FilesystemService: it records attachments and snapshots, and refuses
// the filesystems in refuse.
type fakeFilesystems struct {
	filesystemservicepb.FilesystemServiceClient
	mutex       sync.Mutex
	filesystems map[string]*pb.Filesystem
	refuse      map[string]bool
	calls       []call
	snapshots   []*filesystemservicepb.CreateSnapshotRequest
}

func (f *fakeFilesystems) AttachFilesystem(ctx context.Context, request *filesystemservicepb.AttachFilesystemRequest, _ ...grpc.CallOption) (*pb.Filesystem, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.calls = append(f.calls, call{method: "attach", name: request.GetName(), workspace: request.GetWorkspace(), mode: request.GetMode()})
	if f.refuse[request.GetName()] {
		return nil, status.Errorf(codes.FailedPrecondition, "written by another workspace").Err()
	}
	return f.filesystems[request.GetName()], nil
}

func (f *fakeFilesystems) DetachFilesystem(ctx context.Context, request *filesystemservicepb.DetachFilesystemRequest, _ ...grpc.CallOption) (*pb.Filesystem, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.calls = append(f.calls, call{method: "detach", name: request.GetName(), workspace: request.GetWorkspace()})
	return f.filesystems[request.GetName()], nil
}

func (f *fakeFilesystems) GetFilesystem(ctx context.Context, request *filesystemservicepb.GetFilesystemRequest, _ ...grpc.CallOption) (*pb.Filesystem, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	filesystem, ok := f.filesystems[request.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "filesystem does not exist").Err()
	}
	return filesystem, nil
}

// CreateSnapshot returns a done operation.
func (f *fakeFilesystems) CreateSnapshot(ctx context.Context, request *filesystemservicepb.CreateSnapshotRequest, _ ...grpc.CallOption) (*longrunningpb.Operation, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.snapshots = append(f.snapshots, request)
	snapshot := &pb.Snapshot{Name: request.GetParent() + "/snapshots/s" + strconv.Itoa(len(f.snapshots))}
	response, err := anypb.New(snapshot)
	if err != nil {
		return nil, err
	}
	return &longrunningpb.Operation{Name: "operations/" + snapshot.GetName(), Done: true, Result: &longrunningpb.Operation_Response{Response: response}}, nil
}

// fakeScheduler records the jobs operations start.
type fakeScheduler struct {
	schedulerservicepb.SchedulerServiceClient
	mutex   sync.Mutex
	created []*schedulerservicepb.CreateJobRequest
}

func (f *fakeScheduler) CreateJob(ctx context.Context, request *schedulerservicepb.CreateJobRequest, _ ...grpc.CallOption) (*schedulerpb.Job, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.created = append(f.created, request)
	return &schedulerpb.Job{Name: request.GetParent() + "/jobs/j" + strconv.Itoa(len(f.created))}, nil
}

// fakeSecrets is every pod's SecretService.
type fakeSecrets struct {
	workspaceservicepb.SecretServiceClient
	mutex sync.Mutex
	pods  []string
	sets  []*workspaceservicepb.SetSecretsRequest
}

func (f *fakeSecrets) dial(ctx context.Context, workspacePod *corev1.Pod) (workspaceservicepb.SecretServiceClient, func(), error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.pods = append(f.pods, workspacePod.Name)
	return f, func() {}, nil
}

func (f *fakeSecrets) SetSecrets(ctx context.Context, request *workspaceservicepb.SetSecretsRequest, _ ...grpc.CallOption) (*workspaceservicepb.SetSecretsResponse, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.sets = append(f.sets, request)
	return &workspaceservicepb.SetSecretsResponse{}, nil
}

type testEnvironment struct {
	service     *Service
	clientset   *fake.Clientset
	filesystems *fakeFilesystems
	scheduler   *fakeScheduler
	secrets     *fakeSecrets
	user        string
	repo        string
	shared      string
}

// newTestEnvironment returns a service over the test database and fakes, with a LOCAL repo and a
// SHARED filesystem of a user of its own. A kubelet runs every pod created on testNode.
func newTestEnvironment(t *testing.T) *testEnvironment {
	t.Helper()
	user := "organizations/o/users/" + uuid.MustNewV7().String()
	repo, shared := user+"/filesystems/repo", user+"/filesystems/shared"
	filesystems := &fakeFilesystems{
		filesystems: map[string]*pb.Filesystem{
			repo:   {Name: repo, Kind: pb.Filesystem_KIND_LOCAL},
			shared: {Name: shared, Kind: pb.Filesystem_KIND_SHARED},
		},
		refuse: map[string]bool{},
	}
	scheduler := &fakeScheduler{}
	opts := &Opts{
		Namespace:        testNamespace,
		Image:            "workspace-server:1",
		Port:             9090,
		RuntimeClassName: "gvisor",
		CSIDriver:        "workspaces.onikisu",
		CPURequest:       "250m",
		CPULimit:         "2",
		MemoryRequest:    "512Mi",
		MemoryLimit:      "2Gi",
		TmpSizeLimit:     "4Gi",
		IdleTimeout:      30 * time.Minute,
		MaxRunDuration:   12 * time.Hour,
		ReapInterval:     time.Minute,
	}
	service, err := New(opts, store.New(testPostgresClient), scheduler, filesystems)
	require.NoError(t, err)
	clientset := fake.NewClientset()
	secrets := &fakeSecrets{}
	service.pods = pod.NewPods(clientset, testNamespace)
	service.dialSecrets = secrets.dial
	runKubelet(t, clientset)
	return &testEnvironment{
		service:     service,
		clientset:   clientset,
		filesystems: filesystems,
		scheduler:   scheduler,
		secrets:     secrets,
		user:        user,
		repo:        repo,
		shared:      shared,
	}
}

// runKubelet schedules every pod created on testNode and marks it ready.
func runKubelet(t *testing.T, clientset *fake.Clientset) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pods := clientset.CoreV1().Pods(testNamespace)
	watcher, err := pods.Watch(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	go func() {
		defer watcher.Stop()
		for event := range watcher.ResultChan() {
			created, ok := event.Object.(*corev1.Pod)
			if event.Type != watch.Added || !ok {
				continue
			}
			created = created.DeepCopy()
			created.Spec.NodeName = testNode
			scheduled, err := pods.Update(ctx, created, metav1.UpdateOptions{})
			if err != nil {
				continue
			}
			scheduled.Status = corev1.PodStatus{
				Phase:      corev1.PodRunning,
				PodIP:      "10.0.0.7",
				StartTime:  &metav1.Time{Time: time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)},
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			}
			pods.UpdateStatus(ctx, scheduled, metav1.UpdateOptions{})
		}
	}()
}

func (e *testEnvironment) createWorkspace(t *testing.T, id string) (*pb.Workspace, error) {
	t.Helper()
	createWorkspaceRequest := &workspacemanagerservicepb.CreateWorkspaceRequest{
		Parent:      e.user,
		WorkspaceId: id,
		Workspace: &pb.Workspace{Mounts: []*pb.Mount{
			{Filesystem: e.repo, Path: "/workspace", Mode: pb.Mount_MODE_READ_WRITE},
			{Filesystem: e.shared, Path: "/shared", Mode: pb.Mount_MODE_READ_ONLY},
		}},
	}
	return e.service.CreateWorkspace(context.Background(), createWorkspaceRequest)
}

func (e *testEnvironment) startWorkspace(t *testing.T, name string) *pb.Workspace {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	startWorkspaceRequest := &workspacemanagerservicepb.StartWorkspaceRequest{Name: name}
	workspace, err := e.service.RunStartWorkspace(ctx, startWorkspaceRequest)
	require.NoError(t, err)
	return workspace
}

func (e *testEnvironment) getWorkspace(t *testing.T, name string) (*pb.Workspace, error) {
	t.Helper()
	getWorkspaceRequest := &workspacemanagerservicepb.GetWorkspaceRequest{Name: name}
	return e.service.GetWorkspace(context.Background(), getWorkspaceRequest)
}

func (e *testEnvironment) podNames(t *testing.T) []string {
	t.Helper()
	podList, err := e.clientset.CoreV1().Pods(testNamespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	var names []string
	for _, listed := range podList.Items {
		names = append(names, listed.Name)
	}
	return names
}

// requireProtosEqual compares values holding protos.
func requireProtosEqual(t *testing.T, expected, actual any) {
	t.Helper()
	diff := cmp.Diff(expected, actual, protocmp.Transform(), cmp.AllowUnexported(call{}))
	require.Empty(t, diff, diff)
}

func TestCreateWorkspace(t *testing.T) {
	t.Run("attaches its mounts, STOPPED", func(t *testing.T) {
		environment := newTestEnvironment(t)
		workspace, err := environment.createWorkspace(t, "w1")
		require.NoError(t, err)
		require.Equal(t, pb.Workspace_STATE_STOPPED, workspace.GetState())
		requireProtosEqual(t, []call{
			{method: "attach", name: environment.repo, workspace: workspace.GetName(), mode: pb.Mount_MODE_READ_WRITE},
			{method: "attach", name: environment.shared, workspace: workspace.GetName(), mode: pb.Mount_MODE_READ_ONLY},
		}, environment.filesystems.calls)
		require.Empty(t, environment.podNames(t))
	})

	t.Run("a refused writer undoes the create", func(t *testing.T) {
		environment := newTestEnvironment(t)
		environment.filesystems.refuse[environment.repo] = true
		_, err := environment.createWorkspace(t, "w1")
		grpcrequire.Error(t, codes.FailedPrecondition, err)

		name := environment.user + "/workspaces/w1"
		_, err = environment.getWorkspace(t, name)
		grpcrequire.Error(t, codes.NotFound, err)
		requireProtosEqual(t, []call{
			{method: "attach", name: environment.repo, workspace: name, mode: pb.Mount_MODE_READ_WRITE},
			{method: "detach", name: environment.repo, workspace: name},
			{method: "detach", name: environment.shared, workspace: name},
		}, environment.filesystems.calls)
	})
}

func TestWorkspaceLifecycle(t *testing.T) {
	environment := newTestEnvironment(t)
	created, err := environment.createWorkspace(t, "w1")
	require.NoError(t, err)
	name := created.GetName()
	podName := pod.Name(name)

	// Start: the pod runs, and the workspace records it.
	running := environment.startWorkspace(t, name)
	expected := &pb.Workspace{
		Name:           name,
		CreateTime:     running.GetCreateTime(),
		UpdateTime:     running.GetUpdateTime(),
		Etag:           running.GetEtag(),
		Mounts:         created.GetMounts(),
		State:          pb.Workspace_STATE_RUNNING,
		Node:           testNode,
		StartTime:      running.GetStartTime(),
		LastActiveTime: running.GetLastActiveTime(),
	}
	grpcrequire.Equal(t, expected, running)
	require.Equal(t, time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC), running.GetStartTime().AsTime())
	require.Equal(t, []string{podName}, environment.podNames(t))
	grpcrequire.Equal(t, running, environment.startWorkspace(t, name))

	// Secrets reach the running pod.
	secrets := []*workspaceservicepb.Secret{{Id: "github", EnvVar: "GITHUB_TOKEN", Value: []byte("token")}}
	setWorkspaceSecretsRequest := &workspacemanagerservicepb.SetWorkspaceSecretsRequest{Name: name, Secrets: secrets}
	setWorkspaceSecretsResponse, err := environment.service.SetWorkspaceSecrets(context.Background(), setWorkspaceSecretsRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &workspacemanagerservicepb.SetWorkspaceSecretsResponse{Applied: true}, setWorkspaceSecretsResponse)
	require.Equal(t, []string{podName}, environment.secrets.pods)
	requireProtosEqual(t, []*workspaceservicepb.SetSecretsRequest{{Secrets: secrets}}, environment.secrets.sets)

	// A turn snapshots the LOCAL read-write mount only.
	snapshotWorkspaceRequest := &workspacemanagerservicepb.SnapshotWorkspaceRequest{Name: name, Turn: "turn-1"}
	snapshotWorkspaceResponse, err := environment.service.RunSnapshotWorkspace(jobContext("jobs/snapshot"), snapshotWorkspaceRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &workspacemanagerservicepb.SnapshotWorkspaceResponse{Snapshots: []string{environment.repo + "/snapshots/s1"}}, snapshotWorkspaceResponse)

	// Stop: a last snapshot, then the pod goes.
	stopWorkspaceRequest := &workspacemanagerservicepb.StopWorkspaceRequest{Name: name}
	stopped, err := environment.service.RunStopWorkspace(jobContext("jobs/stop"), stopWorkspaceRequest)
	require.NoError(t, err)
	expected = &pb.Workspace{
		Name:           name,
		CreateTime:     stopped.GetCreateTime(),
		UpdateTime:     stopped.GetUpdateTime(),
		Etag:           stopped.GetEtag(),
		Mounts:         created.GetMounts(),
		State:          pb.Workspace_STATE_STOPPED,
		LastActiveTime: running.GetLastActiveTime(),
	}
	grpcrequire.Equal(t, expected, stopped)
	require.Empty(t, environment.podNames(t))
	requireProtosEqual(t, []*filesystemservicepb.CreateSnapshotRequest{
		{
			Parent:    environment.repo,
			Snapshot:  &pb.Snapshot{Trigger: pb.Snapshot_TRIGGER_TURN, Turn: "turn-1"},
			RequestId: uuid.NewV5(snapshotNamespace, "jobs/snapshot/"+environment.repo).String(),
		},
		{
			Parent:    environment.repo,
			Snapshot:  &pb.Snapshot{Trigger: pb.Snapshot_TRIGGER_STOP},
			RequestId: uuid.NewV5(snapshotNamespace, "jobs/stop/"+environment.repo).String(),
		},
	}, environment.filesystems.snapshots)

	// Stopping again, or setting secrets once stopped, does nothing.
	again, err := environment.service.RunStopWorkspace(jobContext("jobs/stop"), stopWorkspaceRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, stopped, again)
	setWorkspaceSecretsResponse, err = environment.service.SetWorkspaceSecrets(context.Background(), setWorkspaceSecretsRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &workspacemanagerservicepb.SetWorkspaceSecretsResponse{}, setWorkspaceSecretsResponse)
}

func TestRunStartWorkspaceLostPod(t *testing.T) {
	environment := newTestEnvironment(t)
	created, err := environment.createWorkspace(t, "w1")
	require.NoError(t, err)
	environment.startWorkspace(t, created.GetName())

	// Its node lost: the workspace still reads RUNNING, but its pod is gone.
	require.NoError(t, environment.service.pods.Delete(context.Background(), pod.Name(created.GetName())))
	restarted := environment.startWorkspace(t, created.GetName())
	require.Equal(t, pb.Workspace_STATE_RUNNING, restarted.GetState())
	require.Equal(t, []string{pod.Name(created.GetName())}, environment.podNames(t))
}

func TestUpdateWorkspaceMounts(t *testing.T) {
	environment := newTestEnvironment(t)
	created, err := environment.createWorkspace(t, "w1")
	require.NoError(t, err)
	name := created.GetName()
	environment.startWorkspace(t, name)
	environment.filesystems.calls = nil

	mounts := []*pb.Mount{{Filesystem: environment.repo, Path: "/repo", Mode: pb.Mount_MODE_READ_WRITE}}
	updateWorkspaceRequest := &workspacemanagerservicepb.UpdateWorkspaceRequest{
		Workspace:  &pb.Workspace{Name: name, Mounts: mounts},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"mounts"}},
	}
	updated, err := environment.service.UpdateWorkspace(context.Background(), updateWorkspaceRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, &pb.Workspace{Mounts: mounts, State: pb.Workspace_STATE_STARTING}, &pb.Workspace{Mounts: updated.GetMounts(), State: updated.GetState()})
	requireProtosEqual(t, []call{
		{method: "attach", name: environment.repo, workspace: name, mode: pb.Mount_MODE_READ_WRITE},
		{method: "detach", name: environment.shared, workspace: name},
	}, environment.filesystems.calls)
	// The old pod went; a StartWorkspace operation brings the new one.
	require.Empty(t, environment.podNames(t))
	require.Len(t, environment.scheduler.created, 1)
	payload, err := environment.scheduler.created[0].GetJob().GetPayload().UnmarshalNew()
	require.NoError(t, err)
	grpcrequire.Equal(t, &workspacemanagerservicepb.StartWorkspaceRequest{Name: name}, payload)

	restarted := environment.startWorkspace(t, name)
	require.Equal(t, pb.Workspace_STATE_RUNNING, restarted.GetState())

	// A labels change leaves the pod alone.
	updateWorkspaceRequest = &workspacemanagerservicepb.UpdateWorkspaceRequest{
		Workspace:  &pb.Workspace{Name: name, Labels: map[string]string{"agent": "a1"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}
	_, err = environment.service.UpdateWorkspace(context.Background(), updateWorkspaceRequest)
	require.NoError(t, err)
	require.Equal(t, []string{pod.Name(name)}, environment.podNames(t))
}

func TestDeleteWorkspace(t *testing.T) {
	environment := newTestEnvironment(t)
	created, err := environment.createWorkspace(t, "w1")
	require.NoError(t, err)
	name := created.GetName()
	environment.startWorkspace(t, name)
	environment.filesystems.calls = nil

	deleteWorkspaceRequest := &workspacemanagerservicepb.DeleteWorkspaceRequest{Name: name}
	_, err = environment.service.DeleteWorkspace(context.Background(), deleteWorkspaceRequest)
	require.NoError(t, err)
	require.Empty(t, environment.podNames(t))
	requireProtosEqual(t, []call{
		{method: "detach", name: environment.repo, workspace: name},
		{method: "detach", name: environment.shared, workspace: name},
	}, environment.filesystems.calls)
	_, err = environment.getWorkspace(t, name)
	grpcrequire.Error(t, codes.NotFound, err)

	deleteWorkspaceRequest.AllowMissing = true
	_, err = environment.service.DeleteWorkspace(context.Background(), deleteWorkspaceRequest)
	require.NoError(t, err)
}

func TestReap(t *testing.T) {
	environment := newTestEnvironment(t)
	now := time.Now()
	// reapable creates a running workspace whose pod was created and last used at those times.
	reapable := func(id string, createTime, lastActive time.Time) *corev1.Pod {
		created, err := environment.createWorkspace(t, id)
		require.NoError(t, err)
		environment.startWorkspace(t, created.GetName())
		pods := environment.clientset.CoreV1().Pods(testNamespace)
		workspacePod, err := pods.Get(context.Background(), pod.Name(created.GetName()), metav1.GetOptions{})
		require.NoError(t, err)
		workspacePod.UID = types.UID(id)
		workspacePod.CreationTimestamp = metav1.NewTime(createTime)
		workspacePod.Annotations[pod.AnnotationLastActive] = lastActive.UTC().Format(time.RFC3339)
		workspacePod, err = pods.Update(context.Background(), workspacePod, metav1.UpdateOptions{})
		require.NoError(t, err)
		return workspacePod
	}
	active := reapable("active", now.Add(-time.Hour), now.Add(-time.Minute))
	idle := reapable("idle", now.Add(-time.Hour), now.Add(-31*time.Minute))
	overrun := reapable("overrun", now.Add(-13*time.Hour), now.Add(-time.Minute))
	// An orphan: its workspace was deleted without it.
	orphan := reapable("orphan", now.Add(-time.Hour), now.Add(-time.Hour))
	_, err := environment.service.WorkspaceManagerServiceServer.DeleteWorkspace(context.Background(), &workspacemanagerservicepb.DeleteWorkspaceRequest{Name: orphan.Annotations[pod.AnnotationWorkspace]})
	require.NoError(t, err)

	require.NoError(t, environment.service.reap(context.Background()))
	require.NoError(t, environment.service.reap(context.Background()))

	var stopped []*workspacemanagerservicepb.StopWorkspaceRequest
	for _, created := range environment.scheduler.created {
		payload, err := created.GetJob().GetPayload().UnmarshalNew()
		require.NoError(t, err)
		stopped = append(stopped, payload.(*workspacemanagerservicepb.StopWorkspaceRequest))
	}
	stopRequest := func(workspacePod *corev1.Pod) *workspacemanagerservicepb.StopWorkspaceRequest {
		return &workspacemanagerservicepb.StopWorkspaceRequest{
			Name:      workspacePod.Annotations[pod.AnnotationWorkspace],
			RequestId: uuid.NewV5(reapNamespace, string(workspacePod.UID)).String(),
		}
	}
	// Each pass starts the same operation: its request id derives from the pod.
	slices.SortStableFunc(stopped, func(a, b *workspacemanagerservicepb.StopWorkspaceRequest) int {
		return strings.Compare(a.GetName(), b.GetName())
	})
	requireProtosEqual(t, []*workspacemanagerservicepb.StopWorkspaceRequest{stopRequest(idle), stopRequest(idle), stopRequest(overrun), stopRequest(overrun)}, stopped)
	require.ElementsMatch(t, []string{active.Name, idle.Name, overrun.Name}, environment.podNames(t))
}

// jobContext is the context the scheduler runs a job in.
func jobContext(job string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(scheduler.JobMetadataKey, job))
}

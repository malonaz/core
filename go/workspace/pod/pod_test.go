package pod

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	pb "github.com/malonaz/core/genproto/workspace/v1"
	"github.com/malonaz/core/go/grpc"
)

const (
	testNamespace     = "workspaces"
	testWorkspaceName = "organizations/o/users/u/workspaces/w"
	repoFilesystem    = "organizations/o/users/u/filesystems/repo"
	sharedFilesystem  = "organizations/o/users/u/filesystems/shared"
)

func testConfig() *Config {
	return &Config{
		Namespace:        testNamespace,
		Image:            "workspace-server:1",
		Port:             9090,
		RuntimeClassName: "gvisor",
		CSIDriver:        "workspaces.onikisu",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
		},
		TmpSizeLimit: resource.MustParse("1Gi"),
		Env:          []corev1.EnvVar{{Name: "WORKSPACE_SERVICE_GRPC_DISABLE_TLS", Value: "true"}},
	}
}

func testWorkspace() *pb.Workspace {
	return &pb.Workspace{
		Name: testWorkspaceName,
		Mounts: []*pb.Mount{
			{Filesystem: repoFilesystem, Path: "/workspace", Mode: pb.Mount_MODE_READ_WRITE},
			{Filesystem: sharedFilesystem, Path: "/shared/me", Mode: pb.Mount_MODE_READ_ONLY},
		},
	}
}

var testFilesystemNameToKind = map[string]pb.Filesystem_Kind{
	repoFilesystem:   pb.Filesystem_KIND_LOCAL,
	sharedFilesystem: pb.Filesystem_KIND_SHARED,
}

func TestName(t *testing.T) {
	name := Name(testWorkspaceName)
	require.Len(t, name, len("ws-")+20)
	require.Equal(t, name, Name(testWorkspaceName))
	require.NotEqual(t, name, Name("organizations/o/users/other/workspaces/w"))
}

func TestSpec(t *testing.T) {
	t.Run("mounts", func(t *testing.T) {
		pod, err := Spec(testConfig(), testWorkspace(), testFilesystemNameToKind)
		require.NoError(t, err)

		readWrite, readOnly, disabled := false, true, false
		runtimeClassName := "gvisor"
		tmpSizeLimit, runSizeLimit := resource.MustParse("1Gi"), resource.MustParse("16Mi")
		expected := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        Name(testWorkspaceName),
				Namespace:   testNamespace,
				Labels:      map[string]string{LabelName: "workspace"},
				Annotations: map[string]string{AnnotationWorkspace: testWorkspaceName},
			},
			Spec: corev1.PodSpec{
				RuntimeClassName:             &runtimeClassName,
				AutomountServiceAccountToken: &disabled,
				EnableServiceLinks:           &disabled,
				Containers: []corev1.Container{{
					Name:  "workspace-server",
					Image: "workspace-server:1",
					Env: []corev1.EnvVar{
						{Name: "WORKSPACE_SERVICE_DIRECTORY", Value: "/workspace"},
						{Name: "WORKSPACE_SERVICE_GRPC_PORT", Value: "9090"},
						{Name: "BASH_ENV", Value: "/run/workspace/env"},
						{Name: "WORKSPACE_SERVICE_GRPC_DISABLE_TLS", Value: "true"},
					},
					Ports: []corev1.ContainerPort{{Name: "grpc", ContainerPort: 9090}},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "tmp", MountPath: "/tmp"},
						{Name: "run", MountPath: "/run/workspace", ReadOnly: true},
						{Name: "mount-0", MountPath: "/workspace"},
						{Name: "mount-1", MountPath: "/shared/me", ReadOnly: true},
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("grpc")}},
						PeriodSeconds: 1,
					},
				}},
				Volumes: []corev1.Volume{
					{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &tmpSizeLimit}}},
					{Name: "run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: &runSizeLimit}}},
					{Name: "mount-0", VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
						Driver:           "workspaces.onikisu",
						ReadOnly:         &readWrite,
						VolumeAttributes: map[string]string{"filesystem": repoFilesystem, "kind": "local", "readOnly": "false"},
					}}},
					{Name: "mount-1", VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
						Driver:           "workspaces.onikisu",
						ReadOnly:         &readOnly,
						VolumeAttributes: map[string]string{"filesystem": sharedFilesystem, "kind": "shared", "readOnly": "true"},
					}}},
				},
			},
		}
		require.Equal(t, expected, pod)
	})

	t.Run("no mounts runs in /tmp", func(t *testing.T) {
		workspace := &pb.Workspace{Name: testWorkspaceName}
		pod, err := Spec(testConfig(), workspace, nil)
		require.NoError(t, err)
		require.Equal(t, corev1.EnvVar{Name: "WORKSPACE_SERVICE_DIRECTORY", Value: "/tmp"}, pod.Spec.Containers[0].Env[0])
		require.Len(t, pod.Spec.Volumes, 2)
	})

	t.Run("sidecar writes the run directory", func(t *testing.T) {
		config := testConfig()
		config.Sidecar = &corev1.Container{Name: "sidecar", Image: "sidecar:1"}
		pod, err := Spec(config, testWorkspace(), testFilesystemNameToKind)
		require.NoError(t, err)
		expected := corev1.Container{
			Name:         "sidecar",
			Image:        "sidecar:1",
			VolumeMounts: []corev1.VolumeMount{{Name: "run", MountPath: "/run/workspace"}},
		}
		require.Equal(t, expected, pod.Spec.Containers[1])
		require.Empty(t, config.Sidecar.VolumeMounts)
	})

	t.Run("unknown filesystem kind", func(t *testing.T) {
		_, err := Spec(testConfig(), testWorkspace(), map[string]pb.Filesystem_Kind{repoFilesystem: pb.Filesystem_KIND_LOCAL})
		require.ErrorContains(t, err, "no kind for filesystem "+sharedFilesystem)
	})
}

func newTestPods(t *testing.T) (*Pods, *corev1.Pod) {
	t.Helper()
	pod, err := Spec(testConfig(), testWorkspace(), testFilesystemNameToKind)
	require.NoError(t, err)
	return NewPods(fake.NewClientset(), testNamespace), pod
}

func TestGetOrCreate(t *testing.T) {
	ctx := context.Background()
	pods, pod := newTestPods(t)

	created, err := pods.GetOrCreate(ctx, pod)
	require.NoError(t, err)
	require.Equal(t, pod.Spec, created.Spec)

	// Another service creating it first: the existing pod is returned, not replaced.
	other := pod.DeepCopy()
	other.Spec.Containers[0].Image = "workspace-server:2"
	existing, err := pods.GetOrCreate(ctx, other)
	require.NoError(t, err)
	require.Equal(t, "workspace-server:1", existing.Spec.Containers[0].Image)

	require.NoError(t, pods.Delete(ctx, pod.Name))
	require.NoError(t, pods.Delete(ctx, pod.Name))
	_, err = pods.Get(ctx, pod.Name)
	require.Error(t, err)
}

// running sets a pod's status as the kubelet does once it serves.
func running(pod *corev1.Pod, startTime time.Time) *corev1.Pod {
	pod = pod.DeepCopy()
	pod.Spec.NodeName = "node-1"
	pod.Status = corev1.PodStatus{
		Phase:      corev1.PodRunning,
		PodIP:      "10.0.0.7",
		StartTime:  &metav1.Time{Time: startTime},
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
	}
	return pod
}

func TestWaitRunning(t *testing.T) {
	startTime := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)

	t.Run("becomes ready", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pods, pod := newTestPods(t)
		created, err := pods.GetOrCreate(ctx, pod)
		require.NoError(t, err)

		go func() {
			time.Sleep(50 * time.Millisecond)
			if _, err := pods.pods.UpdateStatus(ctx, running(created, startTime), metav1.UpdateOptions{}); err != nil {
				t.Errorf("updating status: %v", err)
			}
		}()
		ready, err := pods.WaitRunning(ctx, pod.Name)
		require.NoError(t, err)

		workspace := testWorkspace()
		RecordStart(workspace, ready)
		expected := testWorkspace()
		expected.State = pb.Workspace_STATE_RUNNING
		expected.Node = "node-1"
		expected.StartTime = timestamppb.New(startTime)
		require.Equal(t, expected, workspace)
	})

	t.Run("already ready", func(t *testing.T) {
		ctx := context.Background()
		pods, pod := newTestPods(t)
		_, err := pods.GetOrCreate(ctx, running(pod, startTime))
		require.NoError(t, err)
		ready, err := pods.WaitRunning(ctx, pod.Name)
		require.NoError(t, err)
		require.Equal(t, "10.0.0.7", ready.Status.PodIP)
	})

	t.Run("failed", func(t *testing.T) {
		ctx := context.Background()
		pods, pod := newTestPods(t)
		pod.Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", Message: "disk pressure"}
		_, err := pods.GetOrCreate(ctx, pod)
		require.NoError(t, err)
		_, err = pods.WaitRunning(ctx, pod.Name)
		require.ErrorContains(t, err, "ended: Evicted disk pressure")
	})

	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		pods, pod := newTestPods(t)
		_, err := pods.GetOrCreate(ctx, pod)
		require.NoError(t, err)
		_, err = pods.WaitRunning(ctx, pod.Name)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("deleted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pods, pod := newTestPods(t)
		_, err := pods.GetOrCreate(ctx, pod)
		require.NoError(t, err)
		go func() {
			time.Sleep(50 * time.Millisecond)
			if err := pods.Delete(ctx, pod.Name); err != nil {
				t.Errorf("deleting pod: %v", err)
			}
		}()
		_, err = pods.WaitRunning(ctx, pod.Name)
		require.ErrorContains(t, err, "deleted while starting")
	})
}

func TestTouch(t *testing.T) {
	ctx := context.Background()
	pods, pod := newTestPods(t)
	created, err := pods.GetOrCreate(ctx, pod)
	require.NoError(t, err)
	_, ok := LastActiveTime(created)
	require.False(t, ok)

	now := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	touched, err := pods.Touch(ctx, created, now)
	require.NoError(t, err)
	lastActive, ok := LastActiveTime(touched)
	require.True(t, ok)
	require.Equal(t, now, lastActive)

	// Within the interval: not stamped again.
	again, err := pods.Touch(ctx, touched, now.Add(30*time.Second))
	require.NoError(t, err)
	require.Same(t, touched, again)

	later, err := pods.Touch(ctx, touched, now.Add(ActivityInterval))
	require.NoError(t, err)
	stored, err := pods.Get(ctx, pod.Name)
	require.NoError(t, err)
	require.Equal(t, later.Annotations, stored.Annotations)
	require.Equal(t, map[string]string{
		AnnotationWorkspace:  testWorkspaceName,
		AnnotationLastActive: "2026-11-27T10:01:00Z",
	}, stored.Annotations)
}

func TestDialer(t *testing.T) {
	dialer := NewDialer(9090, true, nil, nil)
	_, pod := newTestPods(t)

	_, err := dialer.ClientOpts(pod)
	require.ErrorContains(t, err, "has no IP")

	clientOpts, err := dialer.ClientOpts(running(pod, time.Now()))
	require.NoError(t, err)
	require.Equal(t, &grpc.ClientOpts{Host: "10.0.0.7", Port: 9090, DisableTLS: true}, clientOpts)
}

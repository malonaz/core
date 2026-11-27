package pod

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	pb "github.com/malonaz/core/genproto/workspace/v1"
)

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


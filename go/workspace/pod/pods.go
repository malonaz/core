package pod

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/malonaz/core/genproto/workspace/v1"
)

// ActivityInterval is how often a pod's last-active annotation is stamped at most, and how often
// KeepActive stamps it while a call stays open.
const ActivityInterval = time.Minute

// Pods reads and writes the workspace pods of one namespace, and nothing else.
type Pods struct {
	pods typedcorev1.PodInterface
}

// NewPods scopes a clientset to the pods of a namespace.
func NewPods(clientset kubernetes.Interface, namespace string) *Pods {
	return &Pods{pods: clientset.CoreV1().Pods(namespace)}
}

// Get returns a pod: a NotFound error if it does not exist.
func (p *Pods) Get(ctx context.Context, name string) (*corev1.Pod, error) {
	return p.pods.Get(ctx, name, metav1.GetOptions{})
}

// GetOrCreate creates a pod, or returns the existing one of that name: any of the services may
// create it first.
func (p *Pods) GetOrCreate(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	created, err := p.pods.Create(ctx, pod, metav1.CreateOptions{})
	if err == nil {
		return created, nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("creating pod %s: %w", pod.Name, err)
	}
	existing, err := p.Get(ctx, pod.Name)
	if err != nil {
		return nil, fmt.Errorf("getting pod %s: %w", pod.Name, err)
	}
	return existing, nil
}

// Delete deletes a pod; one already gone is not an error.
func (p *Pods) Delete(ctx context.Context, name string) error {
	if err := p.pods.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting pod %s: %w", name, err)
	}
	return nil
}

// WaitRunning waits until a pod is ready to serve, within ctx's deadline.
func (p *Pods) WaitRunning(ctx context.Context, name string) (*corev1.Pod, error) {
	// Watch before getting, so no change falls between the two.
	listOptions := metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String()}
	watcher, err := p.pods.Watch(ctx, listOptions)
	if err != nil {
		return nil, fmt.Errorf("watching pod %s: %w", name, err)
	}
	defer watcher.Stop()

	pod, err := p.Get(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("getting pod %s: %w", name, err)
	}
	for {
		if ready, err := isReady(pod); err != nil || ready {
			return pod, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for pod %s: %w", name, ctx.Err())
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return nil, fmt.Errorf("waiting for pod %s: watch closed", name)
			}
			switch event.Type {
			case watch.Deleted:
				return nil, fmt.Errorf("pod %s deleted while starting", name)
			case watch.Error:
				return nil, fmt.Errorf("watching pod %s: %w", name, apierrors.FromObject(event.Object))
			}
			if eventPod, ok := event.Object.(*corev1.Pod); ok && eventPod.Name == name {
				pod = eventPod
			}
		}
	}
}

// isReady reports whether a pod serves; a pod that ended never will.
func isReady(pod *corev1.Pod) (bool, error) {
	switch pod.Status.Phase {
	case corev1.PodFailed, corev1.PodSucceeded:
		return false, fmt.Errorf("pod %s ended: %s %s", pod.Name, pod.Status.Reason, pod.Status.Message)
	case corev1.PodRunning:
	default:
		return false, nil
	}
	if pod.Status.PodIP == "" {
		return false, nil
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue, nil
		}
	}
	return false, nil
}

// RecordStart records a running pod on its workspace: its node and when it started.
func RecordStart(workspace *pb.Workspace, pod *corev1.Pod) {
	workspace.State = pb.Workspace_STATE_RUNNING
	workspace.Node = pod.Spec.NodeName
	workspace.StartTime = nil
	if pod.Status.StartTime != nil {
		workspace.StartTime = timestamppb.New(pod.Status.StartTime.Time)
	}
}

// LastActiveTime returns when a call last used a pod, if any has.
func LastActiveTime(pod *corev1.Pod) (time.Time, bool) {
	lastActive, err := time.Parse(time.RFC3339, pod.Annotations[AnnotationLastActive])
	if err != nil {
		return time.Time{}, false
	}
	return lastActive, true
}

// Touch stamps a pod as used at now, at most once per ActivityInterval: it returns the pod as is
// when it was stamped more recently.
func (p *Pods) Touch(ctx context.Context, pod *corev1.Pod, now time.Time) (*corev1.Pod, error) {
	if lastActive, ok := LastActiveTime(pod); ok && now.Sub(lastActive) < ActivityInterval {
		return pod, nil
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{AnnotationLastActive: now.UTC().Format(time.RFC3339)},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("marshaling patch: %w", err)
	}
	patched, err := p.pods.Patch(ctx, pod.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return nil, fmt.Errorf("stamping pod %s: %w", pod.Name, err)
	}
	return patched, nil
}

// KeepActive stamps a pod every ActivityInterval until ctx is done: while an Exec or a terminal
// stays open, the reaper must not take its pod. It returns ctx's error, or the first failed stamp.
func (p *Pods) KeepActive(ctx context.Context, pod *corev1.Pod) error {
	ticker := time.NewTicker(ActivityInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			touched, err := p.Touch(ctx, pod, now)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			pod = touched
		}
	}
}

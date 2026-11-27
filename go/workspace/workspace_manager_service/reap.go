package workspace_manager_service

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	corev1 "k8s.io/api/core/v1"

	pb "github.com/malonaz/core/genproto/workspace/v1"
	workspacemanagerservicepb "github.com/malonaz/core/genproto/workspace/workspace_manager_service/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
	"github.com/malonaz/core/go/workspace/pod"
)

// reapNamespace derives a reap's stop request id from the pod: the passes until it stops start one
// operation.
var reapNamespace = uuid.MustParse("c4e8a2d6-1b3f-4f7a-9e5c-8d2b6a0f4e13")

// reap stops every workspace whose pod sat unused past the idle timeout, or ran past the most a
// pod may: an agent looping on commands never goes idle. A pod no workspace owns is deleted.
func (s *Service) reap(ctx context.Context) error {
	workspacePods, err := s.pods.List(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	for i := range workspacePods {
		workspacePod := &workspacePods[i]
		if isTerminating(workspacePod) || !s.expired(workspacePod, now) {
			continue
		}
		if err := s.reapPod(ctx, workspacePod); err != nil {
			s.log.WarnContext(ctx, "reaping workspace pod", "pod", workspacePod.Name, "error", err)
		}
	}
	return nil
}

// expired reports whether a pod sat unused past the idle timeout, or ran past the most it may.
func (s *Service) expired(workspacePod *corev1.Pod, now time.Time) bool {
	createTime := workspacePod.CreationTimestamp.Time
	lastActive, ok := pod.LastActiveTime(workspacePod)
	if !ok {
		lastActive = createTime
	}
	return now.Sub(lastActive) >= s.opts.IdleTimeout || now.Sub(createTime) >= s.opts.MaxRunDuration
}

// reapPod stops a pod's workspace.
func (s *Service) reapPod(ctx context.Context, workspacePod *corev1.Pod) error {
	name := workspacePod.Annotations[pod.AnnotationWorkspace]
	getWorkspaceRequest := &workspacemanagerservicepb.GetWorkspaceRequest{Name: name}
	workspace, err := s.GetWorkspace(ctx, getWorkspaceRequest)
	if err != nil {
		if status.HasCode(err, codes.NotFound, codes.InvalidArgument) {
			return s.pods.Delete(ctx, workspacePod.Name)
		}
		return err
	}
	// A stopped workspace's pod is left over from a stop that failed after its snapshots.
	if workspace.GetState() == pb.Workspace_STATE_STOPPED {
		return s.pods.Delete(ctx, workspacePod.Name)
	}
	stopWorkspaceRequest := &workspacemanagerservicepb.StopWorkspaceRequest{
		Name:      name,
		RequestId: uuid.NewV5(reapNamespace, string(workspacePod.UID)).String(),
	}
	_, err = s.StopWorkspace(ctx, stopWorkspaceRequest)
	return err
}

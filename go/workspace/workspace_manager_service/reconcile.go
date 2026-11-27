package workspace_manager_service

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	corev1 "k8s.io/api/core/v1"

	pb "github.com/malonaz/core/genproto/workspace/v1"
	workspacemanagerservicepb "github.com/malonaz/core/genproto/workspace/workspace_manager_service/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
	"github.com/malonaz/core/go/workspace/pod"
)

// stuckTimeout is how long a workspace may stay STARTING or STOPPING before the reconciler acts:
// longer than a start or stop takes, so it never races one in flight.
const stuckTimeout = 10 * time.Minute

var workspaceNamespace = aip.MustGetUUIDNamespace(&pb.Workspace{})

// reconcile brings pods in line with their workspaces. Pods are listed before workspaces: a
// workspace read RUNNING after its pod was found missing lost the pod, or started one since.
func (s *Service) reconcile(ctx context.Context) error {
	workspacePods, err := s.pods.List(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	podNameToPod := make(map[string]*corev1.Pod, len(workspacePods))
	for i := range workspacePods {
		workspacePod := &workspacePods[i]
		podNameToPod[workspacePod.Name] = workspacePod
		if isTerminating(workspacePod) {
			continue
		}
		if err := s.reconcilePod(ctx, workspacePod, now); err != nil {
			s.log.WarnContext(ctx, "reconciling workspace pod", "pod", workspacePod.Name, "error", err)
		}
	}
	workspaces, err := s.listActiveWorkspaces(ctx)
	if err != nil {
		return err
	}
	for _, workspace := range workspaces {
		workspacePod := podNameToPod[pod.Name(workspace.GetName())]
		if err := s.reconcileWorkspace(ctx, workspace, workspacePod, now); err != nil {
			s.log.WarnContext(ctx, "reconciling workspace", "workspace", workspace.GetName(), "error", err)
		}
	}
	return nil
}

// reconcilePod deletes a pod no running workspace owns, and stops the workspace of one unused past
// the idle timeout or alive past the most a pod may: an agent looping on commands never goes idle.
func (s *Service) reconcilePod(ctx context.Context, workspacePod *corev1.Pod, now time.Time) error {
	name := workspacePod.Annotations[pod.AnnotationWorkspace]
	getWorkspaceRequest := &workspacemanagerservicepb.GetWorkspaceRequest{Name: name}
	workspace, err := s.GetWorkspace(ctx, getWorkspaceRequest)
	if err != nil {
		if status.HasCode(err, codes.NotFound, codes.InvalidArgument) {
			return s.pods.Delete(ctx, workspacePod.Name)
		}
		return err
	}
	switch workspace.GetState() {
	case pb.Workspace_STATE_STOPPED:
		// Left over from a stop that failed after its snapshots.
		return s.pods.Delete(ctx, workspacePod.Name)
	case pb.Workspace_STATE_RUNNING:
		if !s.expired(workspacePod, now) {
			return nil
		}
		// Derived from the pod: the passes until it stops start one operation.
		return s.stopWorkspace(ctx, name, string(workspacePod.UID))
	}
	return nil
}

// reconcileWorkspace starts a RUNNING workspace whose pod is gone, restarts one whose pod has other
// mounts, stops one stuck STARTING, and
// retries the stop of one stuck STOPPING. Request ids derive from the etag, which a start or stop
// changes: the passes until then start one operation.
func (s *Service) reconcileWorkspace(ctx context.Context, workspace *pb.Workspace, workspacePod *corev1.Pod, now time.Time) error {
	stuckFor := now.Sub(workspace.GetUpdateTime().AsTime())
	switch workspace.GetState() {
	case pb.Workspace_STATE_RUNNING:
		// A terminating pod is waited out: a mounts change or a stop is replacing it.
		if workspacePod != nil {
			if isTerminating(workspacePod) || pod.HasMounts(workspacePod, workspace.GetMounts()) {
				return nil
			}
			// Built with other mounts: a mounts change failed before it restarted the pod.
			_, err := s.restartWorkspace(ctx, workspace.GetName())
			return err
		}
		startWorkspaceRequest := &workspacemanagerservicepb.StartWorkspaceRequest{
			Name:      workspace.GetName(),
			RequestId: uuid.NewV5(workspaceNamespace, workspace.GetName()+"/"+workspace.GetEtag()).String(),
		}
		_, err := s.StartWorkspace(ctx, startWorkspaceRequest)
		return err
	case pb.Workspace_STATE_STARTING:
		// A start this late failed: it stops, rather than retrying forever.
		if stuckFor < stuckTimeout {
			return nil
		}
		return s.stopWorkspace(ctx, workspace.GetName(), workspace.GetEtag())
	case pb.Workspace_STATE_STOPPING:
		// A stop must finish, so it is retried once per stuckTimeout.
		if stuckFor < stuckTimeout {
			return nil
		}
		attempt := strconv.Itoa(int(stuckFor / stuckTimeout))
		return s.stopWorkspace(ctx, workspace.GetName(), workspace.GetEtag()+"/"+attempt)
	}
	return nil
}

// stopWorkspace starts a workspace's stop, with a request id derived from key.
func (s *Service) stopWorkspace(ctx context.Context, name, key string) error {
	stopWorkspaceRequest := &workspacemanagerservicepb.StopWorkspaceRequest{
		Name:      name,
		RequestId: uuid.NewV5(workspaceNamespace, name+"/"+key).String(),
	}
	_, err := s.StopWorkspace(ctx, stopWorkspaceRequest)
	return err
}

// listActiveWorkspaces lists every workspace STARTING, RUNNING or STOPPING, across owners.
func (s *Service) listActiveWorkspaces(ctx context.Context) ([]*pb.Workspace, error) {
	states := []int16{int16(pb.Workspace_STATE_STARTING), int16(pb.Workspace_STATE_RUNNING), int16(pb.Workspace_STATE_STOPPING)}
	workspaceModels, err := s.workspacePostgresStore.ListWorkspaces(ctx, "-", "-", "WHERE workspace.state = ANY($1)", "", "", nil, states)
	if err != nil {
		return nil, status.FromError(err, "listing workspaces").Err()
	}
	workspaces := make([]*pb.Workspace, len(workspaceModels))
	for i, workspaceModel := range workspaceModels {
		if workspaces[i], err = workspaceModel.ToPb(); err != nil {
			return nil, status.Errorf(codes.Internal, "converting workspace from model to pb: %v", err).Err()
		}
	}
	return workspaces, nil
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

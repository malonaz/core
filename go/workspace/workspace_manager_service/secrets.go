package workspace_manager_service

import (
	"context"

	"google.golang.org/grpc/codes"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	pb "github.com/malonaz/core/genproto/workspace/v1"
	workspacemanagerservicepb "github.com/malonaz/core/genproto/workspace/workspace_manager_service/v1"
	workspaceservicepb "github.com/malonaz/core/genproto/workspace/workspace_service/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/workspace/pod"
)

// SetWorkspaceSecrets implements the interface: the pod's workspace-server writes them.
func (s *Service) SetWorkspaceSecrets(ctx context.Context, request *workspacemanagerservicepb.SetWorkspaceSecretsRequest) (*workspacemanagerservicepb.SetWorkspaceSecretsResponse, error) {
	getWorkspaceRequest := &workspacemanagerservicepb.GetWorkspaceRequest{Name: request.GetName()}
	workspace, err := s.GetWorkspace(ctx, getWorkspaceRequest)
	if err != nil {
		return nil, err
	}
	if workspace.GetState() != pb.Workspace_STATE_RUNNING {
		return &workspacemanagerservicepb.SetWorkspaceSecretsResponse{}, nil
	}
	workspacePod, err := s.pods.Get(ctx, pod.Name(workspace.GetName()))
	if err != nil {
		if apierrors.IsNotFound(err) {
			return &workspacemanagerservicepb.SetWorkspaceSecretsResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "getting pod: %v", err).Err()
	}
	if isTerminating(workspacePod) {
		return &workspacemanagerservicepb.SetWorkspaceSecretsResponse{}, nil
	}

	client, closeClient, err := s.dialSecrets(ctx, workspacePod)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "reaching workspace: %v", err).Err()
	}
	defer closeClient()
	setSecretsRequest := &workspaceservicepb.SetSecretsRequest{Secrets: request.GetSecrets()}
	if _, err := client.SetSecrets(ctx, setSecretsRequest); err != nil {
		return nil, status.FromError(err, "setting secrets").Err()
	}
	return &workspacemanagerservicepb.SetWorkspaceSecretsResponse{Applied: true}, nil
}

package agent_service

import (
	"context"

	"google.golang.org/grpc/codes"
	"slices"
	"strings"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// CreateDefinition creates a definition, granting platform tools or its organization's.
func (s *Service) CreateDefinition(ctx context.Context, request *agentservicepb.CreateDefinitionRequest) (*agentpb.Definition, error) {
	if err := s.validateTools(request.Parent, request.Definition.GetSpec()); err != nil {
		return nil, err
	}
	return s.AgentServiceServer.CreateDefinition(ctx, request)
}

// UpdateDefinition updates a definition, granting platform tools or its organization's.
func (s *Service) UpdateDefinition(ctx context.Context, request *agentservicepb.UpdateDefinitionRequest) (*agentpb.Definition, error) {
	updatesSpec := slices.ContainsFunc(request.GetUpdateMask().GetPaths(), func(path string) bool {
		return path == "*" || path == "spec" || strings.HasPrefix(path, "spec.")
	})
	if updatesSpec {
		definitionRn, err := agentpb.ParseDefinitionRn(request.Definition.GetName())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "parsing definition name: %v", err).Err()
		}
		if err := s.validateTools(definitionRn.OrganizationRn().String(), request.Definition.GetSpec()); err != nil {
			return nil, err
		}
	}
	return s.AgentServiceServer.UpdateDefinition(ctx, request)
}

package agent_service

import (
	"context"
	"slices"
	"strings"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
)

// CreateDefinition creates a definition, whose tools' services must have targets.
func (s *Service) CreateDefinition(ctx context.Context, request *agentservicepb.CreateDefinitionRequest) (*agentpb.Definition, error) {
	if err := s.validateTools(request.Definition.GetSpec()); err != nil {
		return nil, err
	}
	return s.AgentServiceServer.CreateDefinition(ctx, request)
}

// UpdateDefinition updates a definition, whose new tools' services must have targets.
func (s *Service) UpdateDefinition(ctx context.Context, request *agentservicepb.UpdateDefinitionRequest) (*agentpb.Definition, error) {
	updatesSpec := slices.ContainsFunc(request.GetUpdateMask().GetPaths(), func(path string) bool {
		return path == "*" || path == "spec" || strings.HasPrefix(path, "spec.")
	})
	if updatesSpec {
		if err := s.validateTools(request.Definition.GetSpec()); err != nil {
			return nil, err
		}
	}
	return s.AgentServiceServer.UpdateDefinition(ctx, request)
}

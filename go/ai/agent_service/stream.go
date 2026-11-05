package agent_service

import (
	"google.golang.org/grpc/codes"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// StreamAgent streams an agent's live activity.
func (s *Service) StreamAgent(request *agentservicepb.StreamAgentRequest, srv agentservicepb.AgentService_StreamAgentServer) error {
	return status.Errorf(codes.Unimplemented, "unimplemented").Err()
}

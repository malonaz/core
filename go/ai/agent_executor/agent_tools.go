package agent_executor

import (
	pb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
)

// newAgentTools returns the built-in agent tools. Placeholder: owned by the turn-loop task.
func newAgentTools(agentServiceClient pb.AgentServiceClient) []Tool { return nil }

package agent_service

import (
	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
)

// hub fans this replica's live agent activity out to its StreamAgent calls.
type hub struct{}

func newHub() *hub {
	return &hub{}
}

// publish sends a piece of an agent's activity, keyed by response.Agent, to the streams subscribed to it.
func (h *hub) publish(response *agentservicepb.StreamAgentResponse) {}

package agent_executor

import (
	"context"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
)

// runTurn runs one turn of the agent. Placeholder: owned by the turn-loop task.
func (a *agent) runTurn(ctx context.Context, inputs []*agentpb.Input) {}

package agent_service

import (
	"testing"

	"github.com/stretchr/testify/require"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

func TestTurn_InputStartsATurnThatEndsIdle(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	service.aiService.script(agent.Chat, textGeneration("hello back"))

	sendText(t, service, agent.Name, "hello")

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	messages := service.aiService.messages(agent.Chat)
	require.Equal(t, []aipb.Role{aipb.Role_ROLE_SYSTEM, aipb.Role_ROLE_USER, aipb.Role_ROLE_ASSISTANT}, roles(messages))
	inputs, ok := agentpb.Annotations.Inputs.Get(messages[1])
	require.True(t, ok)
	require.NotEmpty(t, inputs)
}

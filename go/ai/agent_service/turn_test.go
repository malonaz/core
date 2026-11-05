package agent_service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
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

func TestTurn_DeliveryNamesAMissingSenderAgent(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	service.aiService.script(agent.Chat, textGeneration("noted"))
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	missingAgent := agentRn.UserRn().AgentRn("missing").String()
	message := ai.NewUserMessage(ai.NewTextBlock("hello"))
	agentpb.Annotations.Sender.Set(message, missingAgent)
	createInputRequest := &agentservicepb.CreateInputRequest{Parent: agent.Name, Input: &agentpb.Input{Message: message}}
	input, err := service.CreateInput(context.Background(), createInputRequest)
	require.NoError(t, err)

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	require.Equal(t, []string{"[Message from agent " + missingAgent + "]", "hello"}, deliveredTexts(t, service, agent.Chat, input.Name))
}

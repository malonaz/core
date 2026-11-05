package agent_service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

func TestAgent_CreateFromADefinitionSnapshotsItsSpec(t *testing.T) {
	service := newTestService(t)
	user := newTestUser(t)
	userRn, err := agentpb.ParseUserRn(user)
	require.NoError(t, err)
	createDefinitionRequest := &agentservicepb.CreateDefinitionRequest{
		Parent:     userRn.OrganizationRn().String(),
		Definition: &agentpb.Definition{Title: "writer", Spec: &agentpb.AgentSpec{Instructions: "You write.", Model: testModel}},
	}
	definition, err := service.CreateDefinition(context.Background(), createDefinitionRequest)
	require.NoError(t, err)
	createAgentRequest := &agentservicepb.CreateAgentRequest{
		Parent:    user,
		Agent:     &agentpb.Agent{Title: "writer", Definition: definition.Name},
		RequestId: uuid.MustNewV7().String(),
	}

	agent, err := service.CreateAgent(context.Background(), createAgentRequest)
	require.NoError(t, err)
	replayedAgent, err := service.CreateAgent(context.Background(), createAgentRequest)
	require.NoError(t, err)

	require.Equal(t, agent.Name, replayedAgent.Name)
	require.Equal(t, "You write.", agent.Spec.Instructions)
	require.Equal(t, agent.Name, agent.RootAgent)
	require.Equal(t, agentpb.Agent_STATE_IDLE, agent.State)
	require.NotEmpty(t, agent.Chat)

	// Editing the definition never changes the agent.
	updateDefinitionRequest := &agentservicepb.UpdateDefinitionRequest{
		Definition: &agentpb.Definition{Name: definition.Name, Title: "editor", Spec: &agentpb.AgentSpec{Instructions: "You edit.", Model: testModel}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title", "spec"}},
	}
	_, err = service.UpdateDefinition(context.Background(), updateDefinitionRequest)
	require.NoError(t, err)
	require.Equal(t, "You write.", getAgent(t, service, agent.Name).Spec.Instructions)
}

func TestAgent_CreateInputReplaysItsRequestID(t *testing.T) {
	service := newTestService(t)
	service.aiService.setDefaultGeneration(textGeneration("ok"))
	agent := createTestAgent(t, service)
	createInputRequest := &agentservicepb.CreateInputRequest{
		Parent:    agent.Name,
		Input:     &agentpb.Input{Message: ai.NewUserMessage(ai.NewTextBlock("once"))},
		RequestId: uuid.MustNewV7().String(),
	}

	input, err := service.CreateInput(context.Background(), createInputRequest)
	require.NoError(t, err)
	replayedInput, err := service.CreateInput(context.Background(), createInputRequest)
	require.NoError(t, err)

	require.Equal(t, input.Name, replayedInput.Name)
	require.Len(t, listInputs(t, service, agent.Name), 1)
}

func TestAgent_DeleteNeedsForceForSubAgents(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	subAgent := createTestSubAgent(t, service, agent)

	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name}
	_, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.True(t, status.HasCode(err, codes.FailedPrecondition))

	deleteAgentRequest.Force = true
	_, err = service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)
	require.NotNil(t, getAgent(t, service, subAgent.Name).DeleteTime)

	// A sub-agent comes back only after its parent.
	undeleteAgentRequest := &agentservicepb.UndeleteAgentRequest{Name: subAgent.Name}
	_, err = service.UndeleteAgent(context.Background(), undeleteAgentRequest)
	require.True(t, status.HasCode(err, codes.FailedPrecondition))
	undeleteAgentRequest.Name = agent.Name
	undeletedAgent, err := service.UndeleteAgent(context.Background(), undeleteAgentRequest)
	require.NoError(t, err)
	require.Nil(t, undeletedAgent.DeleteTime)
}

func TestAgent_UpdateTitleAndLabels(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	updateAgentRequest := &agentservicepb.UpdateAgentRequest{
		Agent:      &agentpb.Agent{Name: agent.Name, Title: "renamed", Labels: map[string]string{"team": "core"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title", "labels"}},
	}

	updatedAgent, err := service.UpdateAgent(context.Background(), updateAgentRequest)

	require.NoError(t, err)
	require.Equal(t, "renamed", updatedAgent.Title)
	require.Equal(t, map[string]string{"team": "core"}, updatedAgent.Labels)
	require.NotEqual(t, agent.Etag, updatedAgent.Etag)
}

package agent_service

import (
	"context"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
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
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	chat := service.aiService.chat(agent.Chat)
	require.Equal(t, map[string]string{agentpb.Labels.Agent.GetKey(): agentRn.Agent}, chat.Labels)
	require.NoError(t, protovalidate.Validate(chat))

	// Editing the definition never changes the agent.
	updateDefinitionRequest := &agentservicepb.UpdateDefinitionRequest{
		Definition: &agentpb.Definition{Name: definition.Name, Title: "editor", Spec: &agentpb.AgentSpec{Instructions: "You edit.", Model: testModel}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title", "spec"}},
	}
	_, err = service.UpdateDefinition(context.Background(), updateDefinitionRequest)
	require.NoError(t, err)
	require.Equal(t, "You write.", getAgent(t, service, agent.Name).Spec.Instructions)
}

func TestAgent_MessageAgentReplaysItsRequestID(t *testing.T) {
	// Without room to run turns, the messages stay queued.
	service := newIdleReplica(t, newFakeAiService())
	agent := createTestAgent(t, service)
	messageAgentRequest := &agentservicepb.MessageAgentRequest{
		Name:      agent.Name,
		Message:   ai.NewUserMessage(ai.NewTextBlock("once")),
		RequestId: uuid.MustNewV7().String(),
	}

	messagedAgent, err := service.MessageAgent(context.Background(), messageAgentRequest)
	require.NoError(t, err)
	replayedAgent, err := service.MessageAgent(context.Background(), messageAgentRequest)
	require.NoError(t, err)

	require.Len(t, messagedAgent.QueuedMessages, 1)
	require.Equal(t, agentpb.Agent_STATE_RUNNING, messagedAgent.State)
	require.Len(t, replayedAgent.QueuedMessages, 1)
	require.Equal(t, messagedAgent.QueuedMessages[0].Name, replayedAgent.QueuedMessages[0].Name)
	require.Equal(t, messagedAgent.Etag, replayedAgent.Etag)
}

func TestAgent_MessageAgentStampsTheOwningUser(t *testing.T) {
	service := newIdleReplica(t, newFakeAiService())
	agent := createTestAgent(t, service)
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)

	messagedAgent := sendText(t, service, agent.Name, "hello")
	sender, _ := agentpb.Annotations.Sender.Get(messagedAgent.QueuedMessages[0])
	require.Equal(t, agentRn.UserRn().String(), sender)

	forged := ai.NewUserMessage(ai.NewTextBlock("hello"))
	agentpb.Annotations.Sender.Set(forged, agent.Name)
	messageAgentRequest := &agentservicepb.MessageAgentRequest{Name: agent.Name, Message: forged}
	_, err = service.MessageAgent(context.Background(), messageAgentRequest)
	require.True(t, status.HasCode(err, codes.InvalidArgument))
}

func TestAgent_MessageAgentCapsTheQueue(t *testing.T) {
	service := newIdleReplica(t, newFakeAiService())
	agent := createTestAgent(t, service)

	oversized := ai.NewUserMessage(ai.NewTextBlock(strings.Repeat("x", maxQueuedMessageBytes)))
	messageAgentRequest := &agentservicepb.MessageAgentRequest{Name: agent.Name, Message: oversized}
	_, err := service.MessageAgent(context.Background(), messageAgentRequest)
	require.True(t, status.HasCode(err, codes.InvalidArgument))

	for range maxQueuedMessages {
		sendText(t, service, agent.Name, "hello")
	}
	messageAgentRequest.Message = ai.NewUserMessage(ai.NewTextBlock("one too many"))
	_, err = service.MessageAgent(context.Background(), messageAgentRequest)
	require.True(t, status.HasCode(err, codes.ResourceExhausted))
}

func TestAgent_MessageAgentRefusesOtherRoles(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	messageAgentRequest := &agentservicepb.MessageAgentRequest{
		Name:    agent.Name,
		Message: ai.NewAssistantMessage(ai.NewTextBlock("not mine")),
	}

	_, err := service.MessageAgent(context.Background(), messageAgentRequest)

	require.True(t, status.HasCode(err, codes.InvalidArgument))
	require.Empty(t, sentMessages(t, service, agent.Name))
}

func TestAgent_DeleteNeedsForceForSubAgents(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	subAgent := createTestSubAgent(t, service, agent)

	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name}
	_, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.True(t, status.HasCode(err, codes.FailedPrecondition))
	require.Nil(t, getAgent(t, service, agent.Name).DeleteTime)

	deleteAgentRequest.Force = true
	_, err = service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)
	require.NotNil(t, getAgent(t, service, subAgent.Name).DeleteTime)

	// A sub-agent comes back only after its parent.
	undeleteAgentRequest := &agentservicepb.UndeleteAgentRequest{Name: subAgent.Name}
	_, err = service.UndeleteAgent(context.Background(), undeleteAgentRequest)
	require.True(t, status.HasCode(err, codes.FailedPrecondition))
}

func TestAgent_UndeleteRestoresTheForcedDelete(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	subAgent := createTestSubAgent(t, service, agent)
	subSubAgent := createTestSubAgent(t, service, subAgent)
	// Deleted on its own beforehand, with its own sub-agent.
	deletedAgent := createTestSubAgent(t, service, agent)
	deletedSubAgent := createTestSubAgent(t, service, deletedAgent)
	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: deletedAgent.Name, Force: true}
	_, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)
	deleteAgentRequest = &agentservicepb.DeleteAgentRequest{Name: agent.Name, Force: true}
	_, err = service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)

	undeleteAgentRequest := &agentservicepb.UndeleteAgentRequest{Name: agent.Name}
	undeletedAgent, err := service.UndeleteAgent(context.Background(), undeleteAgentRequest)

	require.NoError(t, err)
	require.Nil(t, undeletedAgent.DeleteTime)
	for _, restoredAgent := range []*agentpb.Agent{subAgent, subSubAgent} {
		require.Nil(t, getAgent(t, service, restoredAgent.Name).DeleteTime, restoredAgent.Name)
	}
	for _, keptAgent := range []*agentpb.Agent{deletedAgent, deletedSubAgent} {
		require.NotNil(t, getAgent(t, service, keptAgent.Name).DeleteTime, keptAgent.Name)
	}
	_, err = service.UndeleteAgent(context.Background(), undeleteAgentRequest)
	require.True(t, status.HasCode(err, codes.AlreadyExists), err)

	// The earlier delete comes back as its own unit.
	undeleteAgentRequest.Name = deletedAgent.Name
	_, err = service.UndeleteAgent(context.Background(), undeleteAgentRequest)
	require.NoError(t, err)
	require.Nil(t, getAgent(t, service, deletedSubAgent.Name).DeleteTime)
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

func TestAgent_CreateAgentRequestNamesDefinitionOrSpec(t *testing.T) {
	user := "organizations/o/users/u"
	both := &agentservicepb.CreateAgentRequest{Parent: user, Agent: &agentpb.Agent{Definition: "organizations/o/definitions/d", Spec: &agentpb.AgentSpec{Model: testModel}}}
	require.ErrorContains(t, protovalidate.Validate(both), "exactly one of agent.definition and agent.spec")
	neither := &agentservicepb.CreateAgentRequest{Parent: user, Agent: &agentpb.Agent{}}
	require.Error(t, protovalidate.Validate(neither))
	spec := &agentservicepb.CreateAgentRequest{Parent: user, Agent: &agentpb.Agent{Spec: &agentpb.AgentSpec{Model: testModel}}}
	require.NoError(t, protovalidate.Validate(spec))
}

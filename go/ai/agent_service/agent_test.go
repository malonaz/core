package agent_service

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// newTestSpec returns an ad-hoc spec no test executor can run: agents stay pending unless a test connects one.
func newTestSpec(subAgentDefinitions ...string) *agentpb.AgentSpec {
	return &agentpb.AgentSpec{
		Model:               "providers/mock/models/mock",
		Instructions:        "Be helpful.",
		SubAgentDefinitions: subAgentDefinitions,
	}
}

// createTestDefinition creates a definition in the user's organization.
func createTestDefinition(t *testing.T, service *Service, userRn *agentpb.UserRn, spec *agentpb.AgentSpec) *agentpb.Definition {
	t.Helper()
	createDefinitionRequest := &agentservicepb.CreateDefinitionRequest{
		Parent:     userRn.OrganizationRn().String(),
		Definition: &agentpb.Definition{DisplayName: "Definition", Spec: spec},
	}
	definition, err := service.CreateDefinition(context.Background(), createDefinitionRequest)
	require.NoError(t, err)
	return definition
}

// createTestAgentRPC creates an agent of a user through CreateAgent, ad-hoc unless mutate sets a definition.
func createTestAgentRPC(t *testing.T, service *Service, userRn *agentpb.UserRn, mutate func(agent *agentpb.Agent)) *agentpb.Agent {
	t.Helper()
	agent := &agentpb.Agent{Title: "Agent", Spec: newTestSpec()}
	if mutate != nil {
		mutate(agent)
	}
	createAgentRequest := &agentservicepb.CreateAgentRequest{Parent: userRn.String(), Agent: agent}
	createdAgent, err := service.CreateAgent(context.Background(), createAgentRequest)
	require.NoError(t, err)
	return createdAgent
}

// requireEventLog checks an agent's log is dense from 1 and ends at its last_event_sequence, and returns its kinds.
func requireEventLog(t *testing.T, service *Service, name string) []string {
	t.Helper()
	events := listTestEvents(t, service, name)
	for i, event := range events {
		require.Equal(t, int64(i+1), event.Sequence)
		require.Equal(t, name+"/events/"+strconv.Itoa(i+1), event.Name)
	}
	require.Equal(t, int64(len(events)), getTestAgent(t, service, name).LastEventSequence)
	return eventKinds(events)
}

func requireCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	require.Error(t, err)
	require.True(t, status.HasCode(err, code), "expected %s, got %v", code, err)
}

func TestCreateAgent_AdHoc(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	labels := map[string]string{"team": "core"}
	agent := createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) {
		agent.Labels = labels
		// Output only: ignored.
		agent.State = agentpb.Agent_STATE_RUNNING
		agent.LeasedExecutor = userRn.ExecutorRn("executor").String()
		agent.LastEventSequence = 42
	})

	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	require.Equal(t, userRn.String(), agentRn.UserRn().String())
	require.Equal(t, agentpb.Agent_STATE_PENDING, agent.State)
	require.Empty(t, agent.LeasedExecutor)
	require.Equal(t, agent.Name, agent.RootAgent)
	require.Equal(t, labels, agent.Labels)
	require.True(t, proto.Equal(newTestSpec(), agent.Spec))
	require.NotNil(t, agent.CreateTime)
	require.NotEmpty(t, agent.Etag)
	require.Equal(t, int64(1), agent.LastEventSequence)
	require.Equal(t, []string{"state_changed:STATE_PENDING"}, requireEventLog(t, service, agent.Name))

	createChatRequests := testAiService(service).CreateChatRequests()
	require.Len(t, createChatRequests, 1)
	require.Equal(t, userRn.String(), createChatRequests[0].Parent)
	require.Equal(t, "Agent", createChatRequests[0].Chat.Title)
	require.Empty(t, createChatRequests[0].Chat.Name)
	chatRn, err := agentpb.ParseUserRn(createChatRequests[0].Parent)
	require.NoError(t, err)
	require.Contains(t, agent.Chat, chatRn.String()+"/chats/")
}

func TestCreateAgent_FromDefinitionSnapshotsSpec(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	definition := createTestDefinition(t, service, userRn, newTestSpec())
	agent := createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) {
		agent.Spec = nil
		agent.Definition = definition.Name
	})
	require.Equal(t, definition.Name, agent.Definition)
	require.True(t, proto.Equal(definition.Spec, agent.Spec))

	// Editing the definition never changes the agent.
	updatedSpec := newTestSpec()
	updatedSpec.Instructions = "Be terse."
	updateDefinitionRequest := &agentservicepb.UpdateDefinitionRequest{
		Definition: &agentpb.Definition{Name: definition.Name, Spec: updatedSpec},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}},
	}
	_, err := service.UpdateDefinition(context.Background(), updateDefinitionRequest)
	require.NoError(t, err)
	require.True(t, proto.Equal(definition.Spec, getTestAgent(t, service, agent.Name).Spec))

	// A deleted definition or another organization's is refused.
	deleteDefinitionRequest := &agentservicepb.DeleteDefinitionRequest{Name: definition.Name}
	_, err = service.DeleteDefinition(context.Background(), deleteDefinitionRequest)
	require.NoError(t, err)
	createAgentRequest := &agentservicepb.CreateAgentRequest{Parent: userRn.String(), Agent: &agentpb.Agent{Definition: definition.Name}}
	_, err = service.CreateAgent(context.Background(), createAgentRequest)
	requireCode(t, err, codes.FailedPrecondition)
	otherDefinition := createTestDefinition(t, service, newTestUser(t), newTestSpec())
	createAgentRequest = &agentservicepb.CreateAgentRequest{Parent: userRn.String(), Agent: &agentpb.Agent{Definition: otherDefinition.Name}}
	_, err = service.CreateAgent(context.Background(), createAgentRequest)
	requireCode(t, err, codes.InvalidArgument)
}

func TestCreateAgent_RejectsDefinitionAndSpec(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	definition := createTestDefinition(t, service, userRn, newTestSpec())
	createAgentRequest := &agentservicepb.CreateAgentRequest{
		Parent: userRn.String(),
		Agent:  &agentpb.Agent{Definition: definition.Name, Spec: newTestSpec()},
	}
	_, err := service.CreateAgent(context.Background(), createAgentRequest)
	requireCode(t, err, codes.InvalidArgument)
	require.Empty(t, testAiService(service).CreateChatRequests())
}

func TestCreateAgent_SubAgent(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	root := createTestAgentRPC(t, service, userRn, nil)
	child := createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) { agent.ParentAgent = root.Name })
	grandchild := createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) { agent.ParentAgent = child.Name })

	require.Equal(t, root.Name, child.RootAgent)
	require.Equal(t, root.Name, grandchild.RootAgent)
	require.Equal(t, child.Name, grandchild.ParentAgent)
	require.Equal(t, []string{"state_changed:STATE_PENDING", "sub_agent_created"}, requireEventLog(t, service, root.Name))
	require.Equal(t, []string{"state_changed:STATE_PENDING", "sub_agent_created"}, requireEventLog(t, service, child.Name))
	require.Equal(t, []string{"state_changed:STATE_PENDING"}, requireEventLog(t, service, grandchild.Name))
	require.Equal(t, child.Name, listTestEvents(t, service, root.Name)[1].GetSubAgentCreated().Agent)

	// The parent must exist, be live and be the same user's.
	createAgentRequest := &agentservicepb.CreateAgentRequest{
		Parent: userRn.String(),
		Agent:  &agentpb.Agent{Spec: newTestSpec(), ParentAgent: userRn.AgentRn("missing").String()},
	}
	_, err := service.CreateAgent(context.Background(), createAgentRequest)
	requireCode(t, err, codes.NotFound)
	otherUserAgent := createTestAgentRPC(t, service, userRn.OrganizationRn().UserRn("other"), nil)
	createAgentRequest.Agent.ParentAgent = otherUserAgent.Name
	_, err = service.CreateAgent(context.Background(), createAgentRequest)
	requireCode(t, err, codes.InvalidArgument)
	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: grandchild.Name}
	_, err = service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)
	createAgentRequest.Agent.ParentAgent = grandchild.Name
	_, err = service.CreateAgent(context.Background(), createAgentRequest)
	requireCode(t, err, codes.FailedPrecondition)
}

func TestCreateAgent_SubAgentDefinitions(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	allowed := createTestDefinition(t, service, userRn, newTestSpec())
	other := createTestDefinition(t, service, userRn, newTestSpec())
	parent := createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) { agent.Spec = newTestSpec(allowed.Name) })

	createAgentRequest := &agentservicepb.CreateAgentRequest{
		Parent: userRn.String(),
		Agent:  &agentpb.Agent{Definition: other.Name, ParentAgent: parent.Name},
	}
	_, err := service.CreateAgent(context.Background(), createAgentRequest)
	requireCode(t, err, codes.FailedPrecondition)

	createAgentRequest.Agent.Definition = allowed.Name
	child, err := service.CreateAgent(context.Background(), createAgentRequest)
	require.NoError(t, err)
	require.True(t, proto.Equal(allowed.Spec, child.Spec))

	// Ad-hoc sub-agents are always allowed.
	createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) { agent.ParentAgent = parent.Name })
	require.Equal(t, []string{"state_changed:STATE_PENDING", "sub_agent_created", "sub_agent_created"}, requireEventLog(t, service, parent.Name))
}

func TestCreateAgent_ReplayIsIdempotent(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	parent := createTestAgentRPC(t, service, userRn, nil)
	createAgentRequest := &agentservicepb.CreateAgentRequest{
		Parent:    userRn.String(),
		Agent:     &agentpb.Agent{Spec: newTestSpec(), ParentAgent: parent.Name},
		RequestId: uuid.MustNewV7().String(),
	}
	agent, err := service.CreateAgent(context.Background(), proto.CloneOf(createAgentRequest))
	require.NoError(t, err)
	// Server-generated ids differ across calls: the request id alone identifies the replay.
	replayedAgent, err := service.CreateAgent(context.Background(), proto.CloneOf(createAgentRequest))
	require.NoError(t, err)
	require.True(t, proto.Equal(agent, replayedAgent))

	require.Equal(t, []string{"state_changed:STATE_PENDING"}, requireEventLog(t, service, agent.Name))
	require.Equal(t, []string{"state_changed:STATE_PENDING", "sub_agent_created"}, requireEventLog(t, service, parent.Name))
	createChatRequests := testAiService(service).CreateChatRequests()
	require.Len(t, createChatRequests, 3)
	require.Equal(t, createChatRequests[1].RequestId, createChatRequests[2].RequestId)
	require.Equal(t, uuid.NewV5(aip.MustGetUUIDNamespace(&agentpb.Agent{}), createAgentRequest.RequestId).String(), createChatRequests[1].RequestId)

	// The same id under another request is a conflict.
	createAgentRequest.AgentId = "fixed"
	createAgentRequest.RequestId = uuid.MustNewV7().String()
	_, err = service.CreateAgent(context.Background(), proto.CloneOf(createAgentRequest))
	require.NoError(t, err)
	createAgentRequest.RequestId = uuid.MustNewV7().String()
	_, err = service.CreateAgent(context.Background(), proto.CloneOf(createAgentRequest))
	requireCode(t, err, codes.AlreadyExists)
}

func TestCreateAgent_RejectsAnotherUsersExecutorPin(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	for _, executorRn := range []*agentpb.ExecutorRn{
		userRn.OrganizationRn().UserRn("other").ExecutorRn("executor"),
		newTestUser(t).ExecutorRn("executor"),
	} {
		createAgentRequest := &agentservicepb.CreateAgentRequest{
			Parent: userRn.String(),
			Agent:  &agentpb.Agent{Spec: newTestSpec(), Executor: executorRn.String()},
		}
		_, err := service.CreateAgent(context.Background(), createAgentRequest)
		requireCode(t, err, codes.InvalidArgument)
	}
	agent := createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) { agent.Executor = userRn.ExecutorRn("executor").String() })
	require.Equal(t, userRn.ExecutorRn("executor").String(), agent.Executor)
}

func TestUpdateAgent_LabelsChanged(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	agent := createTestAgentRPC(t, service, userRn, nil)

	updateAgentRequest := &agentservicepb.UpdateAgentRequest{
		Agent:      &agentpb.Agent{Name: agent.Name, Title: "Renamed", Annotations: map[string]string{"a": "b"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title", "annotations"}},
	}
	updatedAgent, err := service.UpdateAgent(context.Background(), updateAgentRequest)
	require.NoError(t, err)
	require.Equal(t, "Renamed", updatedAgent.Title)
	require.NotEqual(t, agent.Etag, updatedAgent.Etag)
	require.Equal(t, []string{"state_changed:STATE_PENDING"}, requireEventLog(t, service, agent.Name))

	labels := map[string]string{"team": "core"}
	updateAgentRequest = &agentservicepb.UpdateAgentRequest{
		Agent:      &agentpb.Agent{Name: agent.Name, Labels: labels, Etag: updatedAgent.Etag},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}
	updatedAgent, err = service.UpdateAgent(context.Background(), updateAgentRequest)
	require.NoError(t, err)
	require.Equal(t, labels, updatedAgent.Labels)
	require.Equal(t, []string{"state_changed:STATE_PENDING", "labels_changed"}, requireEventLog(t, service, agent.Name))
	require.Equal(t, labels, listTestEvents(t, service, agent.Name)[1].GetLabelsChanged().Labels)
	require.True(t, proto.Equal(updatedAgent, getTestAgent(t, service, agent.Name)))

	// Setting the same labels changes nothing.
	updateAgentRequest.Agent.Etag = ""
	_, err = service.UpdateAgent(context.Background(), updateAgentRequest)
	require.NoError(t, err)
	require.Len(t, requireEventLog(t, service, agent.Name), 2)

	// A stale etag is refused.
	updateAgentRequest.Agent.Etag = agent.Etag
	_, err = service.UpdateAgent(context.Background(), updateAgentRequest)
	requireCode(t, err, codes.Aborted)

	// Immutable and output only fields are not updatable.
	updateAgentRequest = &agentservicepb.UpdateAgentRequest{
		Agent:      &agentpb.Agent{Name: agent.Name, Spec: newTestSpec()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}},
	}
	_, err = service.UpdateAgent(context.Background(), updateAgentRequest)
	requireCode(t, err, codes.InvalidArgument)
}

func TestUpdateAgent_LabelKeyPaths(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	agent := createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) {
		agent.Labels = map[string]string{"keep": "1", "change": "old", "remove": "1"}
	})

	updateAgentRequest := &agentservicepb.UpdateAgentRequest{
		Agent: &agentpb.Agent{
			Name:   agent.Name,
			Title:  "Renamed",
			Labels: map[string]string{"change": "new", "add": "1", "keep": "ignored"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title", "labels.`change`", "labels.`add`", "labels.`remove`"}},
	}
	updatedAgent, err := service.UpdateAgent(context.Background(), updateAgentRequest)
	require.NoError(t, err)
	labels := map[string]string{"keep": "1", "change": "new", "add": "1"}
	require.Equal(t, labels, updatedAgent.Labels)
	require.Equal(t, "Renamed", updatedAgent.Title)
	require.Equal(t, labels, getTestAgent(t, service, agent.Name).Labels)
	require.Equal(t, []string{"state_changed:STATE_PENDING", "labels_changed"}, requireEventLog(t, service, agent.Name))
	require.Equal(t, labels, listTestEvents(t, service, agent.Name)[1].GetLabelsChanged().Labels)

	// Removing an absent key changes nothing.
	updateAgentRequest = &agentservicepb.UpdateAgentRequest{
		Agent:      &agentpb.Agent{Name: agent.Name},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels.`remove`"}},
	}
	_, err = service.UpdateAgent(context.Background(), updateAgentRequest)
	require.NoError(t, err)
	require.Len(t, requireEventLog(t, service, agent.Name), 2)
}

func TestUpdateAgent_RejectsAnotherUsersExecutorPin(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	agent := createTestAgentRPC(t, service, userRn, nil)
	for _, executorRn := range []*agentpb.ExecutorRn{
		userRn.OrganizationRn().UserRn("other").ExecutorRn("executor"),
		newTestUser(t).ExecutorRn("executor"),
	} {
		updateAgentRequest := &agentservicepb.UpdateAgentRequest{
			Agent:      &agentpb.Agent{Name: agent.Name, Executor: executorRn.String()},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"executor"}},
		}
		_, err := service.UpdateAgent(context.Background(), updateAgentRequest)
		requireCode(t, err, codes.InvalidArgument)
	}
	require.Empty(t, getTestAgent(t, service, agent.Name).Executor)
}

func TestUpdateAgent_PinPlacesPendingAgent(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	labelled := &agentpb.Executor{MaxAgents: 2, Labels: map[string]string{"pool": "pinned"}}
	testExecutor := connectTestExecutor(t, service, userRn, "executor", labelled)
	// The selector matches no executor: only the pin places the agent.
	agent := createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) {
		agent.Spec.ExecutorSelector = map[string]string{"pool": "none"}
	})
	require.Equal(t, agentpb.Agent_STATE_PENDING, agent.State)
	testExecutor.requireNoResponse(testHeartbeatInterval)

	updateAgentRequest := &agentservicepb.UpdateAgentRequest{
		Agent:      &agentpb.Agent{Name: agent.Name, Executor: testExecutor.name},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"executor"}},
	}
	updatedAgent, err := service.UpdateAgent(context.Background(), updateAgentRequest)
	require.NoError(t, err)
	require.Equal(t, testExecutor.name, updatedAgent.LeasedExecutor)
	require.Equal(t, agent.Name, testExecutor.recvLease().Name)
}

// createTestTree creates root → child → grandchild, plus a second child of root.
func createTestTree(t *testing.T, service *Service, userRn *agentpb.UserRn) (root, child, grandchild, sibling *agentpb.Agent) {
	t.Helper()
	root = createTestAgentRPC(t, service, userRn, nil)
	child = createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) { agent.ParentAgent = root.Name })
	grandchild = createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) { agent.ParentAgent = child.Name })
	sibling = createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) { agent.ParentAgent = root.Name })
	return root, child, grandchild, sibling
}

func TestDeleteAgent_WithoutForceRefusesSubAgents(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	root, child, grandchild, sibling := createTestTree(t, service, userRn)

	for _, agent := range []*agentpb.Agent{root, child} {
		deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name}
		_, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
		requireCode(t, err, codes.FailedPrecondition)
		require.Nil(t, getTestAgent(t, service, agent.Name).DeleteTime)
	}

	// Leaves go first, then their parents.
	for _, agent := range []*agentpb.Agent{grandchild, child, sibling, root} {
		deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name}
		deletedAgent, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
		require.NoError(t, err)
		require.NotNil(t, deletedAgent.DeleteTime)
		require.True(t, proto.Equal(deletedAgent, getTestAgent(t, service, agent.Name)))
		requireEventLog(t, service, agent.Name)
	}
}

func TestDeleteAgent_Force(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	root, child, grandchild, sibling := createTestTree(t, service, userRn)

	// A stale etag is refused before any descendant goes.
	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: root.Name, Force: true, Etag: `"stale"`}
	_, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
	requireCode(t, err, codes.Aborted)
	require.Nil(t, getTestAgent(t, service, grandchild.Name).DeleteTime)

	deleteAgentRequest.Etag = getTestAgent(t, service, root.Name).Etag
	deletedRoot, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)
	require.NotNil(t, deletedRoot.DeleteTime)
	for _, agent := range []*agentpb.Agent{root, child, grandchild, sibling} {
		stored := getTestAgent(t, service, agent.Name)
		require.NotNil(t, stored.DeleteTime, "agent %s was not deleted", agent.Name)
		require.Equal(t, agent.Chat, stored.Chat)
		requireEventLog(t, service, agent.Name)
	}

	// Deleting again is NotFound, unless allow_missing.
	deleteAgentRequest = &agentservicepb.DeleteAgentRequest{Name: root.Name}
	_, err = service.DeleteAgent(context.Background(), deleteAgentRequest)
	requireCode(t, err, codes.NotFound)
	deleteAgentRequest.AllowMissing = true
	tombstone, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)
	require.True(t, proto.Equal(deletedRoot, tombstone))

	// A deleted agent takes no update, input or cancel.
	updateAgentRequest := &agentservicepb.UpdateAgentRequest{
		Agent:      &agentpb.Agent{Name: root.Name, Title: "Renamed"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
	}
	_, err = service.UpdateAgent(context.Background(), updateAgentRequest)
	requireCode(t, err, codes.NotFound)
	createInputRequest := &agentservicepb.CreateInputRequest{Parent: root.Name, Input: &agentpb.Input{Message: ai.NewUserMessage(ai.NewTextBlock("hi"))}}
	_, err = service.CreateInput(context.Background(), createInputRequest)
	requireCode(t, err, codes.NotFound)
	cancelAgentRequest := &agentservicepb.CancelAgentRequest{Name: root.Name}
	_, err = service.CancelAgent(context.Background(), cancelAgentRequest)
	requireCode(t, err, codes.NotFound)
}

func TestUndeleteAgent(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	root, child, _, _ := createTestTree(t, service, userRn)
	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: root.Name, Force: true}
	_, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)

	// A sub-agent comes back only under a live parent.
	undeleteAgentRequest := &agentservicepb.UndeleteAgentRequest{Name: child.Name}
	_, err = service.UndeleteAgent(context.Background(), undeleteAgentRequest)
	requireCode(t, err, codes.FailedPrecondition)

	undeleteAgentRequest = &agentservicepb.UndeleteAgentRequest{Name: root.Name}
	restored, err := service.UndeleteAgent(context.Background(), undeleteAgentRequest)
	require.NoError(t, err)
	require.Nil(t, restored.DeleteTime)
	require.Equal(t, agentpb.Agent_STATE_PENDING, restored.State)
	undeleteAgentRequest = &agentservicepb.UndeleteAgentRequest{Name: child.Name}
	_, err = service.UndeleteAgent(context.Background(), undeleteAgentRequest)
	require.NoError(t, err)
}

func TestDeleteAgent_LeasedAndRunning(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	parent := createTestAgentRPC(t, service, userRn, nil)
	require.Equal(t, parent.Name, testExecutor.recvLease().Name)
	agent := createTestAgentRPC(t, service, userRn, func(agent *agentpb.Agent) { agent.ParentAgent = parent.Name })
	require.Equal(t, agent.Name, testExecutor.recvLease().Name)
	requireAgentCount(t, service, testExecutor.executorRn, 2)
	testExecutor.report(agent.Name, newTurnStartedEvent())
	waitForAgentState(t, service, agent.Name, agentpb.Agent_STATE_RUNNING)

	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name}
	deletedAgent, err := service.DeleteAgent(context.Background(), deleteAgentRequest)
	require.NoError(t, err)
	// The cancelled turn is reported to the parent, which wakes on it, before the revoke.
	sender, ok := agentpb.Annotations.Sender.Get(testExecutor.recvInput().Message)
	require.True(t, ok)
	require.Equal(t, agent.Name, sender)
	require.Equal(t, agent.Name, testExecutor.recv().GetRevoke().GetAgent())
	require.Empty(t, deletedAgent.LeasedExecutor)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_CANCELLED, deletedAgent.LastTurnOutcome)
	require.Equal(t, []string{
		"state_changed:STATE_PENDING", "lease_changed:" + testExecutor.name, "state_changed:STATE_IDLE",
		"turn_started", "state_changed:STATE_RUNNING", "turn_ended:TURN_OUTCOME_CANCELLED", "state_changed:STATE_IDLE",
		"lease_changed:", "state_changed:STATE_PENDING",
	}, requireEventLog(t, service, agent.Name))
	requireAgentCount(t, service, testExecutor.executorRn, 1)
}

func TestCancelAgent_SendsCancelToTheLeasedExecutor(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgentRPC(t, service, userRn, nil)
	testExecutor.recvLease()

	cancelAgentRequest := &agentservicepb.CancelAgentRequest{Name: agent.Name}
	cancelledAgent, err := service.CancelAgent(context.Background(), cancelAgentRequest)
	require.NoError(t, err)
	require.Equal(t, agent.Name, testExecutor.recv().GetCancel().GetAgent())
	require.Equal(t, getTestAgent(t, service, agent.Name).LastEventSequence, cancelledAgent.LastEventSequence)
	require.Len(t, requireEventLog(t, service, agent.Name), 3)
}

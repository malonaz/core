package agent_executor

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

const testDefinition = "organizations/o/definitions/reviewer"

func newCallerAgent() *agentpb.Agent {
	return &agentpb.Agent{
		Name:      testAgent,
		RootAgent: testRoot,
		Chat:      testChat,
		Labels:    map[string]string{"phase": "plan", "stale": "true"},
		Spec: &agentpb.AgentSpec{
			Model:               testModel,
			ReasoningEffort:     aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM,
			Tools:               []string{"shell", "read"},
			SubAgentDefinitions: []string{testDefinition},
			ExecutorSelector:    map[string]string{"pool": "gpu"},
		},
	}
}

func agentTool(t *testing.T, agentServiceClient pb.AgentServiceClient, name string) Tool {
	t.Helper()
	for _, tool := range newAgentTools(agentServiceClient) {
		if tool.Definition().GetName() == name {
			return tool
		}
	}
	require.FailNow(t, "no agent tool "+name)
	return nil
}

func requireSender(t *testing.T, createInputRequest *pb.CreateInputRequest, sender string) {
	t.Helper()
	value, ok := agentpb.Annotations.Sender.Get(createInputRequest.GetInput().GetMessage())
	require.True(t, ok)
	require.Equal(t, sender, value)
}

func TestCreateAgents(t *testing.T) {
	agentServiceClient := newFakeAgentServiceClient()
	tool := agentTool(t, agentServiceClient, createAgentsToolName)
	toolCall := newToolCall(t, "c1", createAgentsToolName, map[string]any{
		"agents": []any{
			map[string]any{"title": "Review", "task": "review it", "definition": testDefinition},
			map[string]any{"title": "Read", "task": "read it", "instructions": "you read", "tools": []any{"read"}, "reasoning_effort": "high"},
			map[string]any{"title": "Inherit", "task": "do it", "instructions": "you do"},
		},
	})
	toolResult, err := tool.Execute(context.Background(), newCallerAgent(), toolCall)
	require.NoError(t, err)

	require.Len(t, agentServiceClient.createAgentRequests, 3)
	titleToAgent := map[string]*agentpb.Agent{}
	for _, createAgentRequest := range agentServiceClient.createAgentRequests {
		require.Equal(t, testUser, createAgentRequest.GetParent())
		require.NotEmpty(t, createAgentRequest.GetRequestId())
		require.Equal(t, testAgent, createAgentRequest.GetAgent().GetParentAgent())
		titleToAgent[createAgentRequest.GetAgent().GetTitle()] = createAgentRequest.GetAgent()
	}
	require.Equal(t, testDefinition, titleToAgent["Review"].GetDefinition())
	require.Nil(t, titleToAgent["Review"].GetSpec())
	readSpec := titleToAgent["Read"].GetSpec()
	require.Equal(t, "you read", readSpec.GetInstructions())
	require.Equal(t, testModel, readSpec.GetModel())
	require.Equal(t, aipb.ReasoningEffort_REASONING_EFFORT_HIGH, readSpec.GetReasoningEffort())
	require.Equal(t, []string{"read"}, readSpec.GetTools())
	require.Equal(t, map[string]string{"pool": "gpu"}, readSpec.GetExecutorSelector())
	inheritSpec := titleToAgent["Inherit"].GetSpec()
	require.Equal(t, aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM, inheritSpec.GetReasoningEffort())
	require.Equal(t, []string{"shell", "read"}, inheritSpec.GetTools())

	// Each sub-agent gets its task, sent as the caller.
	require.Len(t, agentServiceClient.createInputRequests, 3)
	var tasks []string
	for _, createInputRequest := range agentServiceClient.createInputRequests {
		requireSender(t, createInputRequest, testAgent)
		require.Equal(t, aipb.Role_ROLE_USER, createInputRequest.GetInput().GetMessage().GetRole())
		tasks = append(tasks, createInputRequest.GetInput().GetMessage().GetBlocks()[0].GetText())
		require.Contains(t, toolResult.GetContent(), createInputRequest.GetParent())
	}
	slices.Sort(tasks)
	require.Equal(t, []string{"do it", "read it", "review it"}, tasks)
	require.Contains(t, toolResult.GetContent(), "End your turn")
}

func TestCreateAgentsReportsFailuresPerAgent(t *testing.T) {
	agentServiceClient := newFakeAgentServiceClient()
	agentServiceClient.createAgentErrorTitle = "Bad"
	tool := agentTool(t, agentServiceClient, createAgentsToolName)
	toolCall := newToolCall(t, "c1", createAgentsToolName, map[string]any{
		"agents": []any{
			map[string]any{"title": "Bad", "task": "fail", "instructions": "x"},
			map[string]any{"title": "Good", "task": "succeed", "instructions": "x"},
		},
	})
	toolResult, err := tool.Execute(context.Background(), newCallerAgent(), toolCall)
	require.NoError(t, err)
	require.Contains(t, toolResult.GetContent(), `"Bad": launch failed`)
	require.Contains(t, toolResult.GetContent(), `("Good"): launched`)
	require.Len(t, agentServiceClient.createInputRequests, 1)
}

func TestCreateAgentsValidatesBeforeLaunching(t *testing.T) {
	for name, agentArguments := range map[string]map[string]any{
		"foreign definition":       {"title": "x", "task": "x", "definition": "organizations/o/definitions/other"},
		"definition with a spec":   {"title": "x", "task": "x", "definition": testDefinition, "model": testModel},
		"tool not held":            {"title": "x", "task": "x", "instructions": "x", "tools": []any{"deploy"}},
		"no instructions":          {"title": "x", "task": "x"},
		"no task":                  {"title": "x", "instructions": "x"},
		"unknown reasoning effort": {"title": "x", "task": "x", "instructions": "x", "reasoning_effort": "extreme"},
	} {
		t.Run(name, func(t *testing.T) {
			agentServiceClient := newFakeAgentServiceClient()
			tool := agentTool(t, agentServiceClient, createAgentsToolName)
			toolCall := newToolCall(t, "c1", createAgentsToolName, map[string]any{
				"agents": []any{
					map[string]any{"title": "Valid", "task": "x", "instructions": "x"},
					agentArguments,
				},
			})
			_, err := tool.Execute(context.Background(), newCallerAgent(), toolCall)
			require.Error(t, err)
			require.Empty(t, agentServiceClient.createAgentRequests)
		})
	}
}

func TestSendInput(t *testing.T) {
	subAgent := &agentpb.Agent{Name: "organizations/o/users/u/agents/sub", RootAgent: testRoot}
	strangerAgent := &agentpb.Agent{Name: "organizations/o/users/u/agents/stranger", RootAgent: "organizations/o/users/u/agents/stranger"}
	agentServiceClient := newFakeAgentServiceClient(subAgent, strangerAgent)
	tool := agentTool(t, agentServiceClient, sendInputToolName)

	toolCall := newToolCall(t, "c1", sendInputToolName, map[string]any{"agent": subAgent.GetName(), "message": "try again"})
	toolResult, err := tool.Execute(context.Background(), newCallerAgent(), toolCall)
	require.NoError(t, err)
	require.Contains(t, toolResult.GetContent(), "End your turn")
	require.Len(t, agentServiceClient.createInputRequests, 1)
	createInputRequest := agentServiceClient.createInputRequests[0]
	require.Equal(t, subAgent.GetName(), createInputRequest.GetParent())
	require.NotEmpty(t, createInputRequest.GetRequestId())
	require.Equal(t, "try again", createInputRequest.GetInput().GetMessage().GetBlocks()[0].GetText())
	requireSender(t, createInputRequest, testAgent)

	for _, recipient := range []string{strangerAgent.GetName(), testAgent, "organizations/o/users/u/agents/missing", "not-a-name"} {
		toolCall := newToolCall(t, "c2", sendInputToolName, map[string]any{"agent": recipient, "message": "hi"})
		_, err := tool.Execute(context.Background(), newCallerAgent(), toolCall)
		require.Error(t, err, recipient)
	}
	require.Len(t, agentServiceClient.createInputRequests, 1)
}

func TestListAgents(t *testing.T) {
	agentServiceClient := newFakeAgentServiceClient()
	agentServiceClient.listAgentsResponse = &pb.ListAgentsResponse{
		Agents: []*agentpb.Agent{{
			Name:            "organizations/o/users/u/agents/sub",
			Title:           "Read",
			Chat:            "organizations/o/users/u/chats/sub",
			State:           agentpb.Agent_STATE_IDLE,
			LastTurnOutcome: agentpb.Agent_TURN_OUTCOME_SUCCEEDED,
			Labels:          map[string]string{"phase": "done"},
		}},
	}
	tool := agentTool(t, agentServiceClient, listAgentsToolName)
	toolResult, err := tool.Execute(context.Background(), newCallerAgent(), newToolCall(t, "c1", listAgentsToolName, nil))
	require.NoError(t, err)

	require.Len(t, agentServiceClient.listAgentsRequests, 1)
	listAgentsRequest := agentServiceClient.listAgentsRequests[0]
	require.Equal(t, testUser, listAgentsRequest.GetParent())
	require.Equal(t, `parent_agent = "organizations/o/users/u/agents/a"`, listAgentsRequest.GetFilter())
	require.Equal(t, "create_time asc", listAgentsRequest.GetOrderBy())

	subAgents := toolResult.GetStructuredContent().GetStructValue().GetFields()["agents"].GetListValue().GetValues()
	require.Len(t, subAgents, 1)
	fields := subAgents[0].GetStructValue().GetFields()
	require.Equal(t, "Read", fields["title"].GetStringValue())
	require.Equal(t, "STATE_IDLE", fields["state"].GetStringValue())
	require.Equal(t, "TURN_OUTCOME_SUCCEEDED", fields["last_turn_outcome"].GetStringValue())
	require.Equal(t, "done", fields["labels"].GetStructValue().GetFields()["phase"].GetStringValue())
	require.NotContains(t, fields, "chat")
}

func TestUpdateLabels(t *testing.T) {
	agentServiceClient := newFakeAgentServiceClient(newCallerAgent())
	tool := agentTool(t, agentServiceClient, updateLabelsToolName)
	toolCall := newToolCall(t, "c1", updateLabelsToolName, map[string]any{
		"set":    map[string]any{"phase": "build", "owner": "me"},
		"remove": []any{"stale"},
	})
	toolResult, err := tool.Execute(context.Background(), newCallerAgent(), toolCall)
	require.NoError(t, err)

	require.Len(t, agentServiceClient.updateAgentRequests, 1)
	updateAgentRequest := agentServiceClient.updateAgentRequests[0]
	require.Equal(t, testAgent, updateAgentRequest.GetAgent().GetName())
	require.Equal(t, []string{"labels"}, updateAgentRequest.GetUpdateMask().GetPaths())
	wantLabels := map[string]string{"phase": "build", "owner": "me"}
	require.Equal(t, wantLabels, updateAgentRequest.GetAgent().GetLabels())

	fields := toolResult.GetStructuredContent().GetStructValue().GetFields()
	require.Len(t, fields, 1)
	labels := fields["labels"].GetStructValue().AsMap()
	require.Equal(t, map[string]any{"phase": "build", "owner": "me"}, labels)

	_, err = tool.Execute(context.Background(), newCallerAgent(), newToolCall(t, "c2", updateLabelsToolName, nil))
	require.Error(t, err)
}

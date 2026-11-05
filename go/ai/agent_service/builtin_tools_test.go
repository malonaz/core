package agent_service

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

// callBuiltinTool runs a built-in tool call of an agent as its turn would.
func callBuiltinTool(t *testing.T, service *testReplica, agent *agentpb.Agent, toolCall *aipb.ToolCall) *aipb.ToolResult {
	t.Helper()
	toolbox, err := service.newToolbox(context.Background(), agent)
	require.NoError(t, err)
	return toolbox.call(context.Background(), toolCall)
}

// deliveredTexts returns the texts of the chat message delivering a sent message.
func deliveredTexts(t *testing.T, service *testReplica, chat, message string) []string {
	t.Helper()
	for _, chatMessage := range service.aiService.messages(chat) {
		deliveredMessages, _ := agentpb.Annotations.Messages.Get(chatMessage)
		if !slices.Contains(strings.Split(deliveredMessages, ","), message) {
			continue
		}
		var texts []string
		for _, block := range chatMessage.GetBlocks() {
			texts = append(texts, block.GetText())
		}
		return texts
	}
	require.FailNow(t, "message never delivered", message)
	return nil
}

// listChildAgents lists the sub-agents of an agent.
func listChildAgents(t *testing.T, service *testReplica, agent *agentpb.Agent) []*agentpb.Agent {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	listAgentsRequest := &agentservicepb.ListAgentsRequest{Parent: agentRn.UserRn().String(), Filter: `parent_agent = "` + agent.Name + `"`}
	listAgentsResponse, err := service.ListAgents(context.Background(), listAgentsRequest)
	require.NoError(t, err)
	return listAgentsResponse.Agents
}

// createTestSubAgent creates an ad-hoc sub-agent of a parent directly.
func createTestSubAgent(t *testing.T, service *testReplica, parent *agentpb.Agent) *agentpb.Agent {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(parent.Name)
	require.NoError(t, err)
	createAgentRequest := &agentservicepb.CreateAgentRequest{
		Parent: agentRn.UserRn().String(),
		Agent: &agentpb.Agent{
			Title:       "sub-agent",
			ParentAgent: parent.Name,
			Spec:        &agentpb.AgentSpec{Model: testModel},
		},
	}
	subAgent, err := service.CreateAgent(context.Background(), createAgentRequest)
	require.NoError(t, err)
	return subAgent
}

func TestBuiltinTools_CreateAgentsLaunchesAndReports(t *testing.T) {
	service := newTestService(t)
	service.aiService.setDefaultGeneration(textGeneration("sub-agent report"))
	agent := createTestAgent(t, service)
	createAgentsCall := newToolCall("call-1", createAgentsToolName, map[string]any{
		"tool_call_title": "Launch a researcher",
		"agents": []any{map[string]any{
			"title":        "researcher",
			"task":         "research things",
			"instructions": "You research.",
		}},
	})
	service.aiService.script(agent.Chat, toolCallGeneration(createAgentsCall), textGeneration("waiting for the researcher"))

	sendText(t, service, agent.Name, "launch")

	// The parent's first turn ends on its own; the report wakes it for a second, played by the default generation.
	require.Eventually(t, func() bool {
		subAgents := listChildAgents(t, service, agent)
		if len(subAgents) != 1 || subAgents[0].LastTurnOutcome != agentpb.Agent_TURN_OUTCOME_SUCCEEDED {
			return false
		}
		messages := sentMessages(t, service, agent.Name)
		return len(messages) == 2 && !isPending(messages[1])
	}, waitTimeout, waitTick)
	subAgent := listChildAgents(t, service, agent)[0]
	require.Equal(t, "researcher", subAgent.Title)
	require.Equal(t, agent.RootAgent, subAgent.RootAgent)
	require.Equal(t, "You research.", subAgent.Spec.Instructions)
	require.Equal(t, testModel, subAgent.Spec.Model)

	// The sub-agent got its task from its parent.
	taskMessage := sentMessages(t, service, subAgent.Name)[0]
	sender, _ := agentpb.Annotations.Sender.Get(taskMessage)
	require.Equal(t, agent.Name, sender)
	require.Len(t, taskMessage.Blocks, 1)
	require.Equal(t, "research things", taskMessage.Blocks[0].GetText())

	// The parent got the sub-agent's report.
	reportMessage := sentMessages(t, service, agent.Name)[1]
	sender, _ = agentpb.Annotations.Sender.Get(reportMessage)
	require.Equal(t, subAgent.Name, sender)
	require.Len(t, reportMessage.Blocks, 1)
	require.Equal(t, "sub-agent report", reportMessage.Blocks[0].GetText())
	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)

	// The delivered messages name the sender agents: the parent to its sub-agent, the sub-agent to its parent.
	require.Equal(t, []string{fmt.Sprintf("[Message from agent %s (%q)]", agent.Name, agent.Title), "research things"}, deliveredTexts(t, service, subAgent.Chat, taskMessage.Name))
	require.Equal(t, []string{fmt.Sprintf("[Report from sub-agent %s (%q)]", subAgent.Name, subAgent.Title), "sub-agent report"}, deliveredTexts(t, service, agent.Chat, reportMessage.Name))
}

func TestBuiltinTools_CreateAgentsIsIdempotentByToolCallID(t *testing.T) {
	service := newTestService(t)
	service.aiService.setDefaultGeneration(textGeneration("done"))
	agent := createTestAgent(t, service)
	createAgentsCall := newToolCall("call-1", createAgentsToolName, map[string]any{
		"tool_call_title": "Launch two",
		"agents": []any{
			map[string]any{"title": "one", "task": "task one", "instructions": "One."},
			map[string]any{"title": "two", "task": "task two", "instructions": "Two."},
		},
	})

	for range 2 {
		toolResult := callBuiltinTool(t, service, agent, createAgentsCall)
		require.Nil(t, toolResult.GetError())
		require.Equal(t, 2, strings.Count(toolResult.GetContent(), ": launched"), toolResult.GetContent())
	}

	subAgents := listChildAgents(t, service, agent)
	require.Len(t, subAgents, 2)
	for _, subAgent := range subAgents {
		require.Len(t, sentMessages(t, service, subAgent.Name), 1)
	}
}

func TestBuiltinTools_CreateAgentsGatesDefinitions(t *testing.T) {
	service := newTestService(t)
	service.aiService.setDefaultGeneration(textGeneration("done"))
	agent := createTestAgent(t, service)
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	createDefinitionRequest := &agentservicepb.CreateDefinitionRequest{
		Parent: agentRn.OrganizationRn().String(),
		Definition: &agentpb.Definition{
			Title: "reviewer",
			Spec:  &agentpb.AgentSpec{Instructions: "You review.", Model: testModel},
		},
	}
	definition, err := service.CreateDefinition(context.Background(), createDefinitionRequest)
	require.NoError(t, err)
	createAgentsCall := newToolCall("call-1", createAgentsToolName, map[string]any{
		"tool_call_title": "Launch a reviewer",
		"agents":          []any{map[string]any{"title": "reviewer", "task": "review", "definition": definition.Name}},
	})

	toolResult := callBuiltinTool(t, service, agent, createAgentsCall)
	require.Equal(t, int32(codes.InvalidArgument), toolResult.GetError().GetCode())
	require.Contains(t, toolResult.GetError().GetMessage(), "may not launch definition")
	require.Empty(t, listChildAgents(t, service, agent))

	agent = createTestAgentWithSpec(t, service, agentRn.UserRn().String(), &agentpb.AgentSpec{
		Model:               testModel,
		SubAgentDefinitions: []string{definition.Name},
	})
	toolResult = callBuiltinTool(t, service, agent, createAgentsCall)
	require.Nil(t, toolResult.GetError())
	subAgents := listChildAgents(t, service, agent)
	require.Len(t, subAgents, 1)
	require.Equal(t, definition.Name, subAgents[0].Definition)
	require.Equal(t, "You review.", subAgents[0].Spec.Instructions)
}

func TestBuiltinTools_CreateAgentsRejectsToolsTheCallerLacks(t *testing.T) {
	service := newTestReplica(t, newFakeAiService(), nil, (&fakeHealthServer{}).register)
	agent := createTestAgent(t, service, healthCheckMethod)
	createAgentsCall := newToolCall("call-1", createAgentsToolName, map[string]any{
		"tool_call_title": "Launch",
		"agents":          []any{map[string]any{"title": "one", "task": "task", "instructions": "One.", "tools": []any{"other.Service"}}},
	})

	toolResult := callBuiltinTool(t, service, agent, createAgentsCall)
	require.Contains(t, toolResult.GetError().GetMessage(), "tools you do not hold")
	require.Empty(t, listChildAgents(t, service, agent))
}

func TestBuiltinTools_MessageAgentStaysInTheTree(t *testing.T) {
	service := newTestService(t)
	service.aiService.setDefaultGeneration(textGeneration("done"))
	agent := createTestAgent(t, service)
	subAgent := createTestSubAgent(t, service, agent)
	strangerAgent := createTestAgent(t, service)
	messageAgentCall := func(recipient string) *aipb.ToolCall {
		return newToolCall("call-"+recipient, messageAgentToolName, map[string]any{"tool_call_title": "Follow up", "agent": recipient, "message": "carry on"})
	}

	toolResult := callBuiltinTool(t, service, agent, messageAgentCall(strangerAgent.Name))
	require.Equal(t, int32(codes.PermissionDenied), toolResult.GetError().GetCode())
	toolResult = callBuiltinTool(t, service, agent, messageAgentCall(agent.Name))
	require.Equal(t, int32(codes.InvalidArgument), toolResult.GetError().GetCode())

	for range 2 {
		toolResult = callBuiltinTool(t, service, agent, messageAgentCall(subAgent.Name))
		require.Nil(t, toolResult.GetError())
	}
	messages := sentMessages(t, service, subAgent.Name)
	require.Len(t, messages, 1)
	sender, _ := agentpb.Annotations.Sender.Get(messages[0])
	require.Equal(t, agent.Name, sender)
	require.Len(t, messages[0].Blocks, 1)
	require.Equal(t, "carry on", messages[0].Blocks[0].GetText())
}

func TestBuiltinTools_ListAgents(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	subAgent := createTestSubAgent(t, service, agent)
	createTestSubAgent(t, service, subAgent)

	toolResult := callBuiltinTool(t, service, agent, newToolCall("call-1", listAgentsToolName, map[string]any{"tool_call_title": "List"}))

	require.Nil(t, toolResult.GetError())
	agentValues := toolResult.GetStructuredContent().GetStructValue().GetFields()["agents"].GetListValue().GetValues()
	require.Len(t, agentValues, 1)
	agentFields := agentValues[0].GetStructValue().GetFields()
	require.Equal(t, subAgent.Name, agentFields["name"].GetStringValue())
	require.Equal(t, "sub-agent", agentFields["title"].GetStringValue())
	require.NotContains(t, agentFields, "spec")
}

func subAgentLabelsMask() *fieldmaskpb.FieldMask {
	return &fieldmaskpb.FieldMask{Paths: []string{"labels"}}
}

func TestBuiltinTools_UpdateLabels(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	updateAgentRequest := &agentservicepb.UpdateAgentRequest{
		Agent:      &agentpb.Agent{Name: agent.Name, Labels: map[string]string{"keep": "yes", "drop": "yes"}},
		UpdateMask: subAgentLabelsMask(),
	}
	_, err := service.UpdateAgent(context.Background(), updateAgentRequest)
	require.NoError(t, err)

	toolResult := callBuiltinTool(t, service, agent, newToolCall("call-1", updateLabelsToolName, map[string]any{
		"tool_call_title": "Publish progress",
		"set":             map[string]any{"progress": "half"},
		"remove":          []any{"drop"},
	}))

	require.Nil(t, toolResult.GetError())
	require.Equal(t, map[string]string{"keep": "yes", "progress": "half"}, getAgent(t, service, agent.Name).Labels)
	labelFields := toolResult.GetStructuredContent().GetStructValue().GetFields()["labels"].GetStructValue().GetFields()
	require.Equal(t, "half", labelFields["progress"].GetStringValue())

	toolResult = callBuiltinTool(t, service, agent, newToolCall("call-2", updateLabelsToolName, map[string]any{"tool_call_title": "Nothing"}))
	require.Equal(t, int32(codes.InvalidArgument), toolResult.GetError().GetCode())
}

func TestBuiltinTools_InvalidArgumentsAreErrorResults(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)

	toolResult := callBuiltinTool(t, service, agent, newToolCall("call-1", messageAgentToolName, map[string]any{"tool_call_title": "Send"}))

	require.Equal(t, int32(codes.InvalidArgument), toolResult.GetError().GetCode())
	require.Equal(t, messageAgentToolName, toolResult.GetToolName())
	require.Equal(t, "call-1", toolResult.GetToolCallId())
}

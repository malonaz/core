package agent_service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	genuipb "github.com/malonaz/core/genproto/ai/genui/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// waitForState waits until an agent is in a state.
func waitForState(t *testing.T, service *testReplica, name string, state agentpb.Agent_State) *agentpb.Agent {
	t.Helper()
	var agent *agentpb.Agent
	require.Eventually(t, func() bool {
		agent = getAgent(t, service, name)
		return agent.State == state
	}, waitTimeout, waitTick, "agent %s never reached %s", name, state)
	return agent
}

// createGatedAgent creates an agent whose health checks await approval, with sub-agents allowed.
func createGatedAgent(t *testing.T, service *testReplica, extraTools ...*agentpb.AgentSpec_Tool) *agentpb.Agent {
	t.Helper()
	tools := append([]*agentpb.AgentSpec_Tool{{Name: healthCheckMethod}}, extraTools...)
	return createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: tools, MaxSubAgentDepth: 1})
}

func approve(service *testReplica, agent, toolCallID string) (*agentpb.Agent, error) {
	approveToolCallRequest := &agentservicepb.ApproveToolCallRequest{Name: agent, ToolCallId: toolCallID}
	return service.ApproveToolCall(context.Background(), approveToolCallRequest)
}

func resolve(service *testReplica, agent, toolCallID string, toolResult *aipb.ToolResult) (*agentpb.Agent, error) {
	resolveToolCallRequest := &agentservicepb.ResolveToolCallRequest{Name: agent, ToolCallId: toolCallID, Result: toolResult}
	return service.ResolveToolCall(context.Background(), resolveToolCallRequest)
}

func healthCheckCall(id, service string) *aipb.ToolCall {
	return newToolCall(id, healthCheckTool, map[string]any{"tool_call_title": "Check " + service, "service": service})
}

func TestAwaited_ApprovalAndDenialResumeTheTurn(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), targetSelf, healthServer.register)
	agent := createGatedAgent(t, service)
	service.aiService.script(agent.Chat,
		toolCallGeneration(
			healthCheckCall("call-1", "approved"),
			healthCheckCall("call-2", "denied"),
			newToolCall("call-3", listAgentsToolName, map[string]any{"tool_call_title": "List"}),
		),
		textGeneration("done"),
	)
	sendText(t, service, agent.Name, "check")

	// The ungated call ran; the gated ones await, without a lease.
	agent = waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)
	awaitedToolCalls := agent.GetLatestTurn().GetAwaitedToolCalls()
	require.Len(t, awaitedToolCalls, 2)
	require.Equal(t, "call-1", awaitedToolCalls[0].GetToolCallId())
	require.Equal(t, "Check approved", awaitedToolCalls[0].GetTitle())
	require.Equal(t, agentpb.AwaitedToolCall_REASON_APPROVAL, awaitedToolCalls[0].GetReason())
	require.Contains(t, toolResults(service.aiService.messages(agent.Chat)), "call-3")
	require.Empty(t, healthServer.checkedServices())
	require.Empty(t, getLease(t, service, agent.Name))

	// Approving twice is a no-op; denying an approved call fails.
	for range 2 {
		agent, err := approve(service, agent.Name, "call-1")
		require.NoError(t, err)
		require.Equal(t, agentpb.Agent_STATE_AWAITING, agent.State)
	}
	_, err := resolve(service, agent.Name, "call-1", ai.NewErrorToolResult("", "", status.Errorf(codes.PermissionDenied, "no").Err()))
	require.True(t, status.HasCode(err, codes.FailedPrecondition))
	// A call awaiting approval is denied by an error result only.
	_, err = resolve(service, agent.Name, "call-2", ai.NewToolResult("", "", "fine"))
	require.True(t, status.HasCode(err, codes.InvalidArgument))

	_, err = resolve(service, agent.Name, "call-2", ai.NewErrorToolResult("", "", status.Errorf(codes.PermissionDenied, "not that one").Err()))
	require.NoError(t, err)

	agent = waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Empty(t, agent.GetLatestTurn().GetAwaitedToolCalls())
	require.Equal(t, []string{"approved"}, healthServer.checkedServices())
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	toolCallIDToToolResult := toolResults(service.aiService.messages(agent.Chat))
	approvedBy, _ := agentpb.Annotations.ApprovedBy.Get(toolCallIDToToolResult["call-1"])
	require.Equal(t, agentRn.UserRn().String(), approvedBy)
	require.Equal(t, "SERVING", toolCallIDToToolResult["call-1"].GetStructuredContent().GetStructValue().GetFields()["status"].GetStringValue())
	deniedBy, _ := agentpb.Annotations.DeniedBy.Get(toolCallIDToToolResult["call-2"])
	require.Equal(t, agentRn.UserRn().String(), deniedBy)
	require.Equal(t, "not that one", toolCallIDToToolResult["call-2"].GetError().GetMessage())
	require.Len(t, service.aiService.generateMessageRequests(agent.Chat), 2)
}

func TestAwaited_ApprovalPolicies(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), targetSelf, healthServer.register)
	// Required on a method without side effects still awaits; not required never does.
	agent := createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_Tool{
		{Name: healthCheckMethod, Approval: agentpb.AgentSpec_Tool_APPROVAL_REQUIRED},
	}})
	toolbox, err := service.newToolbox(context.Background(), agent)
	require.NoError(t, err)
	_, awaits := toolbox.await(healthCheckCall("call-1", "x"))
	require.True(t, awaits)

	agent = createTestAgent(t, service, healthCheckMethod)
	toolbox, err = service.newToolbox(context.Background(), agent)
	require.NoError(t, err)
	_, awaits = toolbox.await(healthCheckCall("call-1", "x"))
	require.False(t, awaits)

	// Unspecified: a method without side effects runs; any other awaits. Health.Check declares none.
	agent = createGatedAgent(t, service)
	toolbox, err = service.newToolbox(context.Background(), agent)
	require.NoError(t, err)
	_, awaits = toolbox.await(healthCheckCall("call-1", "x"))
	require.True(t, awaits)
}

func TestAwaited_ConcurrentSettlementsResumeOnce(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), targetSelf, healthServer.register)
	agent := createGatedAgent(t, service)
	toolCalls := []*aipb.ToolCall{}
	for _, id := range []string{"call-1", "call-2", "call-3", "call-4"} {
		toolCalls = append(toolCalls, healthCheckCall(id, id))
	}
	service.aiService.script(agent.Chat, toolCallGeneration(toolCalls...), textGeneration("done"))
	sendText(t, service, agent.Name, "check")
	waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)

	var waitGroup sync.WaitGroup
	for _, toolCall := range toolCalls {
		waitGroup.Go(func() {
			_, err := approve(service, agent.Name, toolCall.Id)
			require.NoError(t, err)
		})
	}
	waitGroup.Wait()

	waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.ElementsMatch(t, []string{"call-1", "call-2", "call-3", "call-4"}, healthServer.checkedServices())
	require.Len(t, service.aiService.generateMessageRequests(agent.Chat), 2)
}

func TestAwaited_GenuiDisplayAndInput(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_Tool{
		{Name: "malonaz.ai.genui.v1.KeyValueList"},
		{Name: "malonaz.ai.genui.v1.Choice"},
	}})
	service.aiService.script(agent.Chat,
		toolCallGeneration(
			newToolCall("call-1", "Generate_KeyValueList", map[string]any{"tool_call_title": "Show the facts"}),
			newToolCall("call-2", "Generate_Choice", map[string]any{"tool_call_title": "Ask which", "question": "Which?"}),
		),
		textGeneration("thanks"),
	)
	sendText(t, service, agent.Name, "go")

	// The display component is acknowledged; the input awaits the user's answer.
	agent = waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)
	require.Equal(t, "ok", toolResults(service.aiService.messages(agent.Chat))["call-1"].GetContent())
	require.Len(t, agent.GetLatestTurn().GetAwaitedToolCalls(), 1)
	require.Equal(t, agentpb.AwaitedToolCall_REASON_INPUT, agent.GetLatestTurn().GetAwaitedToolCalls()[0].GetReason())

	_, err := approve(service, agent.Name, "call-2")
	require.True(t, status.HasCode(err, codes.FailedPrecondition))
	_, err = resolve(service, agent.Name, "call-2", ai.NewToolResult("", "", "the first"))
	require.True(t, status.HasCode(err, codes.InvalidArgument))
	wrongVariant := ai.NewToolResult("", "", "")
	require.NoError(t, genuipb.Annotations.InputResponse.Set(wrongVariant, &genuipb.InputResponse{
		ComponentId: "call-2",
		Response:    &genuipb.InputResponse_Confirmation{Confirmation: &genuipb.ConfirmationResponse{Confirmed: true}},
	}))
	_, err = resolve(service, agent.Name, "call-2", wrongVariant)
	require.True(t, status.HasCode(err, codes.InvalidArgument))

	answer := &aipb.ToolResult{}
	require.NoError(t, genuipb.Annotations.InputResponse.Set(answer, &genuipb.InputResponse{
		ComponentId: "call-2",
		Response:    &genuipb.InputResponse_Choice{Choice: &genuipb.ChoiceResponse{Option: "the first"}},
	}))
	_, err = resolve(service, agent.Name, "call-2", answer)
	require.NoError(t, err)

	agent = waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	toolResult := toolResults(service.aiService.messages(agent.Chat))["call-2"]
	require.Contains(t, toolResult.GetContent(), "the first")
	require.True(t, genuipb.Annotations.InputResponse.Has(toolResult))
}

func TestAwaited_UserMessageDeniesAndResumes(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), targetSelf, healthServer.register)
	agent := createGatedAgent(t, service)
	sibling := createTestSubAgent(t, service, agent)
	service.aiService.script(agent.Chat, toolCallGeneration(healthCheckCall("call-1", "x")), textGeneration("ok, never mind"))
	sendText(t, service, agent.Name, "check")
	waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)

	// Another agent's message only queues.
	_, err := service.messageAgent(context.Background(), agent.Name, ai.NewUserMessage(ai.NewTextBlock("a report")), sibling.Name, uuid.MustNewV7().String())
	require.NoError(t, err)
	time.Sleep(3 * service.opts.ClaimInterval)
	require.Equal(t, agentpb.Agent_STATE_AWAITING, getAgent(t, service, agent.Name).State)

	sendText(t, service, agent.Name, "stop, do something else")

	agent = waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Empty(t, healthServer.checkedServices())
	messages := service.aiService.messages(agent.Chat)
	toolResult := toolResults(messages)["call-1"]
	require.Contains(t, toolResult.GetError().GetMessage(), "the user replied instead")
	require.True(t, agentpb.Annotations.DeniedBy.Has(toolResult))
	// The denial comes first, then the queued messages, before the next generation.
	require.Equal(t, []aipb.Role{aipb.Role_ROLE_USER, aipb.Role_ROLE_ASSISTANT, aipb.Role_ROLE_TOOL, aipb.Role_ROLE_USER, aipb.Role_ROLE_USER, aipb.Role_ROLE_ASSISTANT}, roles(messages))
	history := service.aiService.generateMessageHistories(agent.Chat)[1]
	require.Equal(t, "stop, do something else", lastText(history[len(history)-1]))
}

func TestAwaited_CancelWhileAwaiting(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), targetSelf, healthServer.register)
	service.aiService.setDefaultGeneration(textGeneration("noted"))
	parentAgent := createTestAgent(t, service)
	parentAgentRn, err := agentpb.ParseAgentRn(parentAgent.Name)
	require.NoError(t, err)
	createAgentRequest := &agentservicepb.CreateAgentRequest{
		Parent: parentAgentRn.UserRn().String(),
		Agent: &agentpb.Agent{
			ParentAgent: parentAgent.Name,
			Spec:        &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_Tool{{Name: healthCheckMethod}}},
		},
	}
	agent, err := service.CreateAgent(context.Background(), createAgentRequest)
	require.NoError(t, err)
	service.aiService.script(agent.Chat, toolCallGeneration(healthCheckCall("call-1", "x")), textGeneration("ok"))
	sendText(t, service, agent.Name, "check")
	waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)

	cancelAgentRequest := &agentservicepb.CancelAgentRequest{Name: agent.Name}
	cancelledAgent, err := service.CancelAgent(context.Background(), cancelAgentRequest)
	require.NoError(t, err)
	require.Equal(t, agentpb.Agent_STATE_IDLE, cancelledAgent.State)
	require.Equal(t, agentpb.Turn_OUTCOME_CANCELLED, cancelledAgent.GetLatestTurn().GetOutcome())
	// The parent hears of it once.
	_, err = service.CancelAgent(context.Background(), cancelAgentRequest)
	require.NoError(t, err)
	waitForState(t, service, parentAgent.Name, agentpb.Agent_STATE_IDLE)
	reports := sentMessages(t, service, parentAgent.Name)
	require.Len(t, reports, 1)
	require.Equal(t, "My turn was cancelled.", lastText(reports[0]))

	// The next turn writes the cancelled result before delivering the next message.
	sendText(t, service, agent.Name, "again")
	agent = waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Empty(t, healthServer.checkedServices())
	require.Contains(t, toolResults(service.aiService.messages(agent.Chat))["call-1"].GetError().GetMessage(), "cancelled")
	require.Empty(t, agent.GetLatestTurn().GetAwaitedToolCalls())
}

func TestAwaited_TakeoverAwaitsAGatedCallLeftUnrecorded(t *testing.T) {
	aiService := newFakeAiService()
	healthServer := &fakeHealthServer{}
	idleReplica := newTestReplica(t, aiService, func(opts *Opts) {
		opts.MaxConcurrentTurns = 0
		targetSelf(opts)
	})
	agent := createTestAgentWithSpec(t, idleReplica, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_Tool{{Name: healthCheckMethod}}})
	// A crash mid-round left the gated call unrecorded, and the ungated one without a result.
	createMessage(t, aiService, agent.Chat, ai.NewUserMessage(ai.NewTextBlock("check")))
	createMessage(t, aiService, agent.Chat, ai.NewAssistantMessage(
		ai.NewToolCallBlock(healthCheckCall("call-1", "gated")),
		ai.NewToolCallBlock(newToolCall("call-2", listAgentsToolName, map[string]any{})),
	))
	crashTurn(t, idleReplica, agent.Name)

	service := newTestReplica(t, aiService, targetSelf, healthServer.register)

	agent = waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)
	require.Equal(t, "call-1", agent.GetLatestTurn().GetAwaitedToolCalls()[0].GetToolCallId())
	require.Contains(t, toolResults(aiService.messages(agent.Chat))["call-2"].GetError().GetMessage(), "interrupted")
}

func TestAwaited_TakeoverInterruptsAnApprovedCallInFlight(t *testing.T) {
	aiService := newFakeAiService()
	healthServer := &fakeHealthServer{}
	idleReplica := newTestReplica(t, aiService, func(opts *Opts) {
		opts.MaxConcurrentTurns = 0
		targetSelf(opts)
	})
	agent := createTestAgentWithSpec(t, idleReplica, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_Tool{{Name: healthCheckMethod}}})
	createMessage(t, aiService, agent.Chat, ai.NewUserMessage(ai.NewTextBlock("check")))
	createMessage(t, aiService, agent.Chat, ai.NewAssistantMessage(
		ai.NewToolCallBlock(healthCheckCall("call-1", "in flight")),
		ai.NewToolCallBlock(healthCheckCall("call-2", "approved")),
	))
	// The replica running the approved calls crashed after starting the first.
	require.NoError(t, idleReplica.transact(context.Background(), func(ctx context.Context, tx *txn) error {
		lockedAgent, err := tx.getAgent(ctx, agent.Name)
		if err != nil {
			return err
		}
		approval := &agentpb.AwaitedToolCall_Resolution_Approval_{Approval: &agentpb.AwaitedToolCall_Resolution_Approval{}}
		lockedAgent.LatestTurn = &agentpb.Turn{StartTime: timestamppb.Now(), AwaitedToolCalls: []*agentpb.AwaitedToolCall{
			{ToolCallId: "call-1", ToolName: healthCheckTool, Reason: agentpb.AwaitedToolCall_REASON_APPROVAL, Resolution: &agentpb.AwaitedToolCall_Resolution{Resolver: "users/approver", Decision: approval, StartTime: timestamppb.Now()}},
			{ToolCallId: "call-2", ToolName: healthCheckTool, Reason: agentpb.AwaitedToolCall_REASON_APPROVAL, Resolution: &agentpb.AwaitedToolCall_Resolution{Resolver: "users/approver", Decision: approval}},
		}}
		return tx.saveAgent(ctx, lockedAgent)
	}))
	crashTurn(t, idleReplica, agent.Name)
	aiService.script(agent.Chat, textGeneration("done"))

	service := newTestReplica(t, aiService, targetSelf, healthServer.register)

	agent = waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Equal(t, []string{"approved"}, healthServer.checkedServices())
	toolCallIDToToolResult := toolResults(aiService.messages(agent.Chat))
	require.Contains(t, toolCallIDToToolResult["call-1"].GetError().GetMessage(), "interrupted")
	approvedBy, _ := agentpb.Annotations.ApprovedBy.Get(toolCallIDToToolResult["call-1"])
	require.Equal(t, "users/approver", approvedBy)
	require.Nil(t, toolCallIDToToolResult["call-2"].GetError())
}

func TestAwaited_SettledOnAnotherReplicaResumesThere(t *testing.T) {
	aiService := newFakeAiService()
	healthServer := &fakeHealthServer{}
	replicaA := newTestReplica(t, aiService, targetSelf, healthServer.register)
	agent := createGatedAgent(t, replicaA)
	aiService.script(agent.Chat, toolCallGeneration(healthCheckCall("call-1", "x")), textGeneration("done"))
	sendText(t, replicaA, agent.Name, "check")
	waitForState(t, replicaA, agent.Name, agentpb.Agent_STATE_AWAITING)

	// Settled on a replica without room, the RUNNING agent is left to the claim scans.
	idleReplica := newIdleReplica(t, aiService)
	settledAgent, err := approve(idleReplica, agent.Name, "call-1")
	require.NoError(t, err)
	require.Equal(t, agentpb.Agent_STATE_RUNNING, settledAgent.State)

	waitForIdle(t, replicaA, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Equal(t, []string{"x"}, healthServer.checkedServices())
}

func TestAwaited_SubAgentsTightenApprovalsOnly(t *testing.T) {
	parentTools := []*agentpb.AgentSpec_Tool{
		{Name: "pkg.Loose", Approval: agentpb.AgentSpec_Tool_APPROVAL_NOT_REQUIRED},
		{Name: "pkg.Policy"},
		{Name: "pkg.Strict", Approval: agentpb.AgentSpec_Tool_APPROVAL_REQUIRED},
	}

	tools, err := subAgentTools(parentTools, nil)
	require.NoError(t, err)
	require.Len(t, tools, 3)

	tools, err = subAgentTools(parentTools, []*agentpb.AgentSpec_Tool{
		{Name: "pkg.Loose", Approval: agentpb.AgentSpec_Tool_APPROVAL_REQUIRED},
		{Name: "pkg.Strict"},
	})
	require.NoError(t, err)
	require.Equal(t, agentpb.AgentSpec_Tool_APPROVAL_REQUIRED, tools[0].GetApproval())
	require.Equal(t, agentpb.AgentSpec_Tool_APPROVAL_REQUIRED, tools[1].GetApproval())

	_, err = subAgentTools(parentTools, []*agentpb.AgentSpec_Tool{{Name: "pkg.Policy", Approval: agentpb.AgentSpec_Tool_APPROVAL_NOT_REQUIRED}})
	require.ErrorContains(t, err, "may not loosen")
	_, err = subAgentTools(parentTools, []*agentpb.AgentSpec_Tool{{Name: "pkg.Strict", Approval: agentpb.AgentSpec_Tool_APPROVAL_NOT_REQUIRED}})
	require.ErrorContains(t, err, "may not loosen")
}

func TestAwaited_SubAgentDepthIsCapped(t *testing.T) {
	service := newTestService(t)
	service.aiService.setDefaultGeneration(textGeneration("done"))
	launch := newToolCall("call-1", createAgentsToolName, map[string]any{
		"tool_call_title": "Launch",
		"agents":          []any{map[string]any{"title": "child", "task": "task", "instructions": "Child."}},
	})

	leaf := createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Model: testModel})
	toolResult := callBuiltinTool(t, service, leaf, launch)
	require.Equal(t, int32(codes.PermissionDenied), toolResult.GetError().GetCode())

	parent := createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Model: testModel, MaxSubAgentDepth: 1})
	toolResult = callBuiltinTool(t, service, parent, launch)
	require.Nil(t, toolResult.GetError())
	child := listChildAgents(t, service, parent)[0]
	require.Zero(t, child.Spec.GetMaxSubAgentDepth())
	toolResult = callBuiltinTool(t, service, child, launch)
	require.Equal(t, int32(codes.PermissionDenied), toolResult.GetError().GetCode())
}

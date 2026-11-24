package agent_service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
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
func createGatedAgent(t *testing.T, service *testReplica, extraTools ...*agentpb.AgentSpec_ToolGrant) *agentpb.Agent {
	t.Helper()
	tools := append([]*agentpb.AgentSpec_ToolGrant{toolGrant(healthCheckGrant)}, extraTools...)
	return createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: tools, MaxSubAgentDepth: 1})
}

func approve(t *testing.T, service *testReplica, agent, toolCallID string) (*agentpb.Agent, error) {
	t.Helper()
	return approveAlways(t, service, agent, toolCallID, false)
}

func approveAlways(t *testing.T, service *testReplica, agent, toolCallID string, always bool) (*agentpb.Agent, error) {
	t.Helper()
	approveToolCallRequest := &agentservicepb.ApproveToolCallRequest{Name: agent, ToolCallId: toolCallID, Always: always}
	return service.ApproveToolCall(ownerContext(t, agent), approveToolCallRequest)
}

func resolve(t *testing.T, service *testReplica, agent, toolCallID string, toolResult *aipb.ToolResult) (*agentpb.Agent, error) {
	t.Helper()
	resolveToolCallRequest := &agentservicepb.ResolveToolCallRequest{Name: agent, ToolCallId: toolCallID, Result: toolResult}
	return service.ResolveToolCall(ownerContext(t, agent), resolveToolCallRequest)
}

func healthCheckCall(id, service string) *aipb.ToolCall {
	return newToolCall(id, healthCheckTool, map[string]any{"tool_call_title": "Check " + service, "service": service})
}

func TestAwaited_ApprovedOrResolvedResumeTheTurn(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), targetSelf, healthServer.register)
	agent := createGatedAgent(t, service)
	service.aiService.script(agent.Chat,
		toolCallGeneration(
			healthCheckCall("call-1", "approved"),
			healthCheckCall("call-2", "refused"),
			healthCheckCall("call-3", "run-by-the-user"),
			newToolCall("call-4", listAgentsToolName, map[string]any{"tool_call_title": "List"}),
		),
		textGeneration("done"),
	)
	sendText(t, service, agent.Name, "check")

	// The ungated call ran; the gated ones await, without a lease, each naming its tool.
	agent = waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)
	awaitedToolCalls := agent.GetLatestTurn().GetAwaitedToolCalls()
	require.Len(t, awaitedToolCalls, 3)
	require.Equal(t, "call-1", awaitedToolCalls[0].GetToolCallId())
	require.Equal(t, "Check approved", awaitedToolCalls[0].GetTitle())
	require.Equal(t, healthCheckGrant, awaitedToolCalls[0].GetTool())
	require.Equal(t, agentpb.AwaitedToolCall_REASON_APPROVAL, awaitedToolCalls[0].GetReason())
	require.Contains(t, toolResults(service.aiService.messages(agent.Chat)), "call-4")
	require.Empty(t, healthServer.checkedServices())
	require.Empty(t, getLease(t, service, agent.Name))

	// Approving twice is a no-op; resolving an approved call fails.
	for range 2 {
		agent, err := approve(t, service, agent.Name, "call-1")
		require.NoError(t, err)
		require.Equal(t, agentpb.Agent_STATE_AWAITING, agent.State)
	}
	_, err := resolve(t, service, agent.Name, "call-1", ai.NewErrorToolResult("", "", status.Errorf(codes.PermissionDenied, "no").Err()))
	require.True(t, status.HasCode(err, codes.FailedPrecondition))

	// A call awaiting approval resolves with an error status refusing it, or with a result the user supplies.
	refusal := ai.NewErrorToolResult("", "", status.Errorf(codes.PermissionDenied, "not that one").Err())
	for range 2 {
		_, err = resolve(t, service, agent.Name, "call-2", refusal)
		require.NoError(t, err)
	}
	_, err = approve(t, service, agent.Name, "call-2")
	require.True(t, status.HasCode(err, codes.FailedPrecondition))
	_, err = resolve(t, service, agent.Name, "call-3", ai.NewToolResult("", "", "SERVING, checked by hand"))
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
	for _, toolCallID := range []string{"call-2", "call-3"} {
		resolvedBy, _ := agentpb.Annotations.ResolvedBy.Get(toolCallIDToToolResult[toolCallID])
		require.Equal(t, agentRn.UserRn().String(), resolvedBy, toolCallID)
	}
	require.Equal(t, "not that one", toolCallIDToToolResult["call-2"].GetError().GetMessage())
	require.Equal(t, "SERVING, checked by hand", toolCallIDToToolResult["call-3"].GetContent())
	require.Len(t, service.aiService.generateMessageRequests(agent.Chat), 2)
}

func TestAwaited_ApproveAlwaysStopsTheAgentAsking(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), targetSelf, healthServer.register)
	// The set is granted whole: always refines it for the one tool.
	agent := createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Model: testModel, MaxSubAgentDepth: 1, Tools: []*agentpb.AgentSpec_ToolGrant{
		{Grant: &agentpb.AgentSpec_ToolGrant_ToolSet{ToolSet: healthToolSet}, Approval: agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED},
	}})
	service.aiService.script(agent.Chat,
		toolCallGeneration(healthCheckCall("call-1", "first"), healthCheckCall("call-2", "second")),
		toolCallGeneration(healthCheckCall("call-3", "third")),
		textGeneration("done"),
	)
	sendText(t, service, agent.Name, "check")
	agent = waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)
	require.Len(t, agent.GetLatestTurn().GetAwaitedToolCalls(), 2)

	// Approving the first always approves the second, and the third call runs unasked.
	agent, err := approveAlways(t, service, agent.Name, "call-1", true)
	require.NoError(t, err)
	for _, awaitedToolCall := range agent.GetLatestTurn().GetAwaitedToolCalls() {
		require.True(t, awaitedToolCall.GetResolution().GetApproval().GetAlways(), awaitedToolCall.GetToolCallId())
	}
	require.Len(t, agent.GetSpec().GetTools(), 2)
	refinement := agent.GetSpec().GetTools()[1]
	require.Equal(t, healthCheckGrant, refinement.GetTool())
	require.Equal(t, agentpb.AgentSpec_ToolGrant_APPROVAL_NOT_REQUIRED, refinement.GetApproval())
	agent = waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Equal(t, []string{"first", "second", "third"}, healthServer.checkedServices())

	// Sub-agents launched afterwards inherit it.
	tools, err := subAgentTools(agent.GetSpec().GetTools(), nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(refinement, tools[1]))

	// The owner undoes it by updating the agent's grants.
	updateAgentRequest := &agentservicepb.UpdateAgentRequest{
		Agent:      &agentpb.Agent{Name: agent.Name, Spec: &agentpb.AgentSpec{Tools: agent.GetSpec().GetTools()[:1]}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.tools"}},
	}
	agent, err = service.UpdateAgent(ownerContext(t, agent.Name), updateAgentRequest)
	require.NoError(t, err)
	require.Len(t, agent.GetSpec().GetTools(), 1)
	require.Equal(t, testModel, agent.GetSpec().GetModel())
	updateAgentRequest.Agent.Spec.Tools = []*agentpb.AgentSpec_ToolGrant{toolGrant("toolSets/no.such.Service")}
	_, err = service.UpdateAgent(ownerContext(t, agent.Name), updateAgentRequest)
	require.True(t, status.HasCode(err, codes.InvalidArgument), "%v", err)
}

func TestAwaited_ApprovalPolicies(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), targetSelf, healthServer.register)
	// Required on a method without side effects still awaits; not required never does.
	agent := createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_ToolGrant{
		{Grant: &agentpb.AgentSpec_ToolGrant_Tool{Tool: healthCheckGrant}, Approval: agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED},
	}})
	toolbox, err := service.newToolbox(context.Background(), agent)
	require.NoError(t, err)
	_, _, awaits := toolbox.await(healthCheckCall("call-1", "x"))
	require.True(t, awaits)

	agent = createTestAgent(t, service, healthCheckGrant)
	toolbox, err = service.newToolbox(context.Background(), agent)
	require.NoError(t, err)
	_, _, awaits = toolbox.await(healthCheckCall("call-1", "x"))
	require.False(t, awaits)

	// Unspecified: a method without side effects runs; any other awaits. Health.Check declares none.
	agent = createGatedAgent(t, service)
	toolbox, err = service.newToolbox(context.Background(), agent)
	require.NoError(t, err)
	_, _, awaits = toolbox.await(healthCheckCall("call-1", "x"))
	require.True(t, awaits)

	// A tool granted inside a granted set refines its approval, without adding the tool twice.
	agent = createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_ToolGrant{
		{Grant: &agentpb.AgentSpec_ToolGrant_ToolSet{ToolSet: healthToolSet}, Approval: agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED},
		{Grant: &agentpb.AgentSpec_ToolGrant_Tool{Tool: healthCheckGrant}, Approval: agentpb.AgentSpec_ToolGrant_APPROVAL_NOT_REQUIRED},
	}})
	toolbox, err = service.newToolbox(context.Background(), agent)
	require.NoError(t, err)
	_, _, awaits = toolbox.await(healthCheckCall("call-1", "x"))
	require.False(t, awaits)
	require.Len(t, toolbox.toolSets, 1)
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
			_, err := approve(t, service, agent.Name, toolCall.Id)
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
	agent := createTestAgentWithSpec(t, service, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_ToolGrant{
		toolGrant("toolSets/genui/tools/KeyValueList"),
		toolGrant("toolSets/genui/tools/Choice"),
	}})
	service.aiService.script(agent.Chat,
		toolCallGeneration(
			newToolCall("call-1", "Generate_KeyValueList", map[string]any{"tool_call_title": "Show the facts"}),
			newToolCall("call-2", "Generate_Choice", map[string]any{"tool_call_title": "Ask which", "question": "Which?"}),
			newToolCall("call-3", "Generate_Choice", map[string]any{"tool_call_title": "Ask again", "question": "And?"}),
		),
		textGeneration("thanks"),
	)
	sendText(t, service, agent.Name, "go")

	// The display component is acknowledged; the input awaits the user's answer.
	agent = waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)
	require.Equal(t, "ok", toolResults(service.aiService.messages(agent.Chat))["call-1"].GetContent())
	require.Len(t, agent.GetLatestTurn().GetAwaitedToolCalls(), 2)
	require.Equal(t, agentpb.AwaitedToolCall_REASON_RESULT, agent.GetLatestTurn().GetAwaitedToolCalls()[0].GetReason())
	require.Equal(t, "toolSets/genui/tools/Choice", agent.GetLatestTurn().GetAwaitedToolCalls()[0].GetTool())

	_, err := approve(t, service, agent.Name, "call-2")
	require.True(t, status.HasCode(err, codes.FailedPrecondition))
	_, err = resolve(t, service, agent.Name, "call-2", ai.NewToolResult("", "", "the first"))
	require.True(t, status.HasCode(err, codes.InvalidArgument))
	wrongVariant := ai.NewToolResult("", "", "")
	require.NoError(t, genuipb.Annotations.InputResponse.Set(wrongVariant, &genuipb.InputResponse{
		ComponentId: "call-2",
		Response:    &genuipb.InputResponse_Confirmation{Confirmation: &genuipb.ConfirmationResponse{Confirmed: true}},
	}))
	_, err = resolve(t, service, agent.Name, "call-2", wrongVariant)
	require.True(t, status.HasCode(err, codes.InvalidArgument))

	answer := &aipb.ToolResult{}
	require.NoError(t, genuipb.Annotations.InputResponse.Set(answer, &genuipb.InputResponse{
		ComponentId: "call-2",
		Response:    &genuipb.InputResponse_Choice{Choice: &genuipb.ChoiceResponse{Option: "the first"}},
	}))
	_, err = resolve(t, service, agent.Name, "call-2", answer)
	require.NoError(t, err)
	// An error status declines to answer: it carries no InputResponse.
	_, err = resolve(t, service, agent.Name, "call-3", ai.NewErrorToolResult("", "", status.Errorf(codes.PermissionDenied, "not now").Err()))
	require.NoError(t, err)

	agent = waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	toolResult := toolResults(service.aiService.messages(agent.Chat))["call-2"]
	require.Contains(t, toolResult.GetContent(), "the first")
	require.True(t, genuipb.Annotations.InputResponse.Has(toolResult))
	require.Equal(t, "not now", toolResults(service.aiService.messages(agent.Chat))["call-3"].GetError().GetMessage())
}

func TestAwaited_UserMessageResolvesAndResumes(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), targetSelf, healthServer.register)
	agent := createGatedAgent(t, service)
	sibling := createTestSubAgent(t, service, agent)
	service.aiService.script(agent.Chat, toolCallGeneration(healthCheckCall("call-1", "x")), textGeneration("ok, never mind"))
	sendText(t, service, agent.Name, "check")
	waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)

	// Another agent's message only queues.
	_, err := service.messageAgent(context.Background(), agent.Name, ai.NewUserMessage(ai.NewTextBlock("a report")), requester{principal: sibling.Name}, uuid.MustNewV7().String())
	require.NoError(t, err)
	time.Sleep(3 * service.opts.ClaimInterval)
	require.Equal(t, agentpb.Agent_STATE_AWAITING, getAgent(t, service, agent.Name).State)

	sendText(t, service, agent.Name, "stop, do something else")

	agent = waitForIdle(t, service, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Empty(t, healthServer.checkedServices())
	messages := service.aiService.messages(agent.Chat)
	toolResult := toolResults(messages)["call-1"]
	require.Contains(t, toolResult.GetError().GetMessage(), "the user replied instead")
	require.Equal(t, int32(codes.Canceled), toolResult.GetError().GetCode())
	require.True(t, agentpb.Annotations.ResolvedBy.Has(toolResult))
	// The resolution comes first, then the queued messages, before the next generation.
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
			Spec:        &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_ToolGrant{toolGrant(healthCheckGrant)}},
		},
	}
	agent, err := service.CreateAgent(context.Background(), createAgentRequest)
	require.NoError(t, err)
	service.aiService.script(agent.Chat, toolCallGeneration(healthCheckCall("call-1", "x")), textGeneration("ok"))
	sendText(t, service, agent.Name, "check")
	waitForState(t, service, agent.Name, agentpb.Agent_STATE_AWAITING)

	cancelAgentRequest := &agentservicepb.CancelAgentRequest{Name: agent.Name}
	cancelledAgent, err := service.CancelAgent(ownerContext(t, cancelAgentRequest.Name), cancelAgentRequest)
	require.NoError(t, err)
	require.Equal(t, agentpb.Agent_STATE_IDLE, cancelledAgent.State)
	require.Equal(t, agentpb.Turn_OUTCOME_CANCELLED, cancelledAgent.GetLatestTurn().GetOutcome())
	// The parent hears of it once.
	_, err = service.CancelAgent(ownerContext(t, cancelAgentRequest.Name), cancelAgentRequest)
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
	agent := createTestAgentWithSpec(t, idleReplica, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_ToolGrant{toolGrant(healthCheckGrant)}})
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
	agent := createTestAgentWithSpec(t, idleReplica, newTestUser(t), &agentpb.AgentSpec{Model: testModel, Tools: []*agentpb.AgentSpec_ToolGrant{toolGrant(healthCheckGrant)}})
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
	settledAgent, err := approve(t, idleReplica, agent.Name, "call-1")
	require.NoError(t, err)
	require.Equal(t, agentpb.Agent_STATE_RUNNING, settledAgent.State)

	waitForIdle(t, replicaA, agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED)
	require.Equal(t, []string{"x"}, healthServer.checkedServices())
}

func TestAwaited_SubAgentsTightenApprovalsOnly(t *testing.T) {
	grant := func(name string, approval agentpb.AgentSpec_ToolGrant_Approval) *agentpb.AgentSpec_ToolGrant {
		grant := toolGrant(name)
		grant.Approval = approval
		return grant
	}
	const (
		loose  = "toolSets/pkg.Loose/tools/Call"
		policy = "toolSets/pkg.Policy/tools/Call"
		strict = "toolSets/pkg.Strict/tools/Call"
		set    = "toolSets/pkg.Set"
	)
	parentTools := []*agentpb.AgentSpec_ToolGrant{
		grant(loose, agentpb.AgentSpec_ToolGrant_APPROVAL_NOT_REQUIRED),
		grant(policy, agentpb.AgentSpec_ToolGrant_APPROVAL_UNSPECIFIED),
		grant(strict, agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED),
		grant(set, agentpb.AgentSpec_ToolGrant_APPROVAL_UNSPECIFIED),
		grant(set+"/tools/Read", agentpb.AgentSpec_ToolGrant_APPROVAL_NOT_REQUIRED),
	}

	tools, err := subAgentTools(parentTools, nil)
	require.NoError(t, err)
	require.Len(t, tools, 5)

	tools, err = subAgentTools(parentTools, []*agentpb.AgentSpec_ToolGrant{
		grant(loose, agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED),
		grant(strict, agentpb.AgentSpec_ToolGrant_APPROVAL_UNSPECIFIED),
	})
	require.NoError(t, err)
	require.Equal(t, agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED, tools[0].GetApproval())
	require.Equal(t, agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED, tools[1].GetApproval())

	// One tool of a held set, under the parent's approval for it.
	tools, err = subAgentTools(parentTools, []*agentpb.AgentSpec_ToolGrant{grant(set+"/tools/Read", agentpb.AgentSpec_ToolGrant_APPROVAL_UNSPECIFIED)})
	require.NoError(t, err)
	require.Equal(t, agentpb.AgentSpec_ToolGrant_APPROVAL_NOT_REQUIRED, tools[0].GetApproval())
	// A held set keeps the parent's refinements, tightened with it.
	tools, err = subAgentTools(parentTools, []*agentpb.AgentSpec_ToolGrant{grant(set, agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED)})
	require.NoError(t, err)
	require.Len(t, tools, 2)
	require.Equal(t, set+"/tools/Read", tools[1].GetTool())
	require.Equal(t, agentpb.AgentSpec_ToolGrant_APPROVAL_REQUIRED, tools[1].GetApproval())

	_, err = subAgentTools(parentTools, []*agentpb.AgentSpec_ToolGrant{grant(policy, agentpb.AgentSpec_ToolGrant_APPROVAL_NOT_REQUIRED)})
	require.ErrorContains(t, err, "may not loosen")
	_, err = subAgentTools(parentTools, []*agentpb.AgentSpec_ToolGrant{grant(strict, agentpb.AgentSpec_ToolGrant_APPROVAL_NOT_REQUIRED)})
	require.ErrorContains(t, err, "may not loosen")
	_, err = subAgentTools(parentTools, []*agentpb.AgentSpec_ToolGrant{grant("toolSets/pkg.Loose", agentpb.AgentSpec_ToolGrant_APPROVAL_UNSPECIFIED)})
	require.ErrorContains(t, err, "you do not hold tool set")
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

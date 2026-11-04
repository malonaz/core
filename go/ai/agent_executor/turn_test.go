package agent_executor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
)

func TestRunTurnWithToolRound(t *testing.T) {
	aiServiceClient := newFakeAiServiceClient(
		generated(ai.NewTextBlock("checking"), ai.NewToolCallBlock(newToolCall(t, "c1", "echo", nil))),
		generated(ai.NewTextBlock("done")),
	)
	spec := &agentpb.AgentSpec{
		Instructions:    "be helpful",
		Model:           testModel,
		ReasoningEffort: aipb.ReasoningEffort_REASONING_EFFORT_HIGH,
		Tools:           []string{"echo"},
	}
	a, connection := newTestAgent(t, aiServiceClient, spec, echoTool("echo"))
	inputs := []*agentpb.Input{
		newInput("i1", "hello", testUser),
		newInput("i2", "sub-agent report", "organizations/o/users/u/agents/sub"),
	}
	a.runTurn(context.Background(), inputs)

	require.Equal(t, []string{"ROLE_SYSTEM", "ROLE_USER", "generate", "ROLE_TOOL", "generate"}, aiServiceClient.calls)
	instructionsRequest := aiServiceClient.createMessageRequests[0]
	require.Equal(t, testChat, instructionsRequest.GetParent())
	require.Equal(t, instructionsMessageID, instructionsRequest.GetMessageId())
	require.Equal(t, "be helpful", instructionsRequest.GetMessage().GetBlocks()[0].GetText())

	// One user message for every input, naming senders other than the agent's user.
	userMessage := aiServiceClient.persistedMessages(aipb.Role_ROLE_USER)[0]
	var texts []string
	for _, block := range userMessage.GetBlocks() {
		texts = append(texts, block.GetText())
	}
	require.Equal(t, []string{"hello", "[Message from organizations/o/users/u/agents/sub]", "sub-agent report"}, texts)
	require.NotEmpty(t, aiServiceClient.createMessageRequests[1].GetRequestId())

	generateMessageRequest := aiServiceClient.generateMessageRequests[0]
	require.Equal(t, testChat, generateMessageRequest.GetParent())
	require.Equal(t, testModel, generateMessageRequest.GetModel())
	require.Empty(t, generateMessageRequest.GetMessages())
	require.Equal(t, aipb.ReasoningEffort_REASONING_EFFORT_HIGH, generateMessageRequest.GetConfiguration().GetReasoningEffort())
	require.True(t, generateMessageRequest.GetConfiguration().GetStreamPartialToolCalls())
	var toolNames []string
	for _, tool := range generateMessageRequest.GetTools() {
		toolNames = append(toolNames, tool.GetName())
	}
	require.Equal(t, []string{createAgentsToolName, listAgentsToolName, sendInputToolName, updateLabelsToolName, "echo"}, toolNames)

	toolMessage := aiServiceClient.persistedMessages(aipb.Role_ROLE_TOOL)[0]
	require.Len(t, toolMessage.GetBlocks(), 1)
	require.Equal(t, "c1", toolMessage.GetBlocks()[0].GetToolResult().GetToolCallId())
	require.Equal(t, "ran c1", toolMessage.GetBlocks()[0].GetToolResult().GetContent())

	require.Len(t, connection.deltas(), 3)
	events := connection.events()
	require.Len(t, events, 6)
	require.NotNil(t, events[0].GetTurnStarted())
	require.Equal(t, aipb.Role_ROLE_USER, events[1].GetMessageCreated().GetMessage().GetRole())
	require.Equal(t, []string{inputs[0].GetName(), inputs[1].GetName()}, events[1].GetMessageCreated().GetInputs())
	require.NotEmpty(t, events[1].GetMessageCreated().GetMessage().GetName())
	require.Equal(t, aipb.Role_ROLE_ASSISTANT, events[2].GetMessageCreated().GetMessage().GetRole())
	require.Equal(t, aipb.Role_ROLE_TOOL, events[3].GetMessageCreated().GetMessage().GetRole())
	require.Empty(t, events[3].GetMessageCreated().GetInputs())
	require.Equal(t, "done", events[4].GetMessageCreated().GetMessage().GetBlocks()[0].GetText())
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_SUCCEEDED, connection.turnEnded(t).GetOutcome())
}

func TestRunTurnRunsToolCallsConcurrently(t *testing.T) {
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	allArrived := make(chan struct{})
	go func() {
		waitGroup.Wait()
		close(allArrived)
	}()
	barrier := &fakeTool{name: "barrier", execute: func(_ context.Context, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
		waitGroup.Done()
		select {
		case <-allArrived:
			return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), "ran "+toolCall.GetId()), nil
		case <-time.After(5 * time.Second):
			return nil, errors.New("tool calls ran sequentially")
		}
	}}
	aiServiceClient := newFakeAiServiceClient(
		generated(
			ai.NewToolCallBlock(newToolCall(t, "c1", "barrier", nil)),
			ai.NewToolCallBlock(newToolCall(t, "c2", "barrier", nil)),
		),
		generated(ai.NewTextBlock("done")),
	)
	a, connection := newTestAgent(t, aiServiceClient, &agentpb.AgentSpec{Model: testModel, Tools: []string{"barrier"}}, barrier)
	a.runTurn(context.Background(), []*agentpb.Input{newInput("i1", "go", "")})

	toolMessage := aiServiceClient.persistedMessages(aipb.Role_ROLE_TOOL)[0]
	require.Len(t, toolMessage.GetBlocks(), 2)
	for i, toolCallID := range []string{"c1", "c2"} {
		toolResult := toolMessage.GetBlocks()[i].GetToolResult()
		require.Equal(t, toolCallID, toolResult.GetToolCallId())
		require.Equal(t, "ran "+toolCallID, toolResult.GetContent())
	}
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_SUCCEEDED, connection.turnEnded(t).GetOutcome())
}

func TestRunTurnPicksUpInputsBetweenToolRounds(t *testing.T) {
	var a *agent
	lateInput := newInput("late", "also this", "")
	tool := &fakeTool{name: "work", execute: func(_ context.Context, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
		a.addInput(lateInput)
		return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), "worked"), nil
	}}
	aiServiceClient := newFakeAiServiceClient(
		generated(ai.NewToolCallBlock(newToolCall(t, "c1", "work", nil))),
		generated(ai.NewTextBlock("done")),
	)
	a, connection := newTestAgent(t, aiServiceClient, &agentpb.AgentSpec{Model: testModel, Tools: []string{"work"}}, tool)
	a.runTurn(context.Background(), []*agentpb.Input{newInput("i1", "go", "")})

	require.Equal(t, []string{"ROLE_USER", "generate", "ROLE_TOOL", "ROLE_USER", "generate"}, aiServiceClient.calls)
	lateMessage := aiServiceClient.persistedMessages(aipb.Role_ROLE_USER)[1]
	require.Equal(t, "also this", lateMessage.GetBlocks()[0].GetText())
	events := connection.events()
	require.Equal(t, []string{lateInput.GetName()}, events[4].GetMessageCreated().GetInputs())
	require.Empty(t, a.takeInputs())
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_SUCCEEDED, connection.turnEnded(t).GetOutcome())
}

func TestRunTurnCancelledDuringToolExecution(t *testing.T) {
	turnCtx, cancelTurn := context.WithCancel(context.Background())
	defer cancelTurn()
	release := make(chan struct{})
	defer close(release)
	slow := &fakeTool{name: "slow", execute: func(context.Context, *aipb.ToolCall) (*aipb.ToolResult, error) {
		cancelTurn()
		<-release
		return nil, errors.New("released")
	}}
	aiServiceClient := newFakeAiServiceClient(
		generated(
			ai.NewToolCallBlock(newToolCall(t, "c1", "echo", nil)),
			ai.NewToolCallBlock(newToolCall(t, "c2", "slow", nil)),
		),
	)
	a, connection := newTestAgent(t, aiServiceClient, &agentpb.AgentSpec{Model: testModel, Tools: []string{"echo", "slow"}}, echoTool("echo"), slow)
	a.runTurn(turnCtx, []*agentpb.Input{newInput("i1", "go", "")})

	require.Equal(t, agentpb.Agent_TURN_OUTCOME_CANCELLED, connection.turnEnded(t).GetOutcome())
	require.Len(t, aiServiceClient.generateMessageRequests, 1)
	// Every call stays paired: the unfinished one answered as cancelled.
	toolMessage := aiServiceClient.persistedMessages(aipb.Role_ROLE_TOOL)[0]
	require.Len(t, toolMessage.GetBlocks(), 2)
	require.Equal(t, "c1", toolMessage.GetBlocks()[0].GetToolResult().GetToolCallId())
	slowResult := toolMessage.GetBlocks()[1].GetToolResult()
	require.Equal(t, "c2", slowResult.GetToolCallId())
	require.Equal(t, errToolCallCancelled.Error(), slowResult.GetError().GetMessage())
	events := connection.events()
	require.Equal(t, aipb.Role_ROLE_TOOL, events[len(events)-2].GetMessageCreated().GetMessage().GetRole())
}

func TestRunTurnCancelledDuringGeneration(t *testing.T) {
	turnCtx, cancelTurn := context.WithCancel(context.Background())
	defer cancelTurn()
	generation := &fakeGeneration{block: true, started: make(chan struct{})}
	go func() {
		<-generation.started
		cancelTurn()
	}()
	aiServiceClient := newFakeAiServiceClient(generation)
	a, connection := newTestAgent(t, aiServiceClient, &agentpb.AgentSpec{Model: testModel})
	a.runTurn(turnCtx, []*agentpb.Input{newInput("i1", "go", "")})

	require.Equal(t, agentpb.Agent_TURN_OUTCOME_CANCELLED, connection.turnEnded(t).GetOutcome())
	require.Empty(t, aiServiceClient.persistedMessages(aipb.Role_ROLE_TOOL))
	require.Len(t, connection.events(), 3)
}

func TestRunTurnCancelledBeforeToolCallsRun(t *testing.T) {
	turnCtx, cancelTurn := context.WithCancel(context.Background())
	defer cancelTurn()
	echo := echoTool("echo")
	cancelTurn()
	aiServiceClient := newFakeAiServiceClient(generated(ai.NewToolCallBlock(newToolCall(t, "c1", "echo", nil))))
	a, connection := newTestAgent(t, aiServiceClient, &agentpb.AgentSpec{Model: testModel, Tools: []string{"echo"}}, echo)
	// The generated message lands although the turn is already cancelled.
	a.runTurn(turnCtx, []*agentpb.Input{newInput("i1", "go", "")})

	require.Equal(t, agentpb.Agent_TURN_OUTCOME_CANCELLED, connection.turnEnded(t).GetOutcome())
	require.Zero(t, echo.callCount())
	toolResult := aiServiceClient.persistedMessages(aipb.Role_ROLE_TOOL)[0].GetBlocks()[0].GetToolResult()
	require.Equal(t, "c1", toolResult.GetToolCallId())
	require.Equal(t, errToolCallCancelled.Error(), toolResult.GetError().GetMessage())
}

func TestRunTurnFeedsServerResolvedToolCallsBack(t *testing.T) {
	echo := echoTool("echo")
	resolvedToolCall := newToolCall(t, "c1", "echo", nil)
	resolvedToolCall.Result = ai.NewStructuredToolResult("echo", "c1", structpb.NewStringValue("resolved by the server"))
	aiServiceClient := newFakeAiServiceClient(
		generated(ai.NewToolCallBlock(resolvedToolCall)),
		generated(ai.NewTextBlock("done")),
	)
	a, connection := newTestAgent(t, aiServiceClient, &agentpb.AgentSpec{Model: testModel, Tools: []string{"echo"}}, echo)
	a.runTurn(context.Background(), []*agentpb.Input{newInput("i1", "go", "")})

	require.Zero(t, echo.callCount())
	toolResult := aiServiceClient.persistedMessages(aipb.Role_ROLE_TOOL)[0].GetBlocks()[0].GetToolResult()
	require.True(t, proto.Equal(resolvedToolCall.GetResult(), toolResult))
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_SUCCEEDED, connection.turnEnded(t).GetOutcome())
}

func TestRunTurnAnswersToolErrors(t *testing.T) {
	failing := &fakeTool{name: "failing", execute: func(context.Context, *aipb.ToolCall) (*aipb.ToolResult, error) {
		return nil, errors.New("disk full")
	}}
	aiServiceClient := newFakeAiServiceClient(
		generated(
			ai.NewToolCallBlock(newToolCall(t, "c1", "failing", nil)),
			// Provided by the executor, but not to this agent.
			ai.NewToolCallBlock(newToolCall(t, "c2", "echo", nil)),
		),
		generated(ai.NewTextBlock("done")),
	)
	a, connection := newTestAgent(t, aiServiceClient, &agentpb.AgentSpec{Model: testModel, Tools: []string{"failing"}}, failing, echoTool("echo"))
	a.runTurn(context.Background(), []*agentpb.Input{newInput("i1", "go", "")})

	blocks := aiServiceClient.persistedMessages(aipb.Role_ROLE_TOOL)[0].GetBlocks()
	require.Equal(t, "disk full", blocks[0].GetToolResult().GetError().GetMessage())
	require.Contains(t, blocks[1].GetToolResult().GetError().GetMessage(), `unknown tool "echo"`)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_SUCCEEDED, connection.turnEnded(t).GetOutcome())
}

func TestRunTurnGenerationFailure(t *testing.T) {
	aiServiceClient := newFakeAiServiceClient(&fakeGeneration{err: status.Errorf(codes.Unavailable, "provider down").Err()})
	a, connection := newTestAgent(t, aiServiceClient, &agentpb.AgentSpec{Model: testModel})
	a.runTurn(context.Background(), []*agentpb.Input{newInput("i1", "go", "")})

	turnEnded := connection.turnEnded(t)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_FAILED, turnEnded.GetOutcome())
	require.Equal(t, codes.Unavailable, grpcstatus.FromProto(turnEnded.GetStatus()).Code())
}

func TestRunTurnLeaseEndedReportsNothing(t *testing.T) {
	generation := &fakeGeneration{block: true, started: make(chan struct{})}
	aiServiceClient := newFakeAiServiceClient(generation)
	a, connection := newTestAgent(t, aiServiceClient, &agentpb.AgentSpec{Model: testModel})
	go func() {
		<-generation.started
		a.stop()
	}()
	turnCtx, cancelTurn := context.WithCancel(a.ctx)
	defer cancelTurn()
	a.runTurn(turnCtx, []*agentpb.Input{newInput("i1", "go", "")})

	for _, event := range connection.events() {
		require.Nil(t, event.GetTurnEnded())
	}
}

func TestRunTurnEnsuresInstructionsOncePerLease(t *testing.T) {
	aiServiceClient := newFakeAiServiceClient(
		generated(ai.NewTextBlock("one")),
		generated(ai.NewTextBlock("two")),
		generated(ai.NewTextBlock("three")),
	)
	spec := &agentpb.AgentSpec{Instructions: "be helpful", Model: testModel}
	countInstructionsRequests := func() int {
		var count int
		for _, createMessageRequest := range aiServiceClient.createMessageRequests {
			if createMessageRequest.GetMessageId() == instructionsMessageID {
				count++
			}
		}
		return count
	}

	a, connection := newTestAgent(t, aiServiceClient, spec)
	a.runTurn(context.Background(), []*agentpb.Input{newInput("i1", "go", "")})
	a.runTurn(context.Background(), []*agentpb.Input{newInput("i2", "again", "")})
	require.Equal(t, 1, countInstructionsRequests())
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_SUCCEEDED, connection.turnEnded(t).GetOutcome())

	// A new lease finds the system message already there.
	a, connection = newTestAgent(t, aiServiceClient, spec)
	a.runTurn(context.Background(), []*agentpb.Input{newInput("i3", "and again", "")})
	require.Equal(t, 2, countInstructionsRequests())
	require.Len(t, aiServiceClient.persistedMessages(aipb.Role_ROLE_SYSTEM), 2)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_SUCCEEDED, connection.turnEnded(t).GetOutcome())
}

func TestRunTurnWithoutInstructionsCreatesNoSystemMessage(t *testing.T) {
	aiServiceClient := newFakeAiServiceClient(generated(ai.NewTextBlock("done")))
	a, _ := newTestAgent(t, aiServiceClient, &agentpb.AgentSpec{Model: testModel})
	a.runTurn(context.Background(), []*agentpb.Input{newInput("i1", "go", "")})
	require.Empty(t, aiServiceClient.persistedMessages(aipb.Role_ROLE_SYSTEM))
}

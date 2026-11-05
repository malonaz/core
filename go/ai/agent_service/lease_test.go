package agent_service

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/uuid"
)

// newIdleReplica starts a replica that claims no turn: it stores agents and inputs only.
func newIdleReplica(t *testing.T, aiService *fakeAiService) *testReplica {
	t.Helper()
	return newTestReplica(t, aiService, func(opts *Opts) { opts.MaxConcurrentTurns = 0 })
}

// crashTurn leaves an agent as a replica that crashed mid-turn would: RUNNING under an expired lease.
func crashTurn(t *testing.T, replica *testReplica, agent string) {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(agent)
	require.NoError(t, err)
	require.NoError(t, replica.transact(context.Background(), func(ctx context.Context, tx *txn) error {
		lockedAgent, err := tx.getAgent(ctx, agent)
		if err != nil {
			return err
		}
		lockedAgent.State = agentpb.Agent_STATE_RUNNING
		if err := tx.saveAgent(ctx, lockedAgent); err != nil {
			return err
		}
		_, err = tx.ClaimLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, uuid.MustNewV7().String(), "unix:/crashed.sock", time.Microsecond)
		return err
	}))
}

// createMessage appends a message to a chat.
func createMessage(t *testing.T, aiService *fakeAiService, chat string, message *aipb.Message) *aipb.Message {
	t.Helper()
	createMessageRequest := &aiservicepb.CreateMessageRequest{Parent: chat, Message: message}
	createdMessage, err := aiService.CreateMessage(context.Background(), createMessageRequest)
	require.NoError(t, err)
	return createdMessage
}

// getLease reads an agent's lease.
func getLease(t *testing.T, replica *testReplica, agent string) string {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(agent)
	require.NoError(t, err)
	lease, err := replica.agentPostgresStore.GetLease(context.Background(), agentRn.Organization, agentRn.User, agentRn.Agent)
	require.NoError(t, err)
	if lease == nil {
		return ""
	}
	return lease.Replica
}

func TestLease_NoLostWakeups(t *testing.T) {
	service := newTestService(t)
	service.aiService.setDefaultGeneration(textGeneration("noted"))
	agent := createTestAgent(t, service)

	// Inputs race the turns they wake: each lands either in a running turn or wakes a new one.
	var waitGroup sync.WaitGroup
	for i := range 30 {
		waitGroup.Go(func() {
			time.Sleep(time.Duration(i) * time.Millisecond)
			sendText(t, service, agent.Name, "message")
		})
	}
	waitGroup.Wait()

	require.Eventually(t, func() bool {
		inputs := listInputs(t, service, agent.Name)
		for _, input := range inputs {
			if input.ConsumeTime == nil {
				return false
			}
		}
		return len(inputs) == 30 && getAgent(t, service, agent.Name).State == agentpb.Agent_STATE_IDLE
	}, waitTimeout, waitTick)
	// Every input was delivered exactly once.
	deliveredInputSet := map[string]struct{}{}
	for _, message := range service.aiService.messages(agent.Chat) {
		deliveredInputs, ok := agentpb.Annotations.Inputs.Get(message)
		if !ok {
			continue
		}
		for _, input := range strings.Split(deliveredInputs, ",") {
			require.NotContains(t, deliveredInputSet, input)
			deliveredInputSet[input] = struct{}{}
		}
	}
	require.Len(t, deliveredInputSet, 30)
	require.Empty(t, getLease(t, service, agent.Name))
}

func TestLease_DrainingIsIdempotent(t *testing.T) {
	aiService := newFakeAiService()
	idleReplica := newIdleReplica(t, aiService)
	agent := createTestAgent(t, idleReplica)
	firstInput := sendText(t, idleReplica, agent.Name, "first")
	secondInput := sendText(t, idleReplica, agent.Name, "second")
	// A crash between delivering the first input and consuming it left it pending.
	deliveredMessage := ai.NewUserMessage(firstInput.Message.Blocks...)
	agentpb.Annotations.Inputs.Set(deliveredMessage, firstInput.Name)
	createMessageRequest := &aiservicepb.CreateMessageRequest{
		Parent:    agent.Chat,
		Message:   deliveredMessage,
		MessageId: aip.NewDeterministicBase32ResourceID(inputsMessageNamespace, firstInput.Name),
	}
	_, err := aiService.CreateMessage(context.Background(), createMessageRequest)
	require.NoError(t, err)
	aiService.script(agent.Chat, textGeneration("both read"))

	service := newTestReplica(t, aiService, nil)

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	messages := aiService.messages(agent.Chat)
	require.Equal(t, []aipb.Role{aipb.Role_ROLE_USER, aipb.Role_ROLE_SYSTEM, aipb.Role_ROLE_USER, aipb.Role_ROLE_ASSISTANT}, roles(messages))
	deliveredInputs, _ := agentpb.Annotations.Inputs.Get(messages[2])
	require.Equal(t, secondInput.Name, deliveredInputs)
	for _, input := range listInputs(t, service, agent.Name) {
		require.NotNil(t, input.ConsumeTime)
	}
	require.Len(t, aiService.generateMessageRequests(), 1)
}

func TestLease_TakeoverAnswersDanglingToolCalls(t *testing.T) {
	aiService := newFakeAiService()
	idleReplica := newIdleReplica(t, aiService)
	agent := createTestAgent(t, idleReplica)
	createMessage(t, aiService, agent.Chat, ai.NewUserMessage(ai.NewTextBlock("check")))
	answeredCall := newToolCall("call-1", healthCheckTool, map[string]any{"service": "a"})
	danglingCall := newToolCall("call-2", healthCheckTool, map[string]any{"service": "b"})
	serverAnsweredCall := newToolCall("call-3", "Health_Discover", map[string]any{})
	serverAnsweredCall.Result = ai.NewToolResult("Health_Discover", "call-3", "discovered")
	createMessage(t, aiService, agent.Chat, ai.NewAssistantMessage(ai.NewToolCallBlock(answeredCall), ai.NewToolCallBlock(danglingCall), ai.NewToolCallBlock(serverAnsweredCall)))
	createMessage(t, aiService, agent.Chat, ai.NewToolMessage(ai.NewToolResultBlock(ai.NewToolResult(healthCheckTool, "call-1", "SERVING"))))
	crashTurn(t, idleReplica, agent.Name)
	aiService.script(agent.Chat, textGeneration("recovered"))

	service := newTestReplica(t, aiService, nil)

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	toolCallIDToToolResult := toolResults(aiService.messages(agent.Chat))
	require.Equal(t, "SERVING", toolCallIDToToolResult["call-1"].GetContent())
	require.Contains(t, toolCallIDToToolResult["call-2"].GetError().GetMessage(), "interrupted by a server restart")
	require.Equal(t, "discovered", toolCallIDToToolResult["call-3"].GetContent())
	require.Len(t, aiService.generateMessageRequests(), 1)
	require.Empty(t, getLease(t, service, agent.Name))
}

func TestLease_TakeoverRegeneratesAnUnansweredMessage(t *testing.T) {
	aiService := newFakeAiService()
	idleReplica := newIdleReplica(t, aiService)
	agent := createTestAgent(t, idleReplica)
	createMessage(t, aiService, agent.Chat, ai.NewUserMessage(ai.NewTextBlock("hello")))
	crashTurn(t, idleReplica, agent.Name)
	aiService.script(agent.Chat, textGeneration("hello back"))

	service := newTestReplica(t, aiService, nil)

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	messages := aiService.messages(agent.Chat)
	require.Equal(t, "hello back", messages[len(messages)-1].GetBlocks()[0].GetText())
}

func TestLease_TakeoverFinishesAnAnsweredTurnOnce(t *testing.T) {
	aiService := newFakeAiService()
	aiService.setDefaultGeneration(textGeneration("thanks"))
	idleReplica := newIdleReplica(t, aiService)
	parentAgent := createTestAgent(t, idleReplica)
	agent := createTestSubAgent(t, idleReplica, parentAgent)
	createMessage(t, aiService, agent.Chat, ai.NewUserMessage(ai.NewTextBlock("work")))
	createMessage(t, aiService, agent.Chat, ai.NewAssistantMessage(ai.NewTextBlock("final answer")))

	service := newTestReplica(t, aiService, nil)
	for range 2 {
		crashTurn(t, idleReplica, agent.Name)
		waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	}

	// Nothing was generated for the finished turn, and its report was queued once.
	for _, generateMessageRequest := range aiService.generateMessageRequests() {
		require.NotEqual(t, agent.Chat, generateMessageRequest.Parent)
	}
	inputs := listInputs(t, service, parentAgent.Name)
	require.Len(t, inputs, 1)
	require.Equal(t, "final answer", inputs[0].Message.Blocks[1].GetText())
}

func TestLease_LostLeaseIsTakenOver(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	started := make(chan struct{})
	service.aiService.script(agent.Chat, &fakeGeneration{block: true, started: started}, textGeneration("resumed"))
	sendText(t, service, agent.Name, "go")
	<-started

	// Another replica took the lease over, then died: the heartbeat stops this replica's turn, and once that lease
	// expires the claim loop resumes the turn here.
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	require.NoError(t, err)
	require.NoError(t, service.transact(context.Background(), func(ctx context.Context, tx *txn) error {
		if err := tx.DeleteLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent); err != nil {
			return err
		}
		_, err := tx.ClaimLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, uuid.MustNewV7().String(), "unix:/crashed.sock", 2*service.opts.HeartbeatInterval)
		return err
	}))

	waitForIdle(t, service, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	require.Len(t, service.aiService.generateMessageRequests(), 2)
}

func TestLease_CancelLocalTurn(t *testing.T) {
	healthServer := &fakeHealthServer{}
	service := newTestReplica(t, newFakeAiService(), nil, healthServer.register)
	agent := createTestAgent(t, service, healthCheckMethod)
	service.aiService.script(agent.Chat, toolCallGeneration(
		newToolCall("call-1", healthCheckTool, map[string]any{"service": "a"}),
		newToolCall("call-2", healthCheckTool, map[string]any{"service": "block"}),
	))
	sendText(t, service, agent.Name, "go")
	require.Eventually(t, func() bool {
		_, ok := toolResults(service.aiService.messages(agent.Chat))["call-1"]
		return ok && len(healthServer.checkedServices()) == 2
	}, waitTimeout, waitTick)

	cancelAgentRequest := &agentservicepb.CancelAgentRequest{Name: agent.Name}
	cancelledAgent, err := service.CancelAgent(context.Background(), cancelAgentRequest)

	require.NoError(t, err)
	require.Equal(t, agentpb.Agent_STATE_IDLE, cancelledAgent.State)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_CANCELLED, cancelledAgent.LastTurnOutcome)
	toolCallIDToToolResult := toolResults(service.aiService.messages(agent.Chat))
	require.Equal(t, "SERVING", toolCallIDToToolResult["call-1"].GetStructuredContent().GetStructValue().GetFields()["status"].GetStringValue())
	require.Contains(t, toolCallIDToToolResult["call-2"].GetError().GetMessage(), "cancelled")
	require.Empty(t, getLease(t, service, agent.Name))
}

func TestLease_CancelForwardsToTheReplicaRunningTheTurn(t *testing.T) {
	aiService := newFakeAiService()
	replicaA := newTestReplica(t, aiService, nil)
	replicaB := newTestReplica(t, aiService, nil)
	agent := createTestAgent(t, replicaA)
	started := make(chan struct{})
	aiService.script(agent.Chat, &fakeGeneration{block: true, started: started})
	sendText(t, replicaA, agent.Name, "go")
	<-started

	cancelAgentRequest := &agentservicepb.CancelAgentRequest{Name: agent.Name}
	cancelledAgent, err := replicaB.CancelAgent(context.Background(), cancelAgentRequest)

	require.NoError(t, err)
	require.Equal(t, agentpb.Agent_STATE_IDLE, cancelledAgent.State)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_CANCELLED, cancelledAgent.LastTurnOutcome)
}

func TestLease_CancelATurnNoReplicaRuns(t *testing.T) {
	idleReplica := newIdleReplica(t, newFakeAiService())
	agent := createTestAgent(t, idleReplica)
	crashTurn(t, idleReplica, agent.Name)

	cancelAgentRequest := &agentservicepb.CancelAgentRequest{Name: agent.Name}
	cancelledAgent, err := idleReplica.CancelAgent(context.Background(), cancelAgentRequest)

	require.NoError(t, err)
	require.Equal(t, agentpb.Agent_STATE_IDLE, cancelledAgent.State)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_CANCELLED, cancelledAgent.LastTurnOutcome)
	require.Empty(t, getLease(t, idleReplica, agent.Name))
}

func TestLease_DeleteCancelsTheTurn(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	started := make(chan struct{})
	service.aiService.script(agent.Chat, &fakeGeneration{block: true, started: started})
	sendText(t, service, agent.Name, "go")
	<-started

	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name}
	deletedAgent, err := service.DeleteAgent(context.Background(), deleteAgentRequest)

	require.NoError(t, err)
	require.NotNil(t, deletedAgent.DeleteTime)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_CANCELLED, deletedAgent.LastTurnOutcome)
	require.Empty(t, getLease(t, service, agent.Name))
	require.Eventually(t, func() bool { return service.turns.count() == 0 }, waitTimeout, waitTick)
}

func TestLease_ShutdownFinishesToolCallsThenHandsTheTurnOver(t *testing.T) {
	_, endpoint := newTestToolTarget(t)
	aiService := newFakeAiService()
	configure := func(opts *Opts) { opts.ToolTargets = map[string]string{healthService: endpoint} }
	replicaA := newTestReplica(t, aiService, configure)
	agent := createTestAgent(t, replicaA, healthCheckMethod)
	toolCallGeneration := toolCallGeneration(newToolCall("call-1", healthCheckTool, map[string]any{"service": "slow"}))
	toolCallGeneration.started = make(chan struct{})
	aiService.script(agent.Chat, toolCallGeneration, textGeneration("finished elsewhere"))
	sendText(t, replicaA, agent.Name, "go")
	<-toolCallGeneration.started

	replicaA.stop()

	// The slow call finished within the grace, then the lease was released at once.
	require.Equal(t, "SERVING", toolResults(aiService.messages(agent.Chat))["call-1"].GetStructuredContent().GetStructValue().GetFields()["status"].GetStringValue())
	require.Empty(t, getLease(t, replicaA, agent.Name))
	require.Equal(t, agentpb.Agent_STATE_RUNNING, getAgent(t, replicaA, agent.Name).State)

	replicaB := newTestReplica(t, aiService, configure)
	waitForIdle(t, replicaB, agent.Name, agentpb.Agent_TURN_OUTCOME_SUCCEEDED)
	messages := aiService.messages(agent.Chat)
	require.Equal(t, "finished elsewhere", messages[len(messages)-1].GetBlocks()[0].GetText())
}

func TestLease_DeleteCancelsTheTurnOnTheReplicaRunningIt(t *testing.T) {
	aiService := newFakeAiService()
	// Heartbeats too slow to notice the lost lease: only the forwarded cancellation stops the turn in time.
	replicaA := newTestReplica(t, aiService, func(opts *Opts) {
		opts.HeartbeatInterval = time.Minute
		opts.LeaseDuration = 3 * time.Minute
	})
	replicaB := newTestReplica(t, aiService, nil)
	agent := createTestAgent(t, replicaA)
	started := make(chan struct{})
	aiService.script(agent.Chat, &fakeGeneration{block: true, started: started})
	sendText(t, replicaA, agent.Name, "go")
	<-started

	deleteAgentRequest := &agentservicepb.DeleteAgentRequest{Name: agent.Name}
	_, err := replicaB.DeleteAgent(context.Background(), deleteAgentRequest)

	require.NoError(t, err)
	require.Eventually(t, func() bool { return replicaA.turns.count() == 0 }, 5*time.Second, waitTick)
}

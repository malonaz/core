package agent_service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
)

// testStream is a StreamAgent call through a replica's gRPC server, its responses read in the background.
type testStream struct {
	responses chan *agentservicepb.StreamAgentResponse
	cancel    context.CancelFunc
}

func openTestStream(t *testing.T, replica *testReplica, request *agentservicepb.StreamAgentRequest) *testStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	agentServiceClient, err := replica.peerClient(ctx, replica.address)
	require.NoError(t, err)
	stream, err := agentServiceClient.StreamAgent(ctx, request)
	require.NoError(t, err)
	testStream := &testStream{responses: make(chan *agentservicepb.StreamAgentResponse, 4096), cancel: cancel}
	go func() {
		defer close(testStream.responses)
		for {
			response, err := stream.Recv()
			if err != nil {
				return
			}
			testStream.responses <- response
		}
	}()
	return testStream
}

// next returns the stream's next response.
func (s *testStream) next(t *testing.T) *agentservicepb.StreamAgentResponse {
	t.Helper()
	select {
	case response, ok := <-s.responses:
		require.True(t, ok, "stream ended")
		return response
	case <-time.After(waitTimeout):
		require.FailNow(t, "no stream response")
		return nil
	}
}

// until reads responses until match accepts one, returning every response read.
func (s *testStream) until(t *testing.T, match func(*agentservicepb.StreamAgentResponse) bool) []*agentservicepb.StreamAgentResponse {
	t.Helper()
	var responses []*agentservicepb.StreamAgentResponse
	for {
		response := s.next(t)
		responses = append(responses, response)
		if match(response) {
			return responses
		}
	}
}

// isIdleSnapshot matches an agent's snapshot once its turn ended with the outcome.
func isIdleSnapshot(agent string, outcome agentpb.Turn_Outcome) func(*agentservicepb.StreamAgentResponse) bool {
	return func(response *agentservicepb.StreamAgentResponse) bool {
		snapshot := response.GetSnapshot()
		return response.Agent == agent && snapshot.GetState() == agentpb.Agent_STATE_IDLE && snapshot.GetLatestTurn().GetOutcome() == outcome
	}
}

func countContent[T any](responses []*agentservicepb.StreamAgentResponse) int {
	count := 0
	for _, response := range responses {
		if _, ok := response.Content.(T); ok {
			count++
		}
	}
	return count
}

func TestStream_LocalTurn(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	titledCall := newToolCall("call-1", updateLabelsToolName, map[string]any{"tool_call_title": "Publish progress", "set": map[string]any{"progress": "done"}})
	partialCall := newToolCall("call-1", updateLabelsToolName, map[string]any{"tool_call_title": "Publish progress", "set": map[string]any{}})
	partialCall.Partial = true
	toolCallGeneration := toolCallGeneration(titledCall)
	toolCallGeneration.deltas = []*aipb.Block{{Content: &aipb.Block_PartialToolCall{PartialToolCall: partialCall}}}
	finalGeneration := textGeneration("all done")
	finalGeneration.deltas = []*aipb.Block{ai.NewTextBlock("all")}
	service.aiService.script(agent.Chat, toolCallGeneration, finalGeneration)
	fullStream := openTestStream(t, service, &agentservicepb.StreamAgentRequest{Name: agent.Name, View: agentservicepb.AgentView_AGENT_VIEW_FULL})
	basicStream := openTestStream(t, service, &agentservicepb.StreamAgentRequest{Name: agent.Name})
	for _, stream := range []*testStream{fullStream, basicStream} {
		first := stream.next(t)
		require.Equal(t, agent.Name, first.Agent)
		require.Equal(t, agentpb.Agent_STATE_IDLE, first.GetSnapshot().GetState())
	}

	sendText(t, service, agent.Name, "go")

	responses := fullStream.until(t, isIdleSnapshot(agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED))
	require.Equal(t, 1, countContent[*agentservicepb.StreamAgentResponse_ToolCall_](responses))
	require.Equal(t, 1, countContent[*agentservicepb.StreamAgentResponse_ToolCallEnded_](responses))
	require.Equal(t, 2, countContent[*agentservicepb.StreamAgentResponse_Delta](responses))
	// The instructions, the delivered message, both assistant messages and the tool result.
	require.Equal(t, 5, countContent[*agentservicepb.StreamAgentResponse_Message](responses))
	var labelled bool
	for _, response := range responses {
		if toolCall := response.GetToolCall(); toolCall != nil {
			require.Equal(t, "call-1", toolCall.Id)
			require.Equal(t, updateLabelsToolName, toolCall.Name)
			require.Equal(t, "Publish progress", toolCall.Title)
		}
		labelled = labelled || response.GetSnapshot().GetLabels()["progress"] == "done"
	}
	require.True(t, labelled)

	responses = basicStream.until(t, isIdleSnapshot(agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED))
	require.Equal(t, 1, countContent[*agentservicepb.StreamAgentResponse_ToolCall_](responses))
	require.Zero(t, countContent[*agentservicepb.StreamAgentResponse_Delta](responses))
	require.Zero(t, countContent[*agentservicepb.StreamAgentResponse_Message](responses))
	etagSet := map[string]struct{}{}
	for _, response := range responses {
		if snapshot := response.GetSnapshot(); snapshot != nil {
			require.NotContains(t, etagSet, snapshot.Etag)
			etagSet[snapshot.Etag] = struct{}{}
		}
	}
}

func TestStream_IncludesLaterDescendants(t *testing.T) {
	service := newTestService(t)
	agent := createTestAgent(t, service)
	stream := openTestStream(t, service, &agentservicepb.StreamAgentRequest{Name: agent.Name, IncludeDescendants: true})
	require.Equal(t, agent.Name, stream.next(t).Agent)

	subAgent := createTestSubAgent(t, service, agent)
	subSubAgent := createTestSubAgent(t, service, subAgent)
	createTestAgent(t, service)

	for _, descendant := range []*agentpb.Agent{subAgent, subSubAgent} {
		response := stream.next(t)
		require.Equal(t, descendant.Name, response.Agent)
		require.Equal(t, descendant.Etag, response.GetSnapshot().GetEtag())
	}
}

func TestStream_ProxiesTheReplicaRunningTheTurn(t *testing.T) {
	aiService := newFakeAiService()
	replicaA := newTestReplica(t, aiService, nil)
	replicaB := newTestReplica(t, aiService, nil)
	agent := createTestAgent(t, replicaA)
	gate := make(chan struct{})
	aiService.script(agent.Chat,
		&fakeGeneration{gate: gate, deltas: []*aipb.Block{ai.NewTextBlock("cut")}, block: true},
		textGeneration("after the takeover"),
	)

	sendText(t, replicaA, agent.Name, "go")
	stream := openTestStream(t, replicaB, &agentservicepb.StreamAgentRequest{Name: agent.Name, View: agentservicepb.AgentView_AGENT_VIEW_FULL})
	require.Equal(t, agentpb.Agent_STATE_RUNNING, stream.next(t).GetSnapshot().GetState())
	// Replica B proxies replica A once A's hub holds its forwarded stream.
	require.Eventually(t, func() bool {
		replicaA.hub.mutex.RLock()
		defer replicaA.hub.mutex.RUnlock()
		return len(replicaA.hub.agentToSubscriptions[agent.Name]) > 0
	}, waitTimeout, waitTick)
	close(gate)
	stream.until(t, func(response *agentservicepb.StreamAgentResponse) bool {
		return response.GetDelta().GetText() == "cut"
	})

	// Replica A stops mid-generation: B takes the turn over, and its stream resets the cut generation.
	replicaA.stop()

	responses := stream.until(t, isIdleSnapshot(agent.Name, agentpb.Turn_OUTCOME_SUCCEEDED))
	resetIndex, messageIndex := -1, -1
	for i, response := range responses {
		if response.GetGenerationReset() != nil && resetIndex == -1 {
			resetIndex = i
		}
		if response.GetMessage().GetRole() == aipb.Role_ROLE_ASSISTANT {
			messageIndex = i
		}
	}
	require.NotEqual(t, -1, resetIndex)
	require.Greater(t, messageIndex, resetIndex)
	require.Equal(t, "after the takeover", responses[messageIndex].GetMessage().GetBlocks()[0].GetText())
}

func TestHub_OverflowingSubscriptionIsFlagged(t *testing.T) {
	hub := newHub()
	slowSubscription := hub.subscribe()
	hub.add(slowSubscription, "agent")
	response := &agentservicepb.StreamAgentResponse{Agent: "agent", Content: &agentservicepb.StreamAgentResponse_GenerationReset_{}}

	for range subscriptionBufferSize {
		hub.publish(response)
	}
	select {
	case <-slowSubscription.overflowed:
		require.FailNow(t, "overflowed within its buffer")
	default:
	}
	hub.publish(response)
	<-slowSubscription.overflowed

	hub.unsubscribe(slowSubscription, "agent")
	require.Empty(t, hub.agentToSubscriptions)
}

func TestStream_QueuedMessagesReachEveryReplica(t *testing.T) {
	aiService := newFakeAiService()
	replicaA := newIdleReplica(t, aiService)
	replicaB := newIdleReplica(t, aiService)
	agent := createTestAgent(t, replicaA)
	stream := openTestStream(t, replicaB, &agentservicepb.StreamAgentRequest{Name: agent.Name})
	require.Equal(t, agent.Name, stream.next(t).Agent)

	// Queued through the other replica, the message reaches B's stream through its poll; through B, through its hub.
	sendText(t, replicaA, agent.Name, "through a")
	sendText(t, replicaB, agent.Name, "through b")

	stream.until(t, func(response *agentservicepb.StreamAgentResponse) bool {
		return len(response.GetSnapshot().GetQueuedMessages()) == 2
	})
}

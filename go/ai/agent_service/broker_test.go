package agent_service

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

var (
	brokerRootAgentRn    = &agentpb.AgentRn{Organization: "broker", User: "user", Agent: "root"}
	brokerRootAgent      = brokerRootAgentRn.String()
	brokerOtherRootAgent = brokerRootAgentRn.UserRn().AgentRn("other").String()
)

func newBrokerEvent(sequence int64) *agentpb.Event {
	return &agentpb.Event{
		Name:     brokerRootAgentRn.EventRn(strconv.FormatInt(sequence, 10)).String(),
		Sequence: sequence,
		Kind:     &agentpb.Event_TurnStarted_{TurnStarted: &agentpb.Event_TurnStarted{}},
	}
}

func newBrokerDelta() *agentpb.Delta {
	return &agentpb.Delta{Agent: brokerRootAgent, Block: &aipb.Block{}}
}

// drain returns everything buffered on a subscription without blocking: publishing is synchronous.
func drain[T any](subscription *subscription[T]) []T {
	var values []T
	for len(subscription.C) > 0 {
		values = append(values, <-subscription.C)
	}
	return values
}

func requireEvents(t *testing.T, responses []*agentservicepb.StreamAgentResponse, sequences ...int64) {
	t.Helper()
	require.Len(t, responses, len(sequences))
	for i, response := range responses {
		require.Equal(t, sequences[i], response.GetEvent().GetSequence())
	}
}

func TestLocalBroker_TreeFanOut(t *testing.T) {
	ctx := context.Background()
	broker := newLocalBroker()
	basic := broker.subscribeTree(brokerRootAgent, false)
	defer basic.Close()
	full := broker.subscribeTree(brokerRootAgent, true)
	defer full.Close()
	other := broker.subscribeTree(brokerOtherRootAgent, true)
	defer other.Close()

	broker.publishEvents(ctx, brokerRootAgent, []*agentpb.Event{newBrokerEvent(1), newBrokerEvent(2)})
	delta := newBrokerDelta()
	broker.publishDelta(ctx, brokerRootAgent, delta)

	requireEvents(t, drain(basic), 1, 2)
	fullResponses := drain(full)
	requireEvents(t, fullResponses[:2], 1, 2)
	require.Len(t, fullResponses, 3)
	require.True(t, proto.Equal(delta, fullResponses[2].GetDelta()))
	require.Empty(t, drain(other))
}

func TestLocalBroker_Sessions(t *testing.T) {
	ctx := context.Background()
	broker := newLocalBroker()
	first := broker.subscribeSession("first")
	defer first.Close()
	firstAgain := broker.subscribeSession("first")
	defer firstAgain.Close()
	second := broker.subscribeSession("second")
	defer second.Close()

	response := &agentservicepb.ConnectExecutorResponse{
		Content: &agentservicepb.ConnectExecutorResponse_Input{Input: &agentpb.Input{Name: brokerRootAgentRn.InputRn("input").String()}},
	}
	broker.sendToSession(ctx, "first", response)
	// A session nobody subscribes to drops the command.
	broker.sendToSession(ctx, "missing", response)

	require.Equal(t, []*agentservicepb.ConnectExecutorResponse{response}, drain(first))
	require.Equal(t, []*agentservicepb.ConnectExecutorResponse{response}, drain(firstAgain))
	require.Empty(t, drain(second))
}

func TestLocalBroker_Close(t *testing.T) {
	ctx := context.Background()
	broker := newLocalBroker()
	closed := broker.subscribeTree(brokerRootAgent, true)
	open := broker.subscribeTree(brokerRootAgent, true)
	defer open.Close()
	closedSession := broker.subscribeSession("session")

	closed.Close()
	closedSession.Close()
	broker.publishEvents(ctx, brokerRootAgent, []*agentpb.Event{newBrokerEvent(1)})
	broker.publishDelta(ctx, brokerRootAgent, newBrokerDelta())
	broker.sendToSession(ctx, "session", &agentservicepb.ConnectExecutorResponse{})

	require.Empty(t, drain(closed))
	require.Empty(t, drain(closedSession))
	require.Len(t, drain(open), 2)

	// Closing twice is harmless, and the last subscriber leaving forgets its key.
	closed.Close()
	open.Close()
	closedSession.Close()
	require.Empty(t, broker.rootAgentToTreeSubscribers)
	require.Empty(t, broker.sessionToSessionSubscribers)
}

func TestLocalBroker_DropsOnFullBuffer(t *testing.T) {
	ctx := context.Background()
	broker := newLocalBroker()
	slow := broker.subscribeTree(brokerRootAgent, false)
	defer slow.Close()
	slowSession := broker.subscribeSession("session")
	defer slowSession.Close()

	// Publishing never blocks on a full subscriber: the overflow is dropped, the oldest traffic kept.
	events := make([]*agentpb.Event, subscriptionBufferSize+10)
	for i := range events {
		events[i] = newBrokerEvent(int64(i + 1))
	}
	broker.publishEvents(ctx, brokerRootAgent, events)
	for range subscriptionBufferSize + 10 {
		broker.sendToSession(ctx, "session", &agentservicepb.ConnectExecutorResponse{})
	}

	responses := drain(slow)
	require.Len(t, responses, subscriptionBufferSize)
	for i, response := range responses {
		require.Equal(t, int64(i+1), response.GetEvent().GetSequence())
	}
	require.Len(t, drain(slowSession), subscriptionBufferSize)

	// Once drained, traffic flows again.
	broker.publishEvents(ctx, brokerRootAgent, []*agentpb.Event{newBrokerEvent(int64(len(events) + 1))})
	requireEvents(t, drain(slow), int64(len(events)+1))
}

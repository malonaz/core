package agent_service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/gengo/ai/agent/store"
	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/grpcinproc"
)

func newTurnStartedEvent() *agentpb.Event {
	return &agentpb.Event{Kind: &agentpb.Event_TurnStarted_{TurnStarted: &agentpb.Event_TurnStarted{}}}
}

func newSubAgentCreatedEvent(subAgent string) *agentpb.Event {
	return &agentpb.Event{Kind: &agentpb.Event_SubAgentCreated_{SubAgentCreated: &agentpb.Event_SubAgentCreated{Agent: subAgent}}}
}

// appendTestEvents appends events to an agent's log and publishes them, as every service write does.
func appendTestEvents(t *testing.T, service *Service, agent string, events ...*agentpb.Event) []*agentpb.Event {
	t.Helper()
	err := service.transact(context.Background(), func(ctx context.Context, t *txn) error {
		lockedAgent, err := t.getAgent(ctx, agent)
		if err != nil {
			return err
		}
		return t.saveAgent(ctx, lockedAgent, events...)
	})
	require.NoError(t, err)
	return events
}

// writeTestEvents appends events to an agent's log without publishing them, as if the broker had lost them.
func writeTestEvents(t *testing.T, service *Service, agent string, events ...*agentpb.Event) []*agentpb.Event {
	t.Helper()
	err := service.agentPostgresStore.Transaction(context.Background(), func(tx *store.Tx) error {
		transaction := &txn{Tx: tx, service: service, now: time.Now().UTC()}
		lockedAgent, err := transaction.getAgent(context.Background(), agent)
		if err != nil {
			return err
		}
		return transaction.saveAgent(context.Background(), lockedAgent, events...)
	})
	require.NoError(t, err)
	return events
}

// insertTestSubAgent inserts a sub-agent of parent and records its launch on the parent.
func insertTestSubAgent(t *testing.T, service *Service, parent *agentpb.AgentRn, rootAgent, agentID string) *agentpb.AgentRn {
	t.Helper()
	agentRn := parent.UserRn().AgentRn(agentID)
	insertTestAgent(t, agentRn, func(agent *agentpb.Agent) {
		agent.ParentAgent = parent.String()
		agent.RootAgent = rootAgent
	})
	appendTestEvents(t, service, parent.String(), newSubAgentCreatedEvent(agentRn.String()))
	return agentRn
}

// testStream is a StreamAgent call driven in-process.
type testStream struct {
	t      *testing.T
	client interface {
		Recv() (*agentservicepb.StreamAgentResponse, error)
	}
}

func openTestStream(t *testing.T, service *Service, request *agentservicepb.StreamAgentRequest) *testStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	streamAgent := grpcinproc.NewServerStreamAsClient[
		agentservicepb.StreamAgentRequest,
		agentservicepb.StreamAgentResponse,
		agentservicepb.AgentService_StreamAgentServer,
	](service.StreamAgent)
	client, err := streamAgent(ctx, request)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.CloseSend()
		cancel()
	})
	return &testStream{t: t, client: client}
}

func (s *testStream) next() *agentservicepb.StreamAgentResponse {
	s.t.Helper()
	response, err := s.client.Recv()
	require.NoError(s.t, err)
	return response
}

func (s *testStream) nextEvent() *agentpb.Event {
	s.t.Helper()
	response := s.next()
	require.NotNil(s.t, response.GetEvent(), "expected an event, got %v", response)
	return response.GetEvent()
}

// eventsUntilCaughtUp returns the replayed events, failing if anything but events precedes CaughtUp.
func (s *testStream) eventsUntilCaughtUp() []*agentpb.Event {
	s.t.Helper()
	var events []*agentpb.Event
	for {
		response := s.next()
		if response.GetCaughtUp() != nil {
			return events
		}
		require.NotNil(s.t, response.GetEvent(), "expected an event, got %v", response)
		events = append(events, response.GetEvent())
	}
}

// requireSequences checks events are one agent's, with exactly these sequences.
func requireSequences(t *testing.T, agent string, events []*agentpb.Event, from, through int64) {
	t.Helper()
	require.Len(t, events, int(through-from+1))
	for i, event := range events {
		eventRn, err := agentpb.ParseEventRn(event.Name)
		require.NoError(t, err)
		require.Equal(t, agent, eventRn.Parent())
		require.Equal(t, from+int64(i), event.Sequence)
	}
}

func TestStreamAgent_ReplayThenLiveUnderConcurrentWrites(t *testing.T) {
	service := newTestService(t)
	agentRn := newTestUser(t).AgentRn("agent")
	agent := agentRn.String()
	insertTestAgent(t, agentRn, nil)
	appendTestEvents(t, service, agent, newTurnStartedEvent(), newTurnStartedEvent(), newTurnStartedEvent())

	// Writes race the stream's subscription and replay.
	const concurrentWrites = 60
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for range concurrentWrites {
			appendTestEvents(t, service, agent, newTurnStartedEvent())
		}
	}()
	streamAgentRequest := &agentservicepb.StreamAgentRequest{Name: agent}
	stream := openTestStream(t, service, streamAgentRequest)

	replayed := stream.eventsUntilCaughtUp()
	require.GreaterOrEqual(t, len(replayed), 3)
	live := []*agentpb.Event{}
	for len(replayed)+len(live) < 3+concurrentWrites {
		live = append(live, stream.nextEvent())
	}
	waitGroup.Wait()
	requireSequences(t, agent, append(replayed, live...), 1, 3+concurrentWrites)

	// Nothing else was sent: no duplicate, no second CaughtUp.
	appendTestEvents(t, service, agent, newTurnStartedEvent())
	require.Equal(t, int64(4+concurrentWrites), stream.nextEvent().Sequence)
}

func TestStreamAgent_AfterSequence(t *testing.T) {
	service := newTestService(t)
	agentRn := newTestUser(t).AgentRn("agent")
	agent := agentRn.String()
	insertTestAgent(t, agentRn, nil)
	for range 7 {
		appendTestEvents(t, service, agent, newTurnStartedEvent())
	}

	streamAgentRequest := &agentservicepb.StreamAgentRequest{Name: agent, AfterSequence: 4}
	stream := openTestStream(t, service, streamAgentRequest)
	requireSequences(t, agent, stream.eventsUntilCaughtUp(), 5, 7)
	appendTestEvents(t, service, agent, newTurnStartedEvent())
	require.Equal(t, int64(8), stream.nextEvent().Sequence)

	// The agent's last sequence streams live activity only.
	streamAgentRequest = &agentservicepb.StreamAgentRequest{Name: agent, AfterSequence: 8}
	stream = openTestStream(t, service, streamAgentRequest)
	require.Empty(t, stream.eventsUntilCaughtUp())
	appendTestEvents(t, service, agent, newTurnStartedEvent())
	require.Equal(t, int64(9), stream.nextEvent().Sequence)
}

func TestStreamAgent_FillsGapsAndDropsDuplicates(t *testing.T) {
	ctx := context.Background()
	service := newTestService(t)
	agentRn := newTestUser(t).AgentRn("agent")
	agent := agentRn.String()
	insertTestAgent(t, agentRn, nil)
	appendTestEvents(t, service, agent, newTurnStartedEvent())

	streamAgentRequest := &agentservicepb.StreamAgentRequest{Name: agent}
	stream := openTestStream(t, service, streamAgentRequest)
	requireSequences(t, agent, stream.eventsUntilCaughtUp(), 1, 1)

	// Events 2 to 4 are committed but only 4 reaches the broker: 2 and 3 are read from the store.
	events := writeTestEvents(t, service, agent, newTurnStartedEvent(), newTurnStartedEvent(), newTurnStartedEvent())
	service.broker.publishEvents(ctx, agent, events[2:])
	requireSequences(t, agent, []*agentpb.Event{stream.nextEvent(), stream.nextEvent(), stream.nextEvent()}, 2, 4)

	// A late duplicate is dropped.
	service.broker.publishEvents(ctx, agent, events[:1])
	appendTestEvents(t, service, agent, newTurnStartedEvent())
	require.Equal(t, int64(5), stream.nextEvent().Sequence)
}

func TestStreamAgent_IncludeDescendants(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	rootRn := userRn.AgentRn("root")
	root := rootRn.String()
	insertTestAgent(t, rootRn, nil)
	appendTestEvents(t, service, root, newTurnStartedEvent())
	// The streamed agent, its present child, and a sibling branch outside its subtree.
	agentRn := insertTestSubAgent(t, service, rootRn, root, "agent")
	siblingRn := insertTestSubAgent(t, service, rootRn, root, "sibling")
	appendTestEvents(t, service, agentRn.String(), newTurnStartedEvent())
	childRn := insertTestSubAgent(t, service, agentRn, root, "child")
	appendTestEvents(t, service, childRn.String(), newTurnStartedEvent(), newTurnStartedEvent())
	appendTestEvents(t, service, siblingRn.String(), newTurnStartedEvent())

	streamAgentRequest := &agentservicepb.StreamAgentRequest{Name: agentRn.String(), IncludeDescendants: true, AfterSequence: 1}
	stream := openTestStream(t, service, streamAgentRequest)
	replayed := stream.eventsUntilCaughtUp()

	// The agent from after_sequence, its descendants from their beginning, in one chronological order.
	agentToEvents := groupEventsByAgent(t, replayed)
	require.Len(t, agentToEvents, 2)
	requireSequences(t, agentRn.String(), agentToEvents[agentRn.String()], 2, 2)
	requireSequences(t, childRn.String(), agentToEvents[childRn.String()], 1, 2)
	for i := 1; i < len(replayed); i++ {
		require.False(t, replayed[i].CreateTime.AsTime().Before(replayed[i-1].CreateTime.AsTime()))
	}

	// A sub-agent created mid-stream, and its own child, stream from their first event.
	newChildRn := insertTestSubAgent(t, service, agentRn, root, "new-child")
	appendTestEvents(t, service, newChildRn.String(), newTurnStartedEvent())
	grandchildRn := insertTestSubAgent(t, service, newChildRn, root, "grandchild")
	appendTestEvents(t, service, grandchildRn.String(), newTurnStartedEvent())
	// A descendant whose events arrive before its parent's launch is known: resolved through its parent chain.
	lateRn := userRn.AgentRn("late")
	insertTestAgent(t, lateRn, func(agent *agentpb.Agent) {
		agent.ParentAgent = agentRn.String()
		agent.RootAgent = root
	})
	earlyRn := insertTestSubAgent(t, service, lateRn, root, "early")
	appendTestEvents(t, service, earlyRn.String(), newTurnStartedEvent())
	appendTestEvents(t, service, agentRn.String(), newSubAgentCreatedEvent(lateRn.String()))
	// Activity outside the subtree, before and after.
	appendTestEvents(t, service, siblingRn.String(), newTurnStartedEvent())
	insertTestSubAgent(t, service, siblingRn, root, "nephew")
	appendTestEvents(t, service, root, newTurnStartedEvent())
	// A sentinel: everything streamed before it was sent in order.
	sentinel := appendTestEvents(t, service, agentRn.String(), newTurnStartedEvent())[0]

	var live []*agentpb.Event
	for {
		event := stream.nextEvent()
		live = append(live, event)
		if event.Name == sentinel.Name {
			break
		}
	}
	agentToEvents = groupEventsByAgent(t, live)
	require.Len(t, agentToEvents, 5)
	requireSequences(t, agentRn.String(), agentToEvents[agentRn.String()], 3, 5)
	requireSequences(t, newChildRn.String(), agentToEvents[newChildRn.String()], 1, 2)
	requireSequences(t, grandchildRn.String(), agentToEvents[grandchildRn.String()], 1, 1)
	requireSequences(t, lateRn.String(), agentToEvents[lateRn.String()], 1, 1)
	requireSequences(t, earlyRn.String(), agentToEvents[earlyRn.String()], 1, 1)

	// Streaming the root covers the whole tree.
	streamAgentRequest = &agentservicepb.StreamAgentRequest{Name: root, IncludeDescendants: true}
	stream = openTestStream(t, service, streamAgentRequest)
	agentToEvents = groupEventsByAgent(t, stream.eventsUntilCaughtUp())
	// Every agent of the tree but the nephew, which has no events.
	require.Len(t, agentToEvents, 8)
	requireSequences(t, siblingRn.String(), agentToEvents[siblingRn.String()], 1, 3)
}

// groupEventsByAgent groups events by agent, keeping their order.
func groupEventsByAgent(t *testing.T, events []*agentpb.Event) map[string][]*agentpb.Event {
	t.Helper()
	agentToEvents := map[string][]*agentpb.Event{}
	for _, event := range events {
		eventRn, err := agentpb.ParseEventRn(event.Name)
		require.NoError(t, err)
		agentToEvents[eventRn.Parent()] = append(agentToEvents[eventRn.Parent()], event)
	}
	return agentToEvents
}

func TestStreamAgent_Deltas(t *testing.T) {
	ctx := context.Background()
	service := newTestService(t)
	userRn := newTestUser(t)
	agentRn := userRn.AgentRn("agent")
	agent := agentRn.String()
	insertTestAgent(t, agentRn, nil)
	otherRn := insertTestSubAgent(t, service, agentRn, agent, "other")
	delta := &agentpb.Delta{Agent: agent, Block: &aipb.Block{}}
	otherDelta := &agentpb.Delta{Agent: otherRn.String(), Block: &aipb.Block{}}

	fullRequest := &agentservicepb.StreamAgentRequest{Name: agent, View: agentservicepb.AgentView_AGENT_VIEW_FULL}
	full := openTestStream(t, service, fullRequest)
	basicRequest := &agentservicepb.StreamAgentRequest{Name: agent, View: agentservicepb.AgentView_AGENT_VIEW_BASIC}
	basic := openTestStream(t, service, basicRequest)
	full.eventsUntilCaughtUp()
	basic.eventsUntilCaughtUp()

	// Only members' deltas reach a FULL stream; a BASIC one gets none.
	service.broker.publishDelta(ctx, agent, otherDelta)
	service.broker.publishDelta(ctx, agent, delta)
	appendTestEvents(t, service, agent, newTurnStartedEvent())
	require.True(t, proto.Equal(delta, full.next().GetDelta()))
	require.Equal(t, int64(2), full.nextEvent().Sequence)
	require.Equal(t, int64(2), basic.nextEvent().Sequence)
}

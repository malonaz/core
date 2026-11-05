package agent_service

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/grpcinproc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

const (
	// testTimeout bounds every wait of a test on the service.
	testTimeout = 10 * time.Second
	// testHeartbeatInterval keeps test executors well within the test opts' heartbeat timeout.
	testHeartbeatInterval = 300 * time.Millisecond
)

// testExecutor is an executor connected in-process through ConnectExecutor. It heartbeats until its stream ends.
type testExecutor struct {
	t          *testing.T
	executorRn *agentpb.ExecutorRn
	name       string
	cancel     context.CancelFunc
	client     grpc.BidiStreamingClient[agentservicepb.ConnectExecutorRequest, agentservicepb.ConnectExecutorResponse]
	responses  chan *agentservicepb.ConnectExecutorResponse
	// Receives the stream's end once: nil on a clean end, else the error.
	done chan error
	// Closed once the stream ended: in-process sends would otherwise block forever.
	ended chan struct{}
	// The executor the service registered.
	registered *agentpb.Executor
}

// connectTestExecutor connects an executor of a user and waits for its registration. A nil executor gets
// capacity for two agents and no labels or tools.
func connectTestExecutor(t *testing.T, service *Service, userRn *agentpb.UserRn, executorID string, executor *agentpb.Executor) *testExecutor {
	t.Helper()
	if executor == nil {
		executor = &agentpb.Executor{MaxAgents: 2}
	}
	ctx, cancel := context.WithCancel(context.Background())
	connectExecutor := grpcinproc.NewBidiStreamAsClient[
		agentservicepb.ConnectExecutorRequest,
		agentservicepb.ConnectExecutorResponse,
		agentservicepb.AgentService_ConnectExecutorServer,
	](service.ConnectExecutor)
	client, err := connectExecutor(ctx)
	require.NoError(t, err)
	testExecutor := &testExecutor{
		t:          t,
		executorRn: userRn.ExecutorRn(executorID),
		name:       userRn.ExecutorRn(executorID).String(),
		cancel:     cancel,
		client:     client,
		responses:  make(chan *agentservicepb.ConnectExecutorResponse, 1024),
		done:       make(chan error, 1),
		ended:      make(chan struct{}),
	}
	t.Cleanup(cancel)
	go testExecutor.receive()

	register := &agentservicepb.ConnectExecutorRequest{Content: &agentservicepb.ConnectExecutorRequest_Register_{
		Register: &agentservicepb.ConnectExecutorRequest_Register{Parent: userRn.String(), ExecutorId: executorID, Executor: executor},
	}}
	testExecutor.send(register)
	testExecutor.registered = testExecutor.recv().GetRegistered()
	require.NotNil(t, testExecutor.registered, "the first response must be registered")
	go testExecutor.heartbeat(ctx)
	return testExecutor
}

func (e *testExecutor) receive() {
	for {
		response, err := e.client.Recv()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			e.done <- err
			close(e.ended)
			close(e.responses)
			return
		}
		e.responses <- response
	}
}

func (e *testExecutor) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(testHeartbeatInterval)
	defer ticker.Stop()
	heartbeat := &agentservicepb.ConnectExecutorRequest{Content: &agentservicepb.ConnectExecutorRequest_Heartbeat_{
		Heartbeat: &agentservicepb.ConnectExecutorRequest_Heartbeat{},
	}}
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.ended:
			return
		case <-ticker.C:
			if e.client.Send(heartbeat) != nil {
				return
			}
		}
	}
}

func (e *testExecutor) send(request *agentservicepb.ConnectExecutorRequest) {
	e.t.Helper()
	sent := make(chan error, 1)
	go func() { sent <- e.client.Send(request) }()
	select {
	case err := <-sent:
		require.NoError(e.t, err)
	case <-e.ended:
		require.FailNow(e.t, "the service ended the stream", "%v", <-e.done)
	}
}

// recv returns the next response, failing the test if none comes in time.
func (e *testExecutor) recv() *agentservicepb.ConnectExecutorResponse {
	e.t.Helper()
	select {
	case response, ok := <-e.responses:
		require.True(e.t, ok, "the stream ended")
		return response
	case <-time.After(testTimeout):
		require.FailNow(e.t, "no response from the service")
		return nil
	}
}

func (e *testExecutor) recvLease() *agentpb.Agent {
	e.t.Helper()
	response := e.recv()
	require.NotNil(e.t, response.GetLease(), "expected a lease, got %v", response)
	return response.GetLease()
}

func (e *testExecutor) recvInput() *agentpb.Input {
	e.t.Helper()
	response := e.recv()
	require.NotNil(e.t, response.GetInput(), "expected an input, got %v", response)
	return response.GetInput()
}

// requireNoResponse fails if the service sends anything for a while.
func (e *testExecutor) requireNoResponse(duration time.Duration) {
	e.t.Helper()
	select {
	case response, ok := <-e.responses:
		if ok {
			require.FailNow(e.t, "unexpected response", "%v", response)
		}
	case <-time.After(duration):
	}
}

func (e *testExecutor) report(agent string, event *agentpb.Event) {
	e.t.Helper()
	e.send(&agentservicepb.ConnectExecutorRequest{Content: &agentservicepb.ConnectExecutorRequest_Report_{
		Report: &agentservicepb.ConnectExecutorRequest_Report{Agent: agent, Event: event},
	}})
}

func (e *testExecutor) release(agent string) {
	e.t.Helper()
	e.send(&agentservicepb.ConnectExecutorRequest{Content: &agentservicepb.ConnectExecutorRequest_Release_{
		Release: &agentservicepb.ConnectExecutorRequest_Release{Agent: agent},
	}})
}

func (e *testExecutor) delta(agent string, block *aipb.Block) {
	e.t.Helper()
	e.send(&agentservicepb.ConnectExecutorRequest{Content: &agentservicepb.ConnectExecutorRequest_Delta{
		Delta: &agentpb.Delta{Agent: agent, Block: block},
	}})
}

// close ends the stream cleanly, as a shutting down executor does, and returns how the service ended it.
func (e *testExecutor) close() error {
	e.t.Helper()
	require.NoError(e.t, e.client.CloseSend())
	return e.wait()
}

// breakStream drops the stream as a lost connection would.
func (e *testExecutor) breakStream() {
	e.cancel()
}

// wait returns how the service ended the stream, draining what it sent before.
func (e *testExecutor) wait() error {
	e.t.Helper()
	timeout := time.After(testTimeout)
	for {
		select {
		case err := <-e.done:
			return err
		case <-e.responses:
		case <-timeout:
			require.FailNow(e.t, "the stream did not end")
			return nil
		}
	}
}

// insertPendingTestAgent inserts a pending agent of a user, returning its name.
func insertPendingTestAgent(t *testing.T, userRn *agentpb.UserRn, agentID string, mutate func(agent *agentpb.Agent)) string {
	t.Helper()
	agentRn := userRn.AgentRn(agentID)
	insertTestAgent(t, agentRn, mutate)
	return agentRn.String()
}

// createTestAgent inserts a pending agent and places it.
func createTestAgent(t *testing.T, service *Service, userRn *agentpb.UserRn, agentID string, mutate func(agent *agentpb.Agent)) string {
	t.Helper()
	name := insertPendingTestAgent(t, userRn, agentID, mutate)
	require.NoError(t, service.placeAgent(context.Background(), name))
	return name
}

func getTestAgent(t *testing.T, service *Service, name string) *agentpb.Agent {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(name)
	require.NoError(t, err)
	agentModel, err := service.agentPostgresStore.GetAgent(context.Background(), agentRn.Organization, agentRn.User, agentRn.Agent)
	require.NoError(t, err)
	agent, err := agentModel.ToPb()
	require.NoError(t, err)
	return agent
}

func getTestExecutor(t *testing.T, service *Service, executorRn *agentpb.ExecutorRn) *agentpb.Executor {
	t.Helper()
	executorModel, err := service.agentPostgresStore.GetExecutor(context.Background(), executorRn.Organization, executorRn.User, executorRn.Executor)
	require.NoError(t, err)
	executor, err := executorModel.ToPb()
	require.NoError(t, err)
	return executor
}

// waitForAgent polls an agent until it satisfies condition, and returns it.
func waitForAgent(t *testing.T, service *Service, name string, condition func(agent *agentpb.Agent) bool) *agentpb.Agent {
	t.Helper()
	var agent *agentpb.Agent
	require.Eventually(t, func() bool {
		agent = getTestAgent(t, service, name)
		return condition(agent)
	}, testTimeout, 10*time.Millisecond, "agent %s never reached the expected state", name)
	return agent
}

func waitForAgentState(t *testing.T, service *Service, name string, state agentpb.Agent_State) *agentpb.Agent {
	t.Helper()
	return waitForAgent(t, service, name, func(agent *agentpb.Agent) bool { return agent.State == state })
}

// waitForExecutor polls an executor until it satisfies condition, and returns it.
func waitForExecutor(t *testing.T, service *Service, executorRn *agentpb.ExecutorRn, condition func(executor *agentpb.Executor) bool) *agentpb.Executor {
	t.Helper()
	var executor *agentpb.Executor
	require.Eventually(t, func() bool {
		executor = getTestExecutor(t, service, executorRn)
		return condition(executor)
	}, testTimeout, 10*time.Millisecond, "executor %s never reached the expected state", executorRn.String())
	return executor
}

func requireAgentCount(t *testing.T, service *Service, executorRn *agentpb.ExecutorRn, agentCount int32) {
	t.Helper()
	waitForExecutor(t, service, executorRn, func(executor *agentpb.Executor) bool { return executor.AgentCount == agentCount })
}

// waitForSessionCleared waits until an executor has no session: its stream's end was handled.
func waitForSessionCleared(t *testing.T, service *Service, executorRn *agentpb.ExecutorRn) {
	t.Helper()
	require.Eventually(t, func() bool {
		var session string
		err := service.transact(context.Background(), func(ctx context.Context, t *txn) error {
			var err error
			session, err = t.GetExecutorSession(ctx, executorRn.Organization, executorRn.User, executorRn.Executor)
			return err
		})
		return err == nil && session == ""
	}, testTimeout, 10*time.Millisecond)
}

// queueTestInput queues a user message on an agent.
func queueTestInput(t *testing.T, service *Service, agent, text string) *agentpb.Input {
	t.Helper()
	var input *agentpb.Input
	err := service.transact(context.Background(), func(ctx context.Context, t *txn) error {
		lockedAgent, err := t.getAgent(ctx, agent)
		if err != nil {
			return err
		}
		input, err = t.queueInput(ctx, lockedAgent, uuid.MustNewV7().String(), ai.NewUserMessage(ai.NewTextBlock(text)))
		return err
	})
	require.NoError(t, err)
	return input
}

func listTestEvents(t *testing.T, service *Service, name string) []*agentpb.Event {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(name)
	require.NoError(t, err)
	events, err := service.agentPostgresStore.ListEvents(context.Background(), agentRn.Organization, agentRn.User, agentRn.Agent, 0, 1000)
	require.NoError(t, err)
	return events
}

// eventKinds names the kinds of events, with the state or executor they carry, e.g. "state_changed:STATE_IDLE".
func eventKinds(events []*agentpb.Event) []string {
	kinds := make([]string, 0, len(events))
	for _, event := range events {
		kind := string(event.ProtoReflect().WhichOneof(event.ProtoReflect().Descriptor().Oneofs().ByName("kind")).Name())
		switch content := event.Kind.(type) {
		case *agentpb.Event_StateChanged_:
			kind += ":" + content.StateChanged.State.String()
		case *agentpb.Event_LeaseChanged_:
			kind += ":" + content.LeaseChanged.Executor
		case *agentpb.Event_TurnEnded_:
			kind += ":" + content.TurnEnded.Outcome.String()
		}
		kinds = append(kinds, kind)
	}
	return kinds
}

func newMessageCreatedEvent(message *aipb.Message, inputs ...string) *agentpb.Event {
	return &agentpb.Event{Kind: &agentpb.Event_MessageCreated_{MessageCreated: &agentpb.Event_MessageCreated{Message: message, Inputs: inputs}}}
}

func newTurnEndedEvent(outcome agentpb.Agent_TurnOutcome) *agentpb.Event {
	return &agentpb.Event{Kind: &agentpb.Event_TurnEnded_{TurnEnded: &agentpb.Event_TurnEnded{Outcome: outcome}}}
}

func TestConnectExecutor_FirstMessageMustBeRegister(t *testing.T) {
	service := newTestService(t)
	connectExecutor := grpcinproc.NewBidiStreamAsClient[
		agentservicepb.ConnectExecutorRequest,
		agentservicepb.ConnectExecutorResponse,
		agentservicepb.AgentService_ConnectExecutorServer,
	](service.ConnectExecutor)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	client, err := connectExecutor(ctx)
	require.NoError(t, err)
	heartbeat := &agentservicepb.ConnectExecutorRequest{Content: &agentservicepb.ConnectExecutorRequest_Heartbeat_{
		Heartbeat: &agentservicepb.ConnectExecutorRequest_Heartbeat{},
	}}
	require.NoError(t, client.Send(heartbeat))
	_, err = client.Recv()
	require.True(t, status.HasCode(err, codes.FailedPrecondition), "got %v", err)
}

func TestConnectExecutor_RegistersExecutor(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	executor := &agentpb.Executor{DisplayName: "laptop", MaxAgents: 3, Tools: []string{"exec_shell"}, Labels: map[string]string{"os": "linux"}}
	testExecutor := connectTestExecutor(t, service, userRn, "laptop", executor)
	require.Equal(t, testExecutor.name, testExecutor.registered.Name)
	require.Equal(t, agentpb.Executor_STATE_ONLINE, testExecutor.registered.State)
	require.Equal(t, int32(3), testExecutor.registered.MaxAgents)
	require.Equal(t, []string{"exec_shell"}, testExecutor.registered.Tools)
	require.Equal(t, map[string]string{"os": "linux"}, testExecutor.registered.Labels)
}

func TestPlacement_PinNeverFallsThrough(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	executorA := connectTestExecutor(t, service, userRn, "a", nil)
	executorB := connectTestExecutor(t, service, userRn, "b", nil)
	// Load b so least-loaded placement would pick a.
	createTestAgent(t, service, userRn, "filler", func(agent *agentpb.Agent) { agent.Executor = executorB.name })
	executorB.recvLease()

	pinned := createTestAgent(t, service, userRn, "pinned", func(agent *agentpb.Agent) {
		agent.Executor = executorB.name
		// A pin ignores the selector.
		agent.Spec.ExecutorSelector = map[string]string{"gpu": "true"}
	})
	require.Equal(t, pinned, executorB.recvLease().Name)
	require.Equal(t, executorB.name, getTestAgent(t, service, pinned).LeasedExecutor)

	offlinePinned := createTestAgent(t, service, userRn, "offline-pinned", func(agent *agentpb.Agent) {
		agent.Executor = userRn.ExecutorRn("offline").String()
	})
	require.Equal(t, agentpb.Agent_STATE_PENDING, getTestAgent(t, service, offlinePinned).State)
	executorA.requireNoResponse(200 * time.Millisecond)
	requireAgentCount(t, service, executorA.executorRn, 0)
	requireAgentCount(t, service, executorB.executorRn, 2)
}

func TestPlacement_ParentExecutorFirst(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	executorA := connectTestExecutor(t, service, userRn, "a", nil)
	executorB := connectTestExecutor(t, service, userRn, "b", &agentpb.Executor{MaxAgents: 3})
	parent := createTestAgent(t, service, userRn, "parent", func(agent *agentpb.Agent) { agent.Executor = executorB.name })
	executorB.recvLease()

	child := createTestAgent(t, service, userRn, "child", func(agent *agentpb.Agent) {
		agent.ParentAgent = parent
		agent.RootAgent = parent
	})
	require.Equal(t, child, executorB.recvLease().Name)
	executorA.requireNoResponse(200 * time.Millisecond)

	// A parent's executor without the tools falls through to the least loaded match.
	toolChild := createTestAgent(t, service, userRn, "tool-child", func(agent *agentpb.Agent) {
		agent.ParentAgent = parent
		agent.RootAgent = parent
		agent.Spec.Tools = []string{"browser"}
	})
	require.Equal(t, agentpb.Agent_STATE_PENDING, getTestAgent(t, service, toolChild).State)
	executorC := connectTestExecutor(t, service, userRn, "c", &agentpb.Executor{MaxAgents: 1, Tools: []string{"browser"}})
	require.Equal(t, toolChild, executorC.recvLease().Name)
}

func TestPlacement_SelectorToolsCapacityAndLeastLoaded(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	plain := connectTestExecutor(t, service, userRn, "plain", &agentpb.Executor{MaxAgents: 1})
	gpu := connectTestExecutor(t, service, userRn, "gpu", &agentpb.Executor{
		MaxAgents: 2, Labels: map[string]string{"gpu": "true", "os": "linux"}, Tools: []string{"exec_shell", "browser"},
	})

	selected := createTestAgent(t, service, userRn, "selected", func(agent *agentpb.Agent) {
		agent.Spec.ExecutorSelector = map[string]string{"gpu": "true"}
	})
	require.Equal(t, selected, gpu.recvLease().Name)

	tooled := createTestAgent(t, service, userRn, "tooled", func(agent *agentpb.Agent) { agent.Spec.Tools = []string{"browser"} })
	require.Equal(t, tooled, gpu.recvLease().Name)
	requireAgentCount(t, service, gpu.executorRn, 2)

	// gpu is full: an agent needing it waits.
	waiting := createTestAgent(t, service, userRn, "waiting", func(agent *agentpb.Agent) { agent.Spec.Tools = []string{"exec_shell"} })
	require.Equal(t, agentpb.Agent_STATE_PENDING, getTestAgent(t, service, waiting).State)

	// Least loaded: plain (0 agents) over gpu (full anyway), then nobody.
	anywhere := createTestAgent(t, service, userRn, "anywhere", nil)
	require.Equal(t, anywhere, plain.recvLease().Name)
	nowhere := createTestAgent(t, service, userRn, "nowhere", nil)
	require.Equal(t, agentpb.Agent_STATE_PENDING, getTestAgent(t, service, nowhere).State)
	requireAgentCount(t, service, plain.executorRn, 1)
}

func TestPlacement_LeastLoadedFirst(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	executorA := connectTestExecutor(t, service, userRn, "a", &agentpb.Executor{MaxAgents: 5})
	executorB := connectTestExecutor(t, service, userRn, "b", &agentpb.Executor{MaxAgents: 5})
	// Ties break by executor id: a, then b, then a again.
	for i, expected := range []*testExecutor{executorA, executorB, executorA, executorB} {
		agent := createTestAgent(t, service, userRn, fmt.Sprintf("agent-%d", i), nil)
		require.Equal(t, agent, expected.recvLease().Name)
	}
	requireAgentCount(t, service, executorA.executorRn, 2)
	requireAgentCount(t, service, executorB.executorRn, 2)
}

func TestPlacement_NeverAnotherUsersExecutor(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	organizationRn, err := agentpb.ParseOrganizationRn(userRn.OrganizationRn().String())
	require.NoError(t, err)
	otherUserRn := organizationRn.UserRn("other")
	otherExecutor := connectTestExecutor(t, service, otherUserRn, "theirs", nil)

	agent := createTestAgent(t, service, userRn, "mine", nil)
	pinned := createTestAgent(t, service, userRn, "pinned", func(agent *agentpb.Agent) { agent.Executor = otherExecutor.name })
	otherExecutor.requireNoResponse(300 * time.Millisecond)
	require.Equal(t, agentpb.Agent_STATE_PENDING, getTestAgent(t, service, agent).State)
	require.Equal(t, agentpb.Agent_STATE_PENDING, getTestAgent(t, service, pinned).State)

	// The user's own executor gets the unpinned one when it connects.
	ownExecutor := connectTestExecutor(t, service, userRn, "mine", nil)
	require.Equal(t, agent, ownExecutor.recvLease().Name)
	ownExecutor.requireNoResponse(200 * time.Millisecond)
}

func TestConnectExecutor_LeaseThenPendingInputsOnConnect(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	agent := insertPendingTestAgent(t, userRn, "agent", nil)
	first := queueTestInput(t, service, agent, "first")
	second := queueTestInput(t, service, agent, "second")

	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	lease := testExecutor.recvLease()
	require.Equal(t, agent, lease.Name)
	require.Equal(t, agentpb.Agent_STATE_IDLE, lease.State)
	require.Equal(t, testExecutor.name, lease.LeasedExecutor)
	require.Equal(t, first.Name, testExecutor.recvInput().Name)
	require.Equal(t, second.Name, testExecutor.recvInput().Name)
	testExecutor.requireNoResponse(2 * testHeartbeatInterval)

	stored := getTestAgent(t, service, agent)
	require.Equal(t, agentpb.Agent_STATE_IDLE, stored.State)
	require.Equal(t, []string{
		"input_queued", "input_queued", "lease_changed:" + testExecutor.name, "state_changed:STATE_IDLE",
	}, eventKinds(listTestEvents(t, service, agent)))
	requireAgentCount(t, service, testExecutor.executorRn, 1)
}

func TestConnectExecutor_ForwardsInputsLive(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgent(t, service, userRn, "agent", nil)
	testExecutor.recvLease()

	input := queueTestInput(t, service, agent, "hello")
	received := testExecutor.recvInput()
	require.Equal(t, input.Name, received.Name)
	require.True(t, proto.Equal(input.Message, received.Message))
	// Heartbeat reconciles never resend it.
	testExecutor.requireNoResponse(3 * testHeartbeatInterval)
}

func TestConnectExecutor_TurnLifecycle(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgent(t, service, userRn, "agent", nil)
	testExecutor.recvLease()
	input := queueTestInput(t, service, agent, "do it")
	testExecutor.recvInput()

	testExecutor.report(agent, newTurnStartedEvent())
	waitForAgentState(t, service, agent, agentpb.Agent_STATE_RUNNING)

	userMessage := ai.NewUserMessage(ai.NewTextBlock("do it"))
	userMessage.Name = "organizations/o/users/u/chats/c/messages/1"
	testExecutor.report(agent, newMessageCreatedEvent(userMessage, input.Name))
	assistantMessage := ai.NewAssistantMessage(ai.NewTextBlock("done"))
	assistantMessage.Name = "organizations/o/users/u/chats/c/messages/2"
	testExecutor.report(agent, newMessageCreatedEvent(assistantMessage))
	testExecutor.report(agent, newTurnEndedEvent(agentpb.Agent_TURN_OUTCOME_SUCCEEDED))

	stored := waitForAgentState(t, service, agent, agentpb.Agent_STATE_IDLE)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_SUCCEEDED, stored.LastTurnOutcome)
	require.Nil(t, stored.Status)
	events := listTestEvents(t, service, agent)
	require.Equal(t, []string{
		"lease_changed:" + testExecutor.name, "state_changed:STATE_IDLE", "input_queued",
		"turn_started", "state_changed:STATE_RUNNING", "message_created", "message_created",
		"turn_ended:TURN_OUTCOME_SUCCEEDED", "state_changed:STATE_IDLE",
	}, eventKinds(events))
	require.True(t, proto.Equal(assistantMessage, events[6].GetMessageCreated().Message))
	require.Equal(t, stored.LastEventSequence, events[len(events)-1].Sequence)

	agentRn, err := agentpb.ParseAgentRn(agent)
	require.NoError(t, err)
	pendingInputs, err := service.agentPostgresStore.ListPendingInputsOfExecutor(context.Background(), testExecutor.name)
	require.NoError(t, err)
	require.Empty(t, pendingInputs, "agent %s's input was not consumed", agentRn.Agent)
}

func TestConnectExecutor_RejectsAnotherAgentsInput(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgent(t, service, userRn, "agent", nil)
	other := createTestAgent(t, service, userRn, "other", nil)
	testExecutor.recvLease()
	testExecutor.recvLease()
	otherInput := queueTestInput(t, service, other, "not yours")

	testExecutor.report(agent, newMessageCreatedEvent(ai.NewUserMessage(ai.NewTextBlock("x")), otherInput.Name))
	require.True(t, status.HasCode(testExecutor.wait(), codes.InvalidArgument))
}

func TestConnectExecutor_SubAgentTurnReportedToParent(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	parent := createTestAgent(t, service, userRn, "parent", nil)
	testExecutor.recvLease()
	child := createTestAgent(t, service, userRn, "child", func(agent *agentpb.Agent) {
		agent.ParentAgent = parent
		agent.RootAgent = parent
	})
	testExecutor.recvLease()

	testExecutor.report(child, newTurnStartedEvent())
	firstAnswer := ai.NewAssistantMessage(ai.NewTextBlock("not the last"))
	testExecutor.report(child, newMessageCreatedEvent(firstAnswer))
	finalAnswer := ai.NewAssistantMessage(
		ai.NewThoughtBlock("thinking"),
		ai.NewTextBlock("the answer"),
		ai.NewToolCallBlock(&aipb.ToolCall{Id: "call", Name: "exec_shell", Arguments: &structpb.Struct{}}),
		ai.NewTextBlock("is 42"),
	)
	testExecutor.report(child, newMessageCreatedEvent(finalAnswer))
	testExecutor.report(child, newMessageCreatedEvent(ai.NewToolMessage(ai.NewToolResultBlock(ai.NewToolResult("exec_shell", "call", "tool output")))))
	testExecutor.report(child, newTurnEndedEvent(agentpb.Agent_TURN_OUTCOME_SUCCEEDED))

	// Forwarded to the parent's executor.
	input := testExecutor.recvInput()
	inputRn, err := agentpb.ParseInputRn(input.Name)
	require.NoError(t, err)
	require.Equal(t, parent, inputRn.AgentRn().String())
	require.Equal(t, aipb.Role_ROLE_USER, input.Message.Role)
	sender, ok := agentpb.Annotations.Sender.Get(input.Message)
	require.True(t, ok)
	require.Equal(t, child, sender)
	require.Len(t, input.Message.Blocks, 2)
	require.Equal(t, "the answer", input.Message.Blocks[0].GetText())
	require.Equal(t, "is 42", input.Message.Blocks[1].GetText())

	parentEvents := listTestEvents(t, service, parent)
	require.Equal(t, input.Name, parentEvents[len(parentEvents)-1].GetInputQueued().Input)
}

func TestConnectExecutor_DropsReportsOfNonHolders(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	holder := connectTestExecutor(t, service, userRn, "holder", nil)
	agent := createTestAgent(t, service, userRn, "agent", nil)
	holder.recvLease()
	stranger := connectTestExecutor(t, service, userRn, "stranger", nil)

	stranger.report(agent, newTurnStartedEvent())
	stranger.release(agent)
	stranger.delta(agent, ai.NewTextBlock("x"))
	// The stranger stays connected: its heartbeats keep going through and it gets nothing.
	stranger.requireNoResponse(3 * testHeartbeatInterval)
	stored := getTestAgent(t, service, agent)
	require.Equal(t, agentpb.Agent_STATE_IDLE, stored.State)
	require.Equal(t, holder.name, stored.LeasedExecutor)
}

func TestConnectExecutor_SupersededSessionAborted(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	first := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgent(t, service, userRn, "agent", nil)
	first.recvLease()

	second := connectTestExecutor(t, service, userRn, "executor", nil)
	require.Equal(t, agent, second.recvLease().Name)
	// The first session's next heartbeat finds it replaced.
	require.True(t, status.HasCode(first.wait(), codes.Aborted))
	// Its end left the new session alone.
	second.requireNoResponse(3 * testHeartbeatInterval)
	executor := getTestExecutor(t, service, second.executorRn)
	require.Equal(t, agentpb.Executor_STATE_ONLINE, executor.State)
	require.Equal(t, second.name, getTestAgent(t, service, agent).LeasedExecutor)
}

func TestConnectExecutor_SupersededReportAborted(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	first := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgent(t, service, userRn, "agent", nil)
	first.recvLease()
	second := connectTestExecutor(t, service, userRn, "executor", nil)
	second.recvLease()

	first.report(agent, newTurnStartedEvent())
	require.True(t, status.HasCode(first.wait(), codes.Aborted))
	require.Equal(t, agentpb.Agent_STATE_IDLE, getTestAgent(t, service, agent).State)
}

func TestConnectExecutor_CleanCloseReleasesPromptly(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	parentExecutor := connectTestExecutor(t, service, userRn, "parent-executor", nil)
	parent := createTestAgent(t, service, userRn, "parent", func(agent *agentpb.Agent) { agent.Executor = parentExecutor.name })
	parentExecutor.recvLease()
	childExecutor := connectTestExecutor(t, service, userRn, "child-executor", nil)
	child := createTestAgent(t, service, userRn, "child", func(agent *agentpb.Agent) {
		agent.ParentAgent = parent
		agent.RootAgent = parent
		agent.Executor = childExecutor.name
	})
	childExecutor.recvLease()
	idle := createTestAgent(t, service, userRn, "idle", func(agent *agentpb.Agent) { agent.Executor = childExecutor.name })
	childExecutor.recvLease()
	childExecutor.report(child, newTurnStartedEvent())
	waitForAgentState(t, service, child, agentpb.Agent_STATE_RUNNING)
	requireAgentCount(t, service, childExecutor.executorRn, 2)

	start := time.Now()
	require.NoError(t, childExecutor.close())
	require.Less(t, time.Since(start), time.Second)

	// Released at once, not at the heartbeat deadline.
	stored := getTestAgent(t, service, child)
	require.Equal(t, agentpb.Agent_STATE_PENDING, stored.State)
	require.Empty(t, stored.LeasedExecutor)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_FAILED, stored.LastTurnOutcome)
	require.Equal(t, int32(codes.Unavailable), stored.Status.Code)
	require.Equal(t, agentpb.Agent_STATE_PENDING, getTestAgent(t, service, idle).State)
	executor := getTestExecutor(t, service, childExecutor.executorRn)
	require.Equal(t, agentpb.Executor_STATE_OFFLINE, executor.State)
	require.Equal(t, int32(0), executor.AgentCount)

	// The lost turn is reported to the parent.
	report := parentExecutor.recvInput()
	sender, _ := agentpb.Annotations.Sender.Get(report.Message)
	require.Equal(t, child, sender)
	require.Equal(t, "Turn failed: executor lost", report.Message.Blocks[0].GetText())
}

func TestConnectExecutor_CleanCloseAfterCancelledTurns(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgent(t, service, userRn, "agent", nil)
	testExecutor.recvLease()
	testExecutor.report(agent, newTurnStartedEvent())
	// A shutting down executor cancels its turns, then closes: both are handled, in order.
	testExecutor.report(agent, newTurnEndedEvent(agentpb.Agent_TURN_OUTCOME_CANCELLED))
	require.NoError(t, testExecutor.close())

	stored := getTestAgent(t, service, agent)
	require.Equal(t, agentpb.Agent_STATE_PENDING, stored.State)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_CANCELLED, stored.LastTurnOutcome)
	require.Equal(t, []string{
		"lease_changed:" + testExecutor.name, "state_changed:STATE_IDLE", "turn_started", "state_changed:STATE_RUNNING",
		"turn_ended:TURN_OUTCOME_CANCELLED", "state_changed:STATE_IDLE", "lease_changed:", "state_changed:STATE_PENDING",
	}, eventKinds(listTestEvents(t, service, agent)))
}

func TestConnectExecutor_BrokenStreamKeepsLeasesForReconnect(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	first := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgent(t, service, userRn, "agent", nil)
	first.recvLease()
	first.report(agent, newTurnStartedEvent())
	waitForAgentState(t, service, agent, agentpb.Agent_STATE_RUNNING)

	first.breakStream()
	waitForSessionCleared(t, service, first.executorRn)
	stored := getTestAgent(t, service, agent)
	require.Equal(t, agentpb.Agent_STATE_RUNNING, stored.State)
	require.Equal(t, first.name, stored.LeasedExecutor)
	require.Equal(t, agentpb.Executor_STATE_ONLINE, getTestExecutor(t, service, first.executorRn).State)
	// Placement never targets the session-less executor.
	other := createTestAgent(t, service, userRn, "other", nil)
	require.Equal(t, agentpb.Agent_STATE_PENDING, getTestAgent(t, service, other).State)

	second := connectTestExecutor(t, service, userRn, "executor", nil)
	leasedAgentSet := map[string]struct{}{}
	for range 2 {
		lease := second.recvLease()
		require.Equal(t, agentpb.Agent_STATE_IDLE, lease.State)
		leasedAgentSet[lease.Name] = struct{}{}
	}
	require.Equal(t, map[string]struct{}{agent: {}, other: {}}, leasedAgentSet)

	stored = getTestAgent(t, service, agent)
	require.Equal(t, agentpb.Agent_STATE_IDLE, stored.State)
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_FAILED, stored.LastTurnOutcome)
	require.Equal(t, int32(codes.Unavailable), stored.Status.Code)
	// Re-adopted without going through pending.
	require.Equal(t, []string{
		"lease_changed:" + first.name, "state_changed:STATE_IDLE", "turn_started", "state_changed:STATE_RUNNING",
		"turn_ended:TURN_OUTCOME_FAILED", "state_changed:STATE_IDLE",
	}, eventKinds(listTestEvents(t, service, agent)))
	requireAgentCount(t, service, second.executorRn, 2)
}

func TestSweeper_HeartbeatDeadlineReleases(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgent(t, service, userRn, "agent", nil)
	testExecutor.recvLease()
	testExecutor.breakStream()
	waitForSessionCleared(t, service, testExecutor.executorRn)

	stored := waitForAgentState(t, service, agent, agentpb.Agent_STATE_PENDING)
	require.Empty(t, stored.LeasedExecutor)
	executor := getTestExecutor(t, service, testExecutor.executorRn)
	require.Equal(t, agentpb.Executor_STATE_OFFLINE, executor.State)
	require.Equal(t, int32(0), executor.AgentCount)
	require.GreaterOrEqual(t, time.Since(executor.HeartbeatTime.AsTime()), newTestOpts().HeartbeatTimeout)
}

func TestConnectExecutor_ReleaseRefusalAndPlacementElsewhere(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	executorA := connectTestExecutor(t, service, userRn, "a", &agentpb.Executor{MaxAgents: 1})
	agent := createTestAgent(t, service, userRn, "agent", nil)
	executorA.recvLease()
	waiting := createTestAgent(t, service, userRn, "waiting", nil)
	require.Equal(t, agentpb.Agent_STATE_PENDING, getTestAgent(t, service, waiting).State)

	// The released agent is refused here, so the freed capacity goes to the waiting one.
	executorA.release(agent)
	require.Equal(t, waiting, executorA.recvLease().Name)
	require.Equal(t, agentpb.Agent_STATE_PENDING, getTestAgent(t, service, agent).State)
	requireAgentCount(t, service, executorA.executorRn, 1)

	executorB := connectTestExecutor(t, service, userRn, "b", &agentpb.Executor{MaxAgents: 1})
	require.Equal(t, agent, executorB.recvLease().Name)
	requireAgentCount(t, service, executorB.executorRn, 1)
}

func TestConnectExecutor_RefusalLapses(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	agent := createTestAgent(t, service, userRn, "agent", nil)
	testExecutor.recvLease()

	released := time.Now()
	testExecutor.release(agent)
	waitForAgentState(t, service, agent, agentpb.Agent_STATE_PENDING)
	requireAgentCount(t, service, testExecutor.executorRn, 0)
	// No tight loop: the sweeper offers it again only once the refusal lapsed.
	require.Equal(t, agent, testExecutor.recvLease().Name)
	require.GreaterOrEqual(t, time.Since(released), newTestOpts().RefusalCooldown)
}

func TestConnectExecutor_DeltasOfLeasedAgentsOnly(t *testing.T) {
	service := newTestService(t)
	userRn := newTestUser(t)
	testExecutor := connectTestExecutor(t, service, userRn, "executor", nil)
	leased := createTestAgent(t, service, userRn, "leased", nil)
	testExecutor.recvLease()
	unleased := insertPendingTestAgent(t, userRn, "unleased", func(agent *agentpb.Agent) {
		agent.Executor = userRn.ExecutorRn("elsewhere").String()
	})
	leasedSubscription := service.broker.subscribeTree(leased, true)
	defer leasedSubscription.Close()
	unleasedSubscription := service.broker.subscribeTree(unleased, true)
	defer unleasedSubscription.Close()

	testExecutor.delta(unleased, ai.NewTextBlock("dropped"))
	testExecutor.delta(leased, ai.NewTextBlock("fanned out"))
	select {
	case response := <-leasedSubscription.C:
		require.Equal(t, "fanned out", response.GetDelta().Block.GetText())
	case <-time.After(testTimeout):
		require.FailNow(t, "the leased agent's delta was not published")
	}
	// Deltas are handled in order: the dropped one would have been published first.
	require.Empty(t, drain(unleasedSubscription))
}

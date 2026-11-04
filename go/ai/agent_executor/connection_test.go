package agent_executor

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	pb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	connectionTestUser       = "organizations/o/users/u"
	connectionTestExecutorID = "laptop"
	// connectionTestTimeout bounds every wait on the executor.
	connectionTestTimeout = 5 * time.Second
)

// fakeExecutorStream is the service's end of a ConnectExecutor stream.
type fakeExecutorStream struct {
	grpc.ClientStream
	ctx        context.Context
	requests   chan *pb.ConnectExecutorRequest
	responses  chan *pb.ConnectExecutorResponse
	recvErrors chan error
	// sendClosed is closed once the executor half-closes the stream, which
	// the fake service answers by ending it.
	sendClosed    chan struct{}
	closeSendOnce sync.Once
}

func (s *fakeExecutorStream) Send(request *pb.ConnectExecutorRequest) error {
	select {
	case s.requests <- request:
		return nil
	case <-s.ctx.Done():
		return io.EOF
	}
}

func (s *fakeExecutorStream) Recv() (*pb.ConnectExecutorResponse, error) {
	select {
	case response := <-s.responses:
		return response, nil
	case err := <-s.recvErrors:
		return nil, err
	case <-s.sendClosed:
		return nil, io.EOF
	case <-s.ctx.Done():
		return nil, status.FromError(s.ctx.Err(), "receiving").Err()
	}
}

func (s *fakeExecutorStream) CloseSend() error {
	s.closeSendOnce.Do(func() { close(s.sendClosed) })
	return nil
}

func (s *fakeExecutorStream) Context() context.Context { return s.ctx }

// respond sends a message down to the executor.
func (s *fakeExecutorStream) respond(t *testing.T, connectExecutorResponse *pb.ConnectExecutorResponse) {
	t.Helper()
	select {
	case s.responses <- connectExecutorResponse:
	case <-time.After(connectionTestTimeout):
		t.Fatalf("executor did not receive %v", connectExecutorResponse)
	}
}

// await returns the next request matching match, skipping the others.
func (s *fakeExecutorStream) await(t *testing.T, match func(*pb.ConnectExecutorRequest) bool) *pb.ConnectExecutorRequest {
	t.Helper()
	timeout := time.After(connectionTestTimeout)
	for {
		select {
		case request := <-s.requests:
			if match(request) {
				return request
			}
		case <-timeout:
			t.Fatal("timed out awaiting a request")
		}
	}
}

func (s *fakeExecutorStream) lease(t *testing.T, leasedAgent *agentpb.Agent) {
	t.Helper()
	s.respond(t, &pb.ConnectExecutorResponse{Content: &pb.ConnectExecutorResponse_Lease{Lease: leasedAgent}})
}

func (s *fakeExecutorStream) input(t *testing.T, agentName, inputID string) {
	t.Helper()
	agentRn, err := agentpb.ParseAgentRn(agentName)
	require.NoError(t, err)
	input := &agentpb.Input{
		Name:    agentRn.InputRn(inputID).String(),
		Message: ai.NewUserMessage(ai.NewTextBlock("hello")),
	}
	s.respond(t, &pb.ConnectExecutorResponse{Content: &pb.ConnectExecutorResponse_Input{Input: input}})
}

func (s *fakeExecutorStream) awaitRelease(t *testing.T) string {
	t.Helper()
	request := s.await(t, func(request *pb.ConnectExecutorRequest) bool { return request.GetRelease() != nil })
	return request.GetRelease().GetAgent()
}

// fakeExecutorAgentServiceClient hands each ConnectExecutor stream to the test.
type fakeExecutorAgentServiceClient struct {
	pb.AgentServiceClient
	streams chan *fakeExecutorStream
}

func (c *fakeExecutorAgentServiceClient) ConnectExecutor(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[pb.ConnectExecutorRequest, pb.ConnectExecutorResponse], error) {
	stream := &fakeExecutorStream{
		ctx:        ctx,
		requests:   make(chan *pb.ConnectExecutorRequest, 64),
		responses:  make(chan *pb.ConnectExecutorResponse),
		recvErrors: make(chan error, 1),
		sendClosed: make(chan struct{}),
	}
	select {
	case c.streams <- stream:
		return stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// blockingAiServiceClient persists messages and blocks every generation until
// its turn is cancelled, then until windDown closes: a turn's progress is
// observable without depending on its output.
type blockingAiServiceClient struct {
	aiservicepb.AiServiceClient
	// generating and cancelled receive the chat of each generation started and cancelled.
	generating chan string
	cancelled  chan string
	windDown   chan struct{}

	mutex                    sync.Mutex
	messageCount             int
	chatToGenerationCount    map[string]int
	maxConcurrentGenerations int
}

func newBlockingAiServiceClient() *blockingAiServiceClient {
	windDown := make(chan struct{})
	close(windDown)
	return &blockingAiServiceClient{
		generating:            make(chan string, 16),
		cancelled:             make(chan string, 16),
		windDown:              windDown,
		chatToGenerationCount: map[string]int{},
	}
}

func (c *blockingAiServiceClient) CreateMessage(ctx context.Context, request *aiservicepb.CreateMessageRequest, _ ...grpc.CallOption) (*aipb.Message, error) {
	chatRn, err := aipb.ParseChatRn(request.GetParent())
	if err != nil {
		return nil, err
	}
	c.mutex.Lock()
	c.messageCount++
	messageID := request.GetMessageId()
	if messageID == "" {
		messageID = fmt.Sprintf("m%d", c.messageCount)
	}
	c.mutex.Unlock()
	message := proto.Clone(request.GetMessage()).(*aipb.Message)
	message.Name = chatRn.MessageRn(messageID).String()
	return message, nil
}

func (c *blockingAiServiceClient) StreamGenerateMessage(ctx context.Context, request *aiservicepb.GenerateMessageRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[aiservicepb.StreamGenerateMessageResponse], error) {
	chat := request.GetParent()
	c.mutex.Lock()
	c.chatToGenerationCount[chat]++
	c.maxConcurrentGenerations = max(c.maxConcurrentGenerations, c.chatToGenerationCount[chat])
	c.mutex.Unlock()
	defer func() {
		c.mutex.Lock()
		c.chatToGenerationCount[chat]--
		c.mutex.Unlock()
	}()
	c.generating <- chat
	<-ctx.Done()
	c.cancelled <- chat
	<-c.windDown
	return nil, status.FromError(ctx.Err(), "generating").Err()
}

// maxConcurrentGenerationsPerChat is the most generations ever running at once in one chat.
func (c *blockingAiServiceClient) maxConcurrentGenerationsPerChat() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.maxConcurrentGenerations
}

func awaitChat(t *testing.T, chats <-chan string) string {
	t.Helper()
	select {
	case chat := <-chats:
		return chat
	case <-time.After(connectionTestTimeout):
		t.Fatal("timed out awaiting a generation")
		return ""
	}
}

func requireClosed(t *testing.T, channel <-chan struct{}) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(connectionTestTimeout):
		t.Fatal("channel not closed")
	}
}

// connectionHarness runs an executor against the fakes.
type connectionHarness struct {
	executor           *Executor
	agentServiceClient *fakeExecutorAgentServiceClient
	aiServiceClient    *blockingAiServiceClient
	cancel             context.CancelFunc
	runError           chan error
}

func startExecutor(t *testing.T, configure func(*Opts), tools ...Tool) *connectionHarness {
	t.Helper()
	opts := &Opts{
		User:                     connectionTestUser,
		ExecutorID:               connectionTestExecutorID,
		DisplayName:              "Laptop",
		Labels:                   map[string]string{"os": "linux"},
		MaxAgents:                4,
		HeartbeatInterval:        time.Hour,
		ReconnectInitialInterval: time.Millisecond,
		ReconnectMaxInterval:     10 * time.Millisecond,
		ShutdownTimeout:          connectionTestTimeout,
	}
	if configure != nil {
		configure(opts)
	}
	h := &connectionHarness{
		agentServiceClient: &fakeExecutorAgentServiceClient{streams: make(chan *fakeExecutorStream)},
		aiServiceClient:    newBlockingAiServiceClient(),
		runError:           make(chan error, 1),
	}
	executor, err := New(opts, h.agentServiceClient, h.aiServiceClient, tools...)
	require.NoError(t, err)
	h.executor = executor
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.runError <- executor.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-h.runError
	})
	return h
}

// stop shuts the executor down and returns what Run returned.
func (h *connectionHarness) stop(t *testing.T) error {
	t.Helper()
	h.cancel()
	select {
	case err := <-h.runError:
		h.runError <- err
		return err
	case <-time.After(connectionTestTimeout):
		t.Fatal("Run did not return")
		return nil
	}
}

// awaitStream returns the executor's next stream, before registration.
func (h *connectionHarness) awaitStream(t *testing.T) *fakeExecutorStream {
	t.Helper()
	select {
	case stream := <-h.agentServiceClient.streams:
		return stream
	case <-time.After(connectionTestTimeout):
		t.Fatal("executor did not connect")
		return nil
	}
}

// accept returns the executor's next stream, registered, and its registration.
func (h *connectionHarness) accept(t *testing.T) (*fakeExecutorStream, *pb.ConnectExecutorRequest_Register) {
	t.Helper()
	stream := h.awaitStream(t)
	register := stream.await(t, func(*pb.ConnectExecutorRequest) bool { return true }).GetRegister()
	require.NotNil(t, register, "the first message must register")
	userRn, err := agentpb.ParseUserRn(register.GetParent())
	require.NoError(t, err)
	registered := proto.Clone(register.GetExecutor()).(*agentpb.Executor)
	registered.Name = userRn.ExecutorRn(register.GetExecutorId()).String()
	stream.respond(t, &pb.ConnectExecutorResponse{Content: &pb.ConnectExecutorResponse_Registered{Registered: registered}})
	return stream, register
}

// actor returns the live actor of an agent, if any.
func (h *connectionHarness) actor(agentName string) *agent {
	h.executor.mutex.Lock()
	defer h.executor.mutex.Unlock()
	return h.executor.agentNameToAgent[agentName]
}

func (h *connectionHarness) awaitActor(t *testing.T, agentName string) *agent {
	t.Helper()
	require.Eventually(t, func() bool { return h.actor(agentName) != nil }, connectionTestTimeout, time.Millisecond)
	return h.actor(agentName)
}

func newLeasedAgent(t *testing.T, agentID string, toolNames ...string) *agentpb.Agent {
	t.Helper()
	userRn, err := agentpb.ParseUserRn(connectionTestUser)
	require.NoError(t, err)
	chatRn, err := aipb.NewChatRn(connectionTestUser, agentID)
	require.NoError(t, err)
	return &agentpb.Agent{
		Name: userRn.AgentRn(agentID).String(),
		Chat: chatRn.String(),
		Spec: &agentpb.AgentSpec{Model: "providers/p/models/m", Tools: toolNames},
	}
}

func TestNewRejectsInvalidUser(t *testing.T) {
	_, err := New(&Opts{User: "organizations/o"}, &fakeExecutorAgentServiceClient{}, newBlockingAiServiceClient())
	require.Error(t, err)
}

func TestRunRegisters(t *testing.T) {
	h := startExecutor(t, nil, &fakeTool{name: "shell"})
	_, register := h.accept(t)
	require.Equal(t, connectionTestUser, register.GetParent())
	require.Equal(t, connectionTestExecutorID, register.GetExecutorId())
	require.Equal(t, "Laptop", register.GetExecutor().GetDisplayName())
	require.Equal(t, map[string]string{"os": "linux"}, register.GetExecutor().GetLabels())
	require.EqualValues(t, 4, register.GetExecutor().GetMaxAgents())
	require.Equal(t, []string{"create_agents", "list_agents", "send_input", "shell", "update_labels"}, register.GetExecutor().GetTools())
}

func TestRunHeartbeats(t *testing.T) {
	h := startExecutor(t, func(opts *Opts) {
		opts.HeartbeatInterval = time.Millisecond
		// Unread heartbeats fill the stream, stalling the shutdown's half-close.
		opts.ShutdownTimeout = 10 * time.Millisecond
	})
	stream, _ := h.accept(t)
	for range 3 {
		stream.await(t, func(request *pb.ConnectExecutorRequest) bool { return request.GetHeartbeat() != nil })
	}
}

func TestRunRoutesInputsToLeasedAgents(t *testing.T) {
	h := startExecutor(t, nil)
	stream, _ := h.accept(t)
	leasedAgent := newLeasedAgent(t, "a")
	otherAgent := newLeasedAgent(t, "other")
	stream.lease(t, leasedAgent)
	actor := h.awaitActor(t, leasedAgent.GetName())

	// An input for an agent not held is ignored.
	stream.input(t, otherAgent.GetName(), "i0")
	stream.input(t, leasedAgent.GetName(), "i1")
	require.Equal(t, leasedAgent.GetChat(), awaitChat(t, h.aiServiceClient.generating))
	actor.mutex.Lock()
	defer actor.mutex.Unlock()
	require.Len(t, actor.inputNameSet, 1)
	require.Nil(t, h.actor(otherAgent.GetName()))
}

func TestRunIgnoresLeaseOfHeldAgent(t *testing.T) {
	h := startExecutor(t, func(opts *Opts) { opts.MaxAgents = 1 })
	stream, _ := h.accept(t)
	leasedAgent := newLeasedAgent(t, "a")
	stream.lease(t, leasedAgent)
	actor := h.awaitActor(t, leasedAgent.GetName())
	stream.lease(t, leasedAgent)
	// Served in order: once b is released, a's second lease was handled.
	stream.lease(t, newLeasedAgent(t, "b"))
	require.Equal(t, newLeasedAgent(t, "b").GetName(), stream.awaitRelease(t))
	require.Same(t, actor, h.actor(leasedAgent.GetName()))
}

func TestRunCancelsTurn(t *testing.T) {
	h := startExecutor(t, nil)
	stream, _ := h.accept(t)
	leasedAgent := newLeasedAgent(t, "a")
	stream.lease(t, leasedAgent)
	actor := h.awaitActor(t, leasedAgent.GetName())
	stream.input(t, leasedAgent.GetName(), "i1")
	awaitChat(t, h.aiServiceClient.generating)

	cancel := &pb.ConnectExecutorResponse_Cancel{Agent: leasedAgent.GetName()}
	stream.respond(t, &pb.ConnectExecutorResponse{Content: &pb.ConnectExecutorResponse_Cancel_{Cancel: cancel}})
	require.Equal(t, leasedAgent.GetChat(), awaitChat(t, h.aiServiceClient.cancelled))
	// Cancelling ends the turn, not the lease.
	require.NoError(t, actor.ctx.Err())
}

func TestRunRevokesAgent(t *testing.T) {
	h := startExecutor(t, nil)
	stream, _ := h.accept(t)
	leasedAgent := newLeasedAgent(t, "a")
	stream.lease(t, leasedAgent)
	actor := h.awaitActor(t, leasedAgent.GetName())

	revoke := &pb.ConnectExecutorResponse_Revoke{Agent: leasedAgent.GetName()}
	stream.respond(t, &pb.ConnectExecutorResponse{Content: &pb.ConnectExecutorResponse_Revoke_{Revoke: revoke}})
	requireClosed(t, actor.done)
	require.Eventually(t, func() bool { return h.actor(leasedAgent.GetName()) == nil }, connectionTestTimeout, time.Millisecond)
}

func TestRunReleasesLeaseNeedingMissingTools(t *testing.T) {
	h := startExecutor(t, nil, &fakeTool{name: "shell"})
	stream, _ := h.accept(t)
	leasedAgent := newLeasedAgent(t, "a", "shell", "browser")
	stream.lease(t, leasedAgent)
	require.Equal(t, leasedAgent.GetName(), stream.awaitRelease(t))
	require.Nil(t, h.actor(leasedAgent.GetName()))
}

func TestRunReleasesLeaseBeyondMaxAgents(t *testing.T) {
	h := startExecutor(t, func(opts *Opts) { opts.MaxAgents = 1 })
	stream, _ := h.accept(t)
	firstAgent := newLeasedAgent(t, "a")
	secondAgent := newLeasedAgent(t, "b")
	stream.lease(t, firstAgent)
	stream.lease(t, secondAgent)
	require.Equal(t, secondAgent.GetName(), stream.awaitRelease(t))
	require.NotNil(t, h.actor(firstAgent.GetName()))
	require.Nil(t, h.actor(secondAgent.GetName()))
}

func TestRunReconnectsAfterStreamError(t *testing.T) {
	h := startExecutor(t, nil)
	stream, _ := h.accept(t)
	leasedAgent := newLeasedAgent(t, "a")
	stream.lease(t, leasedAgent)
	actor := h.awaitActor(t, leasedAgent.GetName())

	stream.recvErrors <- status.Errorf(codes.Unavailable, "service restarting").Err()
	_, register := h.accept(t)
	require.Equal(t, connectionTestExecutorID, register.GetExecutorId())
	require.Equal(t, connectionTestUser, register.GetParent())
	// The lost connection's leases ended before reconnecting.
	requireClosed(t, actor.done)
	require.Nil(t, h.actor(leasedAgent.GetName()))
}

func TestRunReconnectsWhenNotRegistered(t *testing.T) {
	h := startExecutor(t, nil)
	stream := h.awaitStream(t)
	stream.await(t, func(request *pb.ConnectExecutorRequest) bool { return request.GetRegister() != nil })
	stream.lease(t, newLeasedAgent(t, "a"))
	_, register := h.accept(t)
	require.Equal(t, connectionTestExecutorID, register.GetExecutorId())
}

func TestRunReadoptsAgentAfterReconnect(t *testing.T) {
	h := startExecutor(t, nil)
	stream, _ := h.accept(t)
	leasedAgent := newLeasedAgent(t, "a")
	stream.lease(t, leasedAgent)
	firstActor := h.awaitActor(t, leasedAgent.GetName())
	stream.input(t, leasedAgent.GetName(), "i1")
	awaitChat(t, h.aiServiceClient.generating)

	stream.recvErrors <- status.Errorf(codes.Unavailable, "service restarting").Err()
	stream, _ = h.accept(t)
	stream.lease(t, leasedAgent)
	stream.input(t, leasedAgent.GetName(), "i1")
	require.Equal(t, leasedAgent.GetChat(), awaitChat(t, h.aiServiceClient.generating))
	requireClosed(t, firstActor.done)
	require.NotSame(t, firstActor, h.actor(leasedAgent.GetName()))
	require.Equal(t, 1, h.aiServiceClient.maxConcurrentGenerationsPerChat())
}

func TestRunReleasedAgentWaitsOutItsPredecessor(t *testing.T) {
	h := startExecutor(t, nil)
	windDown := make(chan struct{})
	h.aiServiceClient.windDown = windDown
	stream, _ := h.accept(t)
	leasedAgent := newLeasedAgent(t, "a")
	stream.lease(t, leasedAgent)
	firstActor := h.awaitActor(t, leasedAgent.GetName())
	stream.input(t, leasedAgent.GetName(), "i1")
	awaitChat(t, h.aiServiceClient.generating)

	// Revoked, the first actor winds down slowly.
	revoke := &pb.ConnectExecutorResponse_Revoke{Agent: leasedAgent.GetName()}
	stream.respond(t, &pb.ConnectExecutorResponse{Content: &pb.ConnectExecutorResponse_Revoke_{Revoke: revoke}})
	awaitChat(t, h.aiServiceClient.cancelled)
	stream.lease(t, leasedAgent)
	stream.input(t, leasedAgent.GetName(), "i2")
	require.Never(t, func() bool { return len(h.aiServiceClient.generating) > 0 }, 50*time.Millisecond, time.Millisecond)

	close(windDown)
	requireClosed(t, firstActor.done)
	awaitChat(t, h.aiServiceClient.generating)
	require.Equal(t, 1, h.aiServiceClient.maxConcurrentGenerationsPerChat())
}

func TestRunShutdownReportsCancelledTurns(t *testing.T) {
	h := startExecutor(t, nil)
	stream, _ := h.accept(t)
	leasedAgent := newLeasedAgent(t, "a")
	idleAgent := newLeasedAgent(t, "idle")
	stream.lease(t, leasedAgent)
	stream.lease(t, idleAgent)
	h.awaitActor(t, idleAgent.GetName())
	stream.input(t, leasedAgent.GetName(), "i1")
	awaitChat(t, h.aiServiceClient.generating)

	h.cancel()
	request := stream.await(t, func(request *pb.ConnectExecutorRequest) bool {
		return request.GetReport().GetEvent().GetTurnEnded() != nil
	})
	require.Equal(t, leasedAgent.GetName(), request.GetReport().GetAgent())
	require.Equal(t, agentpb.Agent_TURN_OUTCOME_CANCELLED, request.GetReport().GetEvent().GetTurnEnded().GetOutcome())
	// Half-closed after the report, so the service records it before releasing.
	requireClosed(t, stream.sendClosed)
	require.NoError(t, h.stop(t))
	require.Nil(t, h.actor(leasedAgent.GetName()))
	require.Nil(t, h.actor(idleAgent.GetName()))
}

func TestRunShutdownBeforeRegistration(t *testing.T) {
	h := startExecutor(t, nil)
	h.awaitStream(t)
	require.NoError(t, h.stop(t))
}

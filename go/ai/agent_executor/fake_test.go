package agent_executor

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	testUser  = "organizations/o/users/u"
	testAgent = "organizations/o/users/u/agents/a"
	testRoot  = "organizations/o/users/u/agents/root"
	testChat  = "organizations/o/users/u/chats/c"
	testModel = "providers/p/models/m"
)

// fakeGeneration scripts one StreamGenerateMessage call: its responses, then
// err (EOF when nil), or a block until the call's context is cancelled.
type fakeGeneration struct {
	responses []*aiservicepb.StreamGenerateMessageResponse
	err       error
	block     bool
	// started is closed when the generation is requested, if set.
	started chan struct{}
}

type fakeStream struct {
	grpc.ClientStream
	ctx        context.Context
	generation *fakeGeneration
}

func (s *fakeStream) Recv() (*aiservicepb.StreamGenerateMessageResponse, error) {
	if len(s.generation.responses) > 0 {
		response := s.generation.responses[0]
		s.generation.responses = s.generation.responses[1:]
		return response, nil
	}
	if s.generation.block {
		<-s.ctx.Done()
		return nil, status.FromError(s.ctx.Err(), "generating").Err()
	}
	if s.generation.err != nil {
		return nil, s.generation.err
	}
	return nil, io.EOF
}

// fakeAiServiceClient records persisted messages and plays scripted generations.
type fakeAiServiceClient struct {
	aiservicepb.AiServiceClient
	mutex                   sync.Mutex
	createMessageRequests   []*aiservicepb.CreateMessageRequest
	generateMessageRequests []*aiservicepb.GenerateMessageRequest
	messageIDSet            map[string]struct{}
	generations             []*fakeGeneration
	// calls logs persisted roles and generations in order.
	calls []string
}

func newFakeAiServiceClient(generations ...*fakeGeneration) *fakeAiServiceClient {
	return &fakeAiServiceClient{messageIDSet: map[string]struct{}{}, generations: generations}
}

func (c *fakeAiServiceClient) CreateMessage(ctx context.Context, request *aiservicepb.CreateMessageRequest, _ ...grpc.CallOption) (*aipb.Message, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.createMessageRequests = append(c.createMessageRequests, request)
	c.calls = append(c.calls, request.GetMessage().GetRole().String())
	messageID := request.GetMessageId()
	if messageID == "" {
		messageID = fmt.Sprintf("m%d", len(c.createMessageRequests))
	}
	if _, ok := c.messageIDSet[messageID]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "message already exists").Err()
	}
	c.messageIDSet[messageID] = struct{}{}
	messageRn, err := aipb.NewMessageRn(request.GetParent(), messageID)
	if err != nil {
		return nil, err
	}
	message := proto.Clone(request.GetMessage()).(*aipb.Message)
	message.Name = messageRn.String()
	return message, nil
}

func (c *fakeAiServiceClient) StreamGenerateMessage(ctx context.Context, request *aiservicepb.GenerateMessageRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[aiservicepb.StreamGenerateMessageResponse], error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.generateMessageRequests = append(c.generateMessageRequests, request)
	c.calls = append(c.calls, "generate")
	if len(c.generations) == 0 {
		return nil, status.Errorf(codes.Internal, "unexpected generation").Err()
	}
	generation := c.generations[0]
	c.generations = c.generations[1:]
	if generation.started != nil {
		close(generation.started)
	}
	return &fakeStream{ctx: ctx, generation: generation}, nil
}

// persistedMessages returns the messages the executor created, by role.
func (c *fakeAiServiceClient) persistedMessages(role aipb.Role) []*aipb.Message {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var messages []*aipb.Message
	for _, createMessageRequest := range c.createMessageRequests {
		if createMessageRequest.GetMessage().GetRole() == role {
			messages = append(messages, createMessageRequest.GetMessage())
		}
	}
	return messages
}

// fakeConnection captures what the agent sends up.
type fakeConnection struct {
	mutex    sync.Mutex
	requests []*pb.ConnectExecutorRequest
}

func (c *fakeConnection) send(_ context.Context, request *pb.ConnectExecutorRequest) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.requests = append(c.requests, request)
	return nil
}

func (c *fakeConnection) events() []*agentpb.Event {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var events []*agentpb.Event
	for _, request := range c.requests {
		if report := request.GetReport(); report != nil {
			events = append(events, report.GetEvent())
		}
	}
	return events
}

func (c *fakeConnection) deltas() []*aipb.Block {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var blocks []*aipb.Block
	for _, request := range c.requests {
		if delta := request.GetDelta(); delta != nil {
			blocks = append(blocks, delta.GetBlock())
		}
	}
	return blocks
}

// turnEnded returns the last event, which must end the turn.
func (c *fakeConnection) turnEnded(t *testing.T) *agentpb.Event_TurnEnded {
	t.Helper()
	events := c.events()
	require.NotEmpty(t, events)
	turnEnded := events[len(events)-1].GetTurnEnded()
	require.NotNil(t, turnEnded, "last event: %v", events[len(events)-1])
	return turnEnded
}

// fakeTool runs execute for each call.
type fakeTool struct {
	name    string
	execute func(ctx context.Context, toolCall *aipb.ToolCall) (*aipb.ToolResult, error)
	mutex   sync.Mutex
	calls   int
}

func (t *fakeTool) Definition() *aipb.Tool {
	return &aipb.Tool{Name: t.name, Description: t.name}
}

func (t *fakeTool) Execute(ctx context.Context, _ *agentpb.Agent, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
	t.mutex.Lock()
	t.calls++
	t.mutex.Unlock()
	return t.execute(ctx, toolCall)
}

func (t *fakeTool) callCount() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.calls
}

// echoTool answers each call with its id.
func echoTool(name string) *fakeTool {
	return &fakeTool{name: name, execute: func(_ context.Context, toolCall *aipb.ToolCall) (*aipb.ToolResult, error) {
		return ai.NewToolResult(toolCall.GetName(), toolCall.GetId(), "ran "+toolCall.GetId()), nil
	}}
}

// fakeAgentServiceClient records the agent tools' requests.
type fakeAgentServiceClient struct {
	pb.AgentServiceClient
	mutex                 sync.Mutex
	createAgentRequests   []*pb.CreateAgentRequest
	createInputRequests   []*pb.CreateInputRequest
	listAgentsRequests    []*pb.ListAgentsRequest
	updateAgentRequests   []*pb.UpdateAgentRequest
	nameToAgent           map[string]*agentpb.Agent
	listAgentsResponse    *pb.ListAgentsResponse
	createAgentErrorTitle string
}

func newFakeAgentServiceClient(agents ...*agentpb.Agent) *fakeAgentServiceClient {
	nameToAgent := map[string]*agentpb.Agent{}
	for _, agent := range agents {
		nameToAgent[agent.GetName()] = agent
	}
	return &fakeAgentServiceClient{nameToAgent: nameToAgent, listAgentsResponse: &pb.ListAgentsResponse{}}
}

func (c *fakeAgentServiceClient) CreateAgent(ctx context.Context, request *pb.CreateAgentRequest, _ ...grpc.CallOption) (*agentpb.Agent, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.createAgentRequests = append(c.createAgentRequests, request)
	if request.GetAgent().GetTitle() == c.createAgentErrorTitle {
		return nil, status.Errorf(codes.ResourceExhausted, "too many agents").Err()
	}
	agentRn, err := agentpb.NewAgentRn(request.GetParent(), fmt.Sprintf("sub%d", len(c.createAgentRequests)))
	if err != nil {
		return nil, err
	}
	agent := proto.Clone(request.GetAgent()).(*agentpb.Agent)
	agent.Name = agentRn.String()
	return agent, nil
}

func (c *fakeAgentServiceClient) CreateInput(ctx context.Context, request *pb.CreateInputRequest, _ ...grpc.CallOption) (*agentpb.Input, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.createInputRequests = append(c.createInputRequests, request)
	return request.GetInput(), nil
}

func (c *fakeAgentServiceClient) GetAgent(ctx context.Context, request *pb.GetAgentRequest, _ ...grpc.CallOption) (*agentpb.Agent, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	agent, ok := c.nameToAgent[request.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "agent %q not found", request.GetName()).Err()
	}
	return proto.Clone(agent).(*agentpb.Agent), nil
}

func (c *fakeAgentServiceClient) ListAgents(ctx context.Context, request *pb.ListAgentsRequest, _ ...grpc.CallOption) (*pb.ListAgentsResponse, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.listAgentsRequests = append(c.listAgentsRequests, request)
	return proto.Clone(c.listAgentsResponse).(*pb.ListAgentsResponse), nil
}

func (c *fakeAgentServiceClient) UpdateAgent(ctx context.Context, request *pb.UpdateAgentRequest, _ ...grpc.CallOption) (*agentpb.Agent, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.updateAgentRequests = append(c.updateAgentRequests, request)
	agent := proto.Clone(c.nameToAgent[request.GetAgent().GetName()]).(*agentpb.Agent)
	agent.Labels = request.GetAgent().GetLabels()
	return agent, nil
}

// newTestAgent returns an actor for a leased agent, and what it sends up.
func newTestAgent(t *testing.T, aiServiceClient *fakeAiServiceClient, spec *agentpb.AgentSpec, tools ...Tool) (*agent, *fakeConnection) {
	t.Helper()
	executor, err := New(&Opts{User: testUser}, newFakeAgentServiceClient(), aiServiceClient, tools...)
	require.NoError(t, err)
	leasedAgent := &agentpb.Agent{
		Name:      testAgent,
		RootAgent: testRoot,
		Chat:      testChat,
		Spec:      spec,
	}
	connection := &fakeConnection{}
	a := newAgent(context.Background(), executor, leasedAgent, connection.send)
	t.Cleanup(a.stop)
	return a, connection
}

func newToolCall(t *testing.T, id, name string, arguments map[string]any) *aipb.ToolCall {
	t.Helper()
	argumentsStruct, err := structpb.NewStruct(arguments)
	require.NoError(t, err)
	return &aipb.ToolCall{Id: id, Name: name, Arguments: argumentsStruct}
}

// generated scripts a generation streaming then persisting the given blocks.
func generated(blocks ...*aipb.Block) *fakeGeneration {
	generation := &fakeGeneration{}
	for _, block := range blocks {
		generation.responses = append(generation.responses, &aiservicepb.StreamGenerateMessageResponse{
			Content: &aiservicepb.StreamGenerateMessageResponse_Block{Block: block},
		})
	}
	generatedMessage := ai.NewAssistantMessage(blocks...)
	chatRn, _ := aipb.ParseChatRn(testChat)
	generatedMessage.Name = chatRn.MessageRn("generated").String()
	generation.responses = append(generation.responses, &aiservicepb.StreamGenerateMessageResponse{
		Content: &aiservicepb.StreamGenerateMessageResponse_GeneratedMessage{GeneratedMessage: generatedMessage},
	})
	return generation
}

func newInput(name, text, sender string) *agentpb.Input {
	message := ai.NewUserMessage(ai.NewTextBlock(text))
	if sender != "" {
		agentpb.Annotations.Sender.Set(message, sender)
	}
	agentRn, _ := agentpb.ParseAgentRn(testAgent)
	return &agentpb.Input{Name: agentRn.InputRn(name).String(), Message: message}
}

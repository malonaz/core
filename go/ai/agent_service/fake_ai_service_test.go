package agent_service

import (
	"context"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/aip"
	coregrpc "github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// fakeGeneration scripts one StreamGenerateMessage call: its deltas, then the assistant message it persists, or
// err, or, with block, a wait until the call is cancelled.
type fakeGeneration struct {
	deltas  []*aipb.Block
	message *aipb.Message
	err     error
	block   bool
	// started, if set, is closed when the generation is requested.
	started chan struct{}
	// gate, if set, holds the generation back until it is closed.
	gate chan struct{}
}

// fakeAiService is an in-memory AiService client: chats, messages and scripted generations. It admits the service's
// identity alone, as the scheduler fake does, but on generations: the chat's owner's, an agent of theirs its actor.
// Methods not implemented panic.
type fakeAiService struct {
	aiservicepb.AiServiceClient

	mutex              sync.Mutex
	requestIDToChat    map[string]*aipb.Chat
	chatToMessages     map[string][]*aipb.Message
	requestIDToMessage map[string]*aipb.Message
	chatToGenerations  map[string][]*fakeGeneration
	generateRequests   []*aiservicepb.GenerateMessageRequest
	// The conversation history each generation would send its model, as the AiService filters it.
	generateHistories [][]*aipb.Message
	lastCreateTime    time.Time
	// defaultGeneration, if set, plays when a chat has no scripted generation left.
	defaultGeneration *fakeGeneration
	// The stored tools: the platform tools of the tests' targets, as the ai processor mirrors them.
	tools []*aipb.Tool
}

// refusedModel is a model no owner may generate with: the fake refuses it as a host's rule on the model would.
const refusedModel = "providers/test/models/refused"

// authenticateOwner refuses a generation not made as the chat's owner with an agent of theirs as actor: the token a
// turn exchanges, never the service's.
func authenticateOwner(ctx context.Context, chat string) error {
	callMetadata, err := coregrpc.CallMetadata(ctx)
	if err != nil {
		return err
	}
	chatRn := &aipb.ChatRn{}
	if err := chatRn.UnmarshalString(chat); err != nil {
		return status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	owner := chatRn.UserRn().String()
	token, err := jwt.ParseInsecure([]byte(strings.TrimPrefix(callMetadata["authorization"], "Bearer ")))
	if err != nil || token.Subject() != owner {
		return status.Errorf(codes.Unauthenticated, "not called as the chat's owner").Err()
	}
	act, _ := token.Get("act")
	actor, _ := act.(map[string]any)["sub"].(string)
	if !strings.HasPrefix(actor, owner+"/agents/") {
		return status.Errorf(codes.Unauthenticated, "not called by an agent of the chat's owner").Err()
	}
	return nil
}

func newFakeAiService() *fakeAiService {
	return &fakeAiService{
		requestIDToChat:    map[string]*aipb.Chat{},
		chatToMessages:     map[string][]*aipb.Message{},
		requestIDToMessage: map[string]*aipb.Message{},
		chatToGenerations:  map[string][]*fakeGeneration{},
		tools:              mirroredTools,
	}
}

// mirroredTools are the platform tools of the services the tests route, as the ai processor mirrors them.
var mirroredTools = func() []*aipb.Tool {
	files := map[string]string{
		healthService:    "grpc/health/v1/health.proto",
		agentServiceName: "malonaz/ai/agent/agent_service/v1/agent_service.proto",
		libraryService:   "malonaz/test/library/library_service/v1/library_service.proto",
		mockService:      "malonaz/test/mock/v1/mock_service.proto",
	}
	var tools []*aipb.Tool
	for service, file := range files {
		builder, err := newInProcessBuilder(context.Background(), file)
		if err != nil {
			panic(err)
		}
		descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service))
		if err != nil {
			panic(err)
		}
		methods := descriptor.(protoreflect.ServiceDescriptor).Methods()
		for i := 0; i < methods.Len(); i++ {
			method := methods.Get(i)
			if method.IsStreamingClient() || method.IsStreamingServer() {
				continue
			}
			tool, err := builder.CreateTool(aitool.NewMethodToolRequest(method))
			if err != nil {
				panic(err)
			}
			tool.Title = tool.GetName()
			tool.Name = "tools/" + aitool.MethodToolID(method)
			tool.Labels = map[string]string{aipb.Labels.ToolSet.GetKey(): strings.ReplaceAll(strings.ToLower(service), ".", "-")}
			tools = append(tools, tool)
		}
	}
	return tools
}()

// toolSetFilter matches the filter the AgentService lists a tool set's tools with.
var toolSetFilter = regexp.MustCompile(`^labels\."ai\.malonaz\.com/tool-set" = "([^"]+)"$`)

// ListTools lists the stored platform tools of a set, in one page.
func (c *fakeAiService) ListTools(ctx context.Context, request *aiservicepb.ListToolsRequest, _ ...grpc.CallOption) (*aiservicepb.ListToolsResponse, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	match := toolSetFilter.FindStringSubmatch(request.Filter)
	if match == nil {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported filter %q", request.Filter).Err()
	}
	listToolsResponse := &aiservicepb.ListToolsResponse{}
	for _, tool := range c.tools {
		if request.Parent == "" && tool.GetLabels()[aipb.Labels.ToolSet.GetKey()] == match[1] {
			listToolsResponse.Tools = append(listToolsResponse.Tools, proto.CloneOf(tool))
		}
	}
	return listToolsResponse, nil
}

// GetTool returns a stored tool.
func (c *fakeAiService) GetTool(ctx context.Context, request *aiservicepb.GetToolRequest, _ ...grpc.CallOption) (*aipb.Tool, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	for _, tool := range c.tools {
		if tool.GetName() == request.Name {
			return proto.CloneOf(tool), nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "tool %s not found", request.Name).Err()
}

// script queues generations for a chat, played in order by its StreamGenerateMessage calls.
func (c *fakeAiService) script(chat string, generations ...*fakeGeneration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.chatToGenerations[chat] = append(c.chatToGenerations[chat], generations...)
}

// chat returns a chat created by CreateChat.
func (c *fakeAiService) chat(name string) *aipb.Chat {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, chat := range c.requestIDToChat {
		if chat.Name == name {
			return proto.CloneOf(chat)
		}
	}
	return nil
}

// setDefaultGeneration sets the generation played by chats with no scripted generation left.
func (c *fakeAiService) setDefaultGeneration(generation *fakeGeneration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.defaultGeneration = generation
}

// messages returns the messages of a chat, oldest first.
func (c *fakeAiService) messages(chat string) []*aipb.Message {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	messages := make([]*aipb.Message, 0, len(c.chatToMessages[chat]))
	for _, message := range c.chatToMessages[chat] {
		messages = append(messages, proto.CloneOf(message))
	}
	return messages
}

// generateMessageHistories returns the conversation history of each generation of a chat so far.
func (c *fakeAiService) generateMessageHistories(chat string) [][]*aipb.Message {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var histories [][]*aipb.Message
	for i, generateRequest := range c.generateRequests {
		if generateRequest.Parent == chat {
			histories = append(histories, c.generateHistories[i])
		}
	}
	return histories
}

// generateMessageRequests returns the StreamGenerateMessage requests of a chat received so far. Tests filter by chat:
// replicas also take over the agents other tests left running.
func (c *fakeAiService) generateMessageRequests(chat string) []*aiservicepb.GenerateMessageRequest {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var generateRequests []*aiservicepb.GenerateMessageRequest
	for _, generateRequest := range c.generateRequests {
		if generateRequest.Parent == chat {
			generateRequests = append(generateRequests, generateRequest)
		}
	}
	return generateRequests
}

// CreateChat creates a chat named after its request id: replaying a request id returns the same chat.
func (c *fakeAiService) CreateChat(ctx context.Context, request *aiservicepb.CreateChatRequest, _ ...grpc.CallOption) (*aipb.Chat, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	requestID := request.RequestId
	if requestID == "" {
		requestID = uuid.MustNewV7().String()
	}
	if chat, ok := c.requestIDToChat[requestID]; ok {
		return proto.CloneOf(chat), nil
	}
	userRn, err := aipb.ParseUserRn(request.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	chat := proto.CloneOf(request.Chat)
	chat.Name = userRn.ChatRn(aip.NewDeterministicBase32ResourceID(aip.MustGetUUIDNamespace(&aipb.Chat{}), requestID)).String()
	c.requestIDToChat[requestID] = chat
	return proto.CloneOf(chat), nil
}

// CreateMessage appends a message to its chat: an existing message id is AlreadyExists, a replayed request id
// returns the message it created.
func (c *fakeAiService) CreateMessage(ctx context.Context, request *aiservicepb.CreateMessageRequest, _ ...grpc.CallOption) (*aipb.Message, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.createMessage(request)
}

func (c *fakeAiService) createMessage(request *aiservicepb.CreateMessageRequest) (*aipb.Message, error) {
	if request.RequestId != "" {
		if message, ok := c.requestIDToMessage[request.RequestId]; ok {
			return proto.CloneOf(message), nil
		}
	}
	chatRn, err := aipb.ParseChatRn(request.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	messageID := request.MessageId
	if messageID == "" {
		messageID = aip.NewSystemGeneratedBase32ResourceID()
	}
	message := proto.CloneOf(request.Message)
	message.Name = chatRn.MessageRn(messageID).String()
	for _, existingMessage := range c.chatToMessages[request.Parent] {
		if existingMessage.Name == message.Name {
			return nil, status.Errorf(codes.AlreadyExists, "message %s already exists", message.Name).Err()
		}
	}
	// Strictly increasing, so create_time orders the chat.
	createTime := time.Now().UTC()
	if !createTime.After(c.lastCreateTime) {
		createTime = c.lastCreateTime.Add(time.Microsecond)
	}
	c.lastCreateTime = createTime
	message.CreateTime = timestamppb.New(createTime)
	c.chatToMessages[request.Parent] = append(c.chatToMessages[request.Parent], message)
	if request.RequestId != "" {
		c.requestIDToMessage[request.RequestId] = message
	}
	return proto.CloneOf(message), nil
}

func (c *fakeAiService) GetMessage(ctx context.Context, request *aiservicepb.GetMessageRequest, _ ...grpc.CallOption) (*aipb.Message, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	messageRn, err := aipb.ParseMessageRn(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing name: %v", err).Err()
	}
	for _, message := range c.chatToMessages[messageRn.ChatRn().String()] {
		if message.Name == request.Name {
			return proto.CloneOf(message), nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "message %s does not exist", request.Name).Err()
}

// ListMessages pages through a chat in create_time order, ascending unless ordered "create_time desc". It knows the
// filter the AgentService sends: the conversation history.
func (c *fakeAiService) ListMessages(ctx context.Context, request *aiservicepb.ListMessagesRequest, _ ...grpc.CallOption) (*aiservicepb.ListMessagesResponse, error) {
	if err := authenticate(ctx); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	messages, err := filterMessages(c.chatToMessages[request.Parent], request.Filter)
	if err != nil {
		return nil, err
	}
	if request.OrderBy == "create_time desc" {
		slices.Reverse(messages)
	}
	offset := 0
	if request.PageToken != "" {
		if offset, err = strconv.Atoi(request.PageToken); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "parsing page token: %v", err).Err()
		}
	}
	pageSize := int(request.PageSize)
	if pageSize == 0 {
		pageSize = 2
	}
	end := min(offset+pageSize, len(messages))
	listMessagesResponse := &aiservicepb.ListMessagesResponse{}
	for _, message := range messages[offset:end] {
		listMessagesResponse.Messages = append(listMessagesResponse.Messages, proto.CloneOf(message))
	}
	if end < len(messages) {
		listMessagesResponse.NextPageToken = strconv.Itoa(end)
	}
	return listMessagesResponse, nil
}

// filterMessages applies one of the filters the AgentService sends.
func filterMessages(messages []*aipb.Message, filter string) ([]*aipb.Message, error) {
	var keep func(*aipb.Message) bool
	switch filter {
	case "":
		keep = func(*aipb.Message) bool { return true }
	case ai.HistoryFilter:
		keep = func(message *aipb.Message) bool {
			return message.GetStatus() == nil && !aip.HasLabel(message, aipb.Labels.Superseded.GetKey())
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported filter %q", filter).Err()
	}
	var kept []*aipb.Message
	for _, message := range messages {
		if keep(message) {
			kept = append(kept, message)
		}
	}
	return kept, nil
}

// StreamGenerateMessage plays the chat's next scripted generation, persisting its message as the AiService does.
func (c *fakeAiService) StreamGenerateMessage(ctx context.Context, request *aiservicepb.GenerateMessageRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[aiservicepb.StreamGenerateMessageResponse], error) {
	if err := authenticateOwner(ctx, request.Parent); err != nil {
		return nil, err
	}
	if request.Model == refusedModel {
		return nil, status.Errorf(codes.PermissionDenied, "no grant allows /malonaz.ai.ai_service.v1.AiService/StreamGenerateMessage").Err()
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.generateRequests = append(c.generateRequests, proto.CloneOf(request))
	history, _ := filterMessages(c.chatToMessages[request.Parent], ai.HistoryFilter)
	c.generateHistories = append(c.generateHistories, history)
	generations := c.chatToGenerations[request.Parent]
	var generation *fakeGeneration
	switch {
	case len(generations) > 0:
		generation = generations[0]
		c.chatToGenerations[request.Parent] = generations[1:]
		if generation.started != nil {
			close(generation.started)
		}
	case c.defaultGeneration != nil:
		generation = c.defaultGeneration
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "no generation scripted for chat %s", request.Parent).Err()
	}
	return &fakeGenerationStream{ctx: ctx, service: c, chat: request.Parent, generation: generation}, nil
}

type fakeGenerationStream struct {
	grpc.ClientStream
	ctx        context.Context
	service    *fakeAiService
	chat       string
	generation *fakeGeneration
	deltaIndex int
	done       bool
}

func (s *fakeGenerationStream) Recv() (*aiservicepb.StreamGenerateMessageResponse, error) {
	if s.generation.gate != nil {
		select {
		case <-s.generation.gate:
		case <-s.ctx.Done():
		}
	}
	if err := s.ctx.Err(); err != nil {
		return nil, status.FromError(err, "generating").Err()
	}
	if s.deltaIndex < len(s.generation.deltas) {
		block := s.generation.deltas[s.deltaIndex]
		s.deltaIndex++
		return &aiservicepb.StreamGenerateMessageResponse{Content: &aiservicepb.StreamGenerateMessageResponse_Block{Block: block}}, nil
	}
	switch {
	case s.done:
		return nil, io.EOF
	case s.generation.block:
		<-s.ctx.Done()
		return nil, status.FromError(s.ctx.Err(), "generating").Err()
	case s.generation.err != nil:
		return nil, s.generation.err
	}
	s.done = true
	s.service.mutex.Lock()
	defer s.service.mutex.Unlock()
	createMessageRequest := &aiservicepb.CreateMessageRequest{Parent: s.chat, Message: s.generation.message}
	message, err := s.service.createMessage(createMessageRequest)
	if err != nil {
		return nil, err
	}
	return &aiservicepb.StreamGenerateMessageResponse{Content: &aiservicepb.StreamGenerateMessageResponse_GeneratedMessage{GeneratedMessage: message}}, nil
}

// textGeneration scripts a final answer.
func textGeneration(text string) *fakeGeneration {
	return &fakeGeneration{message: ai.NewAssistantMessage(ai.NewTextBlock(text))}
}

// toolCallGeneration scripts an assistant message calling tools.
func toolCallGeneration(toolCalls ...*aipb.ToolUse) *fakeGeneration {
	blocks := make([]*aipb.Block, 0, len(toolCalls))
	for _, toolCall := range toolCalls {
		blocks = append(blocks, ai.NewToolCallBlock(toolCall))
	}
	return &fakeGeneration{message: ai.NewAssistantMessage(blocks...)}
}

// newToolCall returns a tool call with the given arguments, which must convert to a Struct.
func newToolCall(id, name string, arguments map[string]any) *aipb.ToolUse {
	argumentsStruct, err := structpb.NewStruct(arguments)
	if err != nil {
		panic(err)
	}
	return &aipb.ToolUse{Id: id, Name: name, Arguments: argumentsStruct}
}

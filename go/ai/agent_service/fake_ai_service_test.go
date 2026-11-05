package agent_service

import (
	"context"
	"io"
	"slices"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
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
}

// fakeAiService is an in-memory AiService client: chats, messages and scripted generations. Methods not
// implemented panic.
type fakeAiService struct {
	aiservicepb.AiServiceClient

	mutex              sync.Mutex
	requestIDToChat    map[string]*aipb.Chat
	chatToMessages     map[string][]*aipb.Message
	requestIDToMessage map[string]*aipb.Message
	chatToGenerations  map[string][]*fakeGeneration
	generateRequests   []*aiservicepb.GenerateMessageRequest
	lastCreateTime     time.Time
	// defaultGeneration, if set, plays when a chat has no scripted generation left.
	defaultGeneration *fakeGeneration
}

func newFakeAiService() *fakeAiService {
	return &fakeAiService{
		requestIDToChat:    map[string]*aipb.Chat{},
		chatToMessages:     map[string][]*aipb.Message{},
		requestIDToMessage: map[string]*aipb.Message{},
		chatToGenerations:  map[string][]*fakeGeneration{},
	}
}

// script queues generations for a chat, played in order by its StreamGenerateMessage calls.
func (c *fakeAiService) script(chat string, generations ...*fakeGeneration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.chatToGenerations[chat] = append(c.chatToGenerations[chat], generations...)
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

// generateMessageRequests returns the StreamGenerateMessage requests received so far.
func (c *fakeAiService) generateMessageRequests() []*aiservicepb.GenerateMessageRequest {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return slices.Clone(c.generateRequests)
}

// CreateChat creates a chat named after its request id: replaying a request id returns the same chat.
func (c *fakeAiService) CreateChat(ctx context.Context, request *aiservicepb.CreateChatRequest, _ ...grpc.CallOption) (*aipb.Chat, error) {
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

// ListMessages pages through a chat in create_time order, ascending unless ordered "create_time desc". Filters are
// ignored: the fake keeps no failed or superseded message.
func (c *fakeAiService) ListMessages(ctx context.Context, request *aiservicepb.ListMessagesRequest, _ ...grpc.CallOption) (*aiservicepb.ListMessagesResponse, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	messages := slices.Clone(c.chatToMessages[request.Parent])
	if request.OrderBy == "create_time desc" {
		slices.Reverse(messages)
	}
	offset := 0
	if request.PageToken != "" {
		var err error
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

// StreamGenerateMessage plays the chat's next scripted generation, persisting its message as the AiService does.
func (c *fakeAiService) StreamGenerateMessage(ctx context.Context, request *aiservicepb.GenerateMessageRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[aiservicepb.StreamGenerateMessageResponse], error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.generateRequests = append(c.generateRequests, proto.CloneOf(request))
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
func toolCallGeneration(toolCalls ...*aipb.ToolCall) *fakeGeneration {
	blocks := make([]*aipb.Block, 0, len(toolCalls))
	for _, toolCall := range toolCalls {
		blocks = append(blocks, ai.NewToolCallBlock(toolCall))
	}
	return &fakeGeneration{message: ai.NewAssistantMessage(blocks...)}
}

// newToolCall returns a tool call with the given arguments, which must convert to a Struct.
func newToolCall(id, name string, arguments map[string]any) *aipb.ToolCall {
	argumentsStruct, err := structpb.NewStruct(arguments)
	if err != nil {
		panic(err)
	}
	return &aipb.ToolCall{Id: id, Name: name, Arguments: argumentsStruct}
}

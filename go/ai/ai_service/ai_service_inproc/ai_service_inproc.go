package ai_service_inproc

import (
	"context"

	"google.golang.org/grpc"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/grpcinproc"
)

// Client wraps the AI service for in-process calls with client-friendly methods.
type Client struct {
	server aiservicepb.AiServiceServer
}

// New creates a new in-process client wrapping the AI service.
func New(server aiservicepb.AiServiceServer) (*Client, error) {
	return &Client{server: server}, nil
}

// CreateModel creates a new model configuration and returns the resulting resource.
func (c *Client) CreateModel(
	ctx context.Context,
	request *aiservicepb.CreateModelRequest,
	opts ...grpc.CallOption,
) (*aipb.Model, error) {
	return c.server.CreateModel(ctx, request)
}

// GetModel retrieves a single model by its unique identifier.
func (c *Client) GetModel(
	ctx context.Context,
	request *aiservicepb.GetModelRequest,
	opts ...grpc.CallOption,
) (*aipb.Model, error) {
	return c.server.GetModel(ctx, request)
}

// ListModels returns a paginated list of all models visible to the caller.
func (c *Client) ListModels(
	ctx context.Context,
	request *aiservicepb.ListModelsRequest,
	opts ...grpc.CallOption,
) (*aiservicepb.ListModelsResponse, error) {
	return c.server.ListModels(ctx, request)
}

// CreateVoice registers a new voice profile and returns the created voice resource.
func (c *Client) CreateVoice(
	ctx context.Context,
	request *aiservicepb.CreateVoiceRequest,
	opts ...grpc.CallOption,
) (*aipb.Voice, error) {
	return c.server.CreateVoice(ctx, request)
}

// GetVoice fetches a single voice profile by its unique identifier.
func (c *Client) GetVoice(
	ctx context.Context,
	request *aiservicepb.GetVoiceRequest,
	opts ...grpc.CallOption,
) (*aipb.Voice, error) {
	return c.server.GetVoice(ctx, request)
}

// ListVoices returns a paginated list of all voices visible to the caller.
func (c *Client) ListVoices(
	ctx context.Context,
	request *aiservicepb.ListVoicesRequest,
	opts ...grpc.CallOption,
) (*aiservicepb.ListVoicesResponse, error) {
	return c.server.ListVoices(ctx, request)
}

// TextToTextStream provides a client-facing streaming interface.
// It wraps the underlying server streaming implementation using grpcinproc.
func (c *Client) TextToTextStream(
	ctx context.Context,
	request *aiservicepb.TextToTextStreamRequest,
	opts ...grpc.CallOption,
) (aiservicepb.AiService_TextToTextStreamClient, error) {
	// Use grpcinproc to convert the provider's server streaming implementation to a client
	serverStreamClient := grpcinproc.NewServerStreamAsClient[
		aiservicepb.TextToTextStreamRequest,
		aiservicepb.TextToTextStreamResponse,
		aiservicepb.AiService_TextToTextStreamServer,
	](c.server.TextToTextStream)

	return serverStreamClient(ctx, request, opts...)
}

// TextToText provides a client-facing interface for text-to-text conversion.
func (c *Client) TextToText(
	ctx context.Context,
	request *aiservicepb.TextToTextRequest,
	opts ...grpc.CallOption,
) (*aiservicepb.TextToTextResponse, error) {
	return c.server.TextToText(ctx, request)
}

// SpeechToText provides a client-facing interface for speech-to-text conversion.
func (c *Client) SpeechToText(
	ctx context.Context,
	request *aiservicepb.SpeechToTextRequest,
	opts ...grpc.CallOption,
) (*aiservicepb.SpeechToTextResponse, error) {
	return c.server.SpeechToText(ctx, request)
}

// TextToSpeech provides a client-facing interface for text-to-speech conversion.
func (c *Client) TextToSpeech(
	ctx context.Context,
	request *aiservicepb.TextToSpeechRequest,
	opts ...grpc.CallOption,
) (*aiservicepb.TextToSpeechResponse, error) {
	return c.server.TextToSpeech(ctx, request)
}

// TextToSpeechStream provides a client-facing streaming interface.
// It wraps the underlying server streaming implementation using grpcinproc.
func (c *Client) TextToSpeechStream(
	ctx context.Context,
	request *aiservicepb.TextToSpeechStreamRequest,
	opts ...grpc.CallOption,
) (aiservicepb.AiService_TextToSpeechStreamClient, error) {
	// Use grpcinproc to convert the provider's server streaming implementation to a client
	serverStreamClient := grpcinproc.NewServerStreamAsClient[
		aiservicepb.TextToSpeechStreamRequest,
		aiservicepb.TextToSpeechStreamResponse,
		aiservicepb.AiService_TextToSpeechStreamServer,
	](c.server.TextToSpeechStream)

	return serverStreamClient(ctx, request, opts...)
}

// SpeechToTextStream provides a client-facing streaming interface.
// It wraps the underlying server streaming implementation using grpcinproc.
// SpeechToTextStream provides a client-facing bidirectional streaming interface.
func (c *Client) SpeechToTextStream(
	ctx context.Context,
	opts ...grpc.CallOption,
) (aiservicepb.AiService_SpeechToTextStreamClient, error) {
	return grpcinproc.NewBidiStreamAsClient[
		aiservicepb.SpeechToTextStreamRequest,
		aiservicepb.SpeechToTextStreamResponse,
		aiservicepb.AiService_SpeechToTextStreamServer,
	](c.server.SpeechToTextStream)(ctx, opts...)
}

// CreateChat creates a new chat and returns the created resource.
func (c *Client) CreateChat(
	ctx context.Context,
	request *aiservicepb.CreateChatRequest,
	opts ...grpc.CallOption,
) (*aipb.Chat, error) {
	return c.server.CreateChat(ctx, request)
}

// GetChat retrieves a single chat by its resource name.
func (c *Client) GetChat(
	ctx context.Context,
	request *aiservicepb.GetChatRequest,
	opts ...grpc.CallOption,
) (*aipb.Chat, error) {
	return c.server.GetChat(ctx, request)
}

// UpdateChat updates an existing chat and returns the updated resource.
func (c *Client) UpdateChat(
	ctx context.Context,
	request *aiservicepb.UpdateChatRequest,
	opts ...grpc.CallOption,
) (*aipb.Chat, error) {
	return c.server.UpdateChat(ctx, request)
}

// DeleteChat soft-deletes a chat by its resource name.
func (c *Client) DeleteChat(
	ctx context.Context,
	request *aiservicepb.DeleteChatRequest,
	opts ...grpc.CallOption,
) (*aipb.Chat, error) {
	return c.server.DeleteChat(ctx, request)
}

// ListChats returns a paginated list of chats for a user.
func (c *Client) ListChats(
	ctx context.Context,
	request *aiservicepb.ListChatsRequest,
	opts ...grpc.CallOption,
) (*aiservicepb.ListChatsResponse, error) {
	return c.server.ListChats(ctx, request)
}

// Verify interface compliance at compile time.
var (
	_ aiservicepb.AiServiceClient = (*Client)(nil)
)

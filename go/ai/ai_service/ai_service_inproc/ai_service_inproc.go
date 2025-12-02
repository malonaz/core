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
	server aiservicepb.AiServer
}

// New creates a new in-process client wrapping the AI service.
func New(server aiservicepb.AiServer) (*Client, error) {
	return &Client{server: server}, nil
}

// CreateModel creates a new model configuration and returns the resulting resource.
func (c *Client) CreateModel(
	ctx context.Context,
	request *aiservicepb.CreateModelRequest,
	_ ...grpc.CallOption,
) (*aipb.Model, error) {
	return c.server.CreateModel(ctx, request)
}

// GetModel retrieves a single model by its unique identifier.
func (c *Client) GetModel(
	ctx context.Context,
	request *aiservicepb.GetModelRequest,
	_ ...grpc.CallOption,
) (*aipb.Model, error) {
	return c.server.GetModel(ctx, request)
}

// ListModels returns a paginated list of all models visible to the caller.
func (c *Client) ListModels(
	ctx context.Context,
	request *aiservicepb.ListModelsRequest,
	_ ...grpc.CallOption,
) (*aiservicepb.ListModelsResponse, error) {
	return c.server.ListModels(ctx, request)
}

// CreateVoice registers a new voice profile and returns the created voice resource.
func (c *Client) CreateVoice(
	ctx context.Context,
	request *aiservicepb.CreateVoiceRequest,
	_ ...grpc.CallOption,
) (*aipb.Voice, error) {
	return c.server.CreateVoice(ctx, request)
}

// GetVoice fetches a single voice profile by its unique identifier.
func (c *Client) GetVoice(
	ctx context.Context,
	request *aiservicepb.GetVoiceRequest,
	_ ...grpc.CallOption,
) (*aipb.Voice, error) {
	return c.server.GetVoice(ctx, request)
}

// ListVoices returns a paginated list of all voices visible to the caller.
func (c *Client) ListVoices(
	ctx context.Context,
	request *aiservicepb.ListVoicesRequest,
	_ ...grpc.CallOption,
) (*aiservicepb.ListVoicesResponse, error) {
	return c.server.ListVoices(ctx, request)
}

// TextToTextStream provides a client-facing streaming interface.
// It wraps the underlying server streaming implementation using grpcinproc.
func (c *Client) TextToTextStream(
	ctx context.Context,
	request *aiservicepb.TextToTextStreamRequest,
	_ ...grpc.CallOption,
) (aiservicepb.Ai_TextToTextStreamClient, error) {
	// Use grpcinproc to convert the provider's server streaming implementation to a client
	serverStreamClient := grpcinproc.NewServerStreamAsClient[
		aiservicepb.TextToTextStreamRequest,
		aiservicepb.TextToTextStreamResponse,
		aiservicepb.Ai_TextToTextStreamServer,
	](c.server.TextToTextStream)

	return serverStreamClient(ctx, request)
}

// TextToText provides a client-facing interface for text-to-text conversion.
func (c *Client) TextToText(
	ctx context.Context,
	request *aiservicepb.TextToTextRequest,
	_ ...grpc.CallOption,
) (*aiservicepb.TextToTextResponse, error) {
	return c.server.TextToText(ctx, request)
}

// SpeechToText provides a client-facing interface for speech-to-text conversion.
func (c *Client) SpeechToText(
	ctx context.Context,
	request *aiservicepb.SpeechToTextRequest,
	_ ...grpc.CallOption,
) (*aiservicepb.SpeechToTextResponse, error) {
	return c.server.SpeechToText(ctx, request)
}

// TextToSpeech provides a client-facing interface for text-to-speech conversion.
func (c *Client) TextToSpeech(
	ctx context.Context,
	request *aiservicepb.TextToSpeechRequest,
	_ ...grpc.CallOption,
) (*aiservicepb.TextToSpeechResponse, error) {
	return c.server.TextToSpeech(ctx, request)
}

// TextToSpeechStream provides a client-facing streaming interface.
// It wraps the underlying server streaming implementation using grpcinproc.
func (c *Client) TextToSpeechStream(
	ctx context.Context,
	request *aiservicepb.TextToSpeechStreamRequest,
	_ ...grpc.CallOption,
) (aiservicepb.Ai_TextToSpeechStreamClient, error) {
	// Use grpcinproc to convert the provider's server streaming implementation to a client
	serverStreamClient := grpcinproc.NewServerStreamAsClient[
		aiservicepb.TextToSpeechStreamRequest,
		aiservicepb.TextToSpeechStreamResponse,
		aiservicepb.Ai_TextToSpeechStreamServer,
	](c.server.TextToSpeechStream)

	return serverStreamClient(ctx, request)
}

// Verify interface compliance at compile time.
var (
	_ aiservicepb.AiClient = (*Client)(nil)
)

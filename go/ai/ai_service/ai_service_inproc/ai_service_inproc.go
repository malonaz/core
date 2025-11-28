package ai_service_inproc

import (
	"context"

	"google.golang.org/grpc"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	"github.com/malonaz/core/go/ai/ai_service"
	"github.com/malonaz/core/go/grpc/grpcinproc"
)

// Client wraps the AI service for in-process calls with client-friendly methods.
type Client struct {
	*ai_service.Service
}

// New creates a new in-process client wrapping the AI service.
func New(opts *ai_service.Opts) (*Client, error) {
	svc, err := ai_service.New(opts)
	if err != nil {
		return nil, err
	}
	return &Client{Service: svc}, nil
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
	](c.Service.TextToSpeechStream)

	return serverStreamClient(ctx, request)
}

// TextToText provides a client-facing interface for text-to-text conversion.
func (c *Client) TextToText(
	ctx context.Context,
	request *aiservicepb.TextToTextRequest,
	_ ...grpc.CallOption,
) (*aiservicepb.TextToTextResponse, error) {
	return c.Service.TextToText(ctx, request)
}

// SpeechToText provides a client-facing interface for speech-to-text conversion.
func (c *Client) SpeechToText(
	ctx context.Context,
	request *aiservicepb.SpeechToTextRequest,
	_ ...grpc.CallOption,
) (*aiservicepb.SpeechToTextResponse, error) {
	return c.Service.SpeechToText(ctx, request)
}

// TextToSpeech provides a client-facing interface for text-to-speech conversion.
func (c *Client) TextToSpeech(
	ctx context.Context,
	request *aiservicepb.TextToSpeechRequest,
	_ ...grpc.CallOption,
) (*aiservicepb.TextToSpeechResponse, error) {
	return c.Service.TextToSpeech(ctx, request)
}

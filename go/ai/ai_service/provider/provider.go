package provider

import (
	"context"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

type Provider interface {
	ProviderId() string
	Start(context.Context) error
	Stop()
}

type SpeechToTextClient interface {
	Provider
	SpeechToText(context.Context, *aiservicepb.SpeechToTextRequest) (*aiservicepb.SpeechToTextResponse, error)
}

type SpeechToTextStreamClient interface {
	Provider
	SpeechToTextStream(aiservicepb.AiService_SpeechToTextStreamServer) error
}

// StreamMessageClient generates an assistant message from the given
// conversation history. Messages are passed separately from the request as
// they are loaded from the chat by the service layer.
type StreamMessageClient interface {
	Provider
	StreamMessage(request *aiservicepb.StreamMessageRequest, messages []*aipb.Message, srv aiservicepb.AiService_StreamMessageServer) error
}

// TextToSpeechClient uses the exact gRPC server streaming interface
type TextToSpeechClient interface {
	Provider
	TextToSpeechStream(*aiservicepb.TextToSpeechStreamRequest, aiservicepb.AiService_TextToSpeechStreamServer) error
}

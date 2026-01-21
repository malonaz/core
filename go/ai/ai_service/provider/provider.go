package provider

import (
	"context"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
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

type TextToTextClient interface {
	Provider
	TextToTextStream(*aiservicepb.TextToTextStreamRequest, aiservicepb.AiService_TextToTextStreamServer) error
}

// TextToSpeechClient uses the exact gRPC server streaming interface
type TextToSpeechClient interface {
	Provider
	TextToSpeechStream(*aiservicepb.TextToSpeechStreamRequest, aiservicepb.AiService_TextToSpeechStreamServer) error
}

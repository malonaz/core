package provider

import (
	"context"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

type Provider interface {
	ProviderId() string
	DefaultModels() []*aipb.Model
	Start(context.Context) error
	Stop()
}

type SpeechToTextClient interface {
	Provider
	SpeechToText(context.Context, *aiservicepb.SpeechToTextRequest) (*aiservicepb.SpeechToTextResponse, error)
}

type TextToTextClient interface {
	Provider
	TextToTextStream(*aiservicepb.TextToTextStreamRequest, aiservicepb.Ai_TextToTextStreamServer) error
}

// TextToSpeechClient uses the exact gRPC server streaming interface
type TextToSpeechClient interface {
	Provider
	TextToSpeechStream(*aiservicepb.TextToSpeechStreamRequest, aiservicepb.Ai_TextToSpeechStreamServer) error
}

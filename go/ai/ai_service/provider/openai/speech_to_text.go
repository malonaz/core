package openai

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	openai "github.com/sashabaranov/go-openai"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/audio"
)

func (c *Client) SpeechToText(ctx context.Context, request *aiservicepb.SpeechToTextRequest) (*aiservicepb.SpeechToTextResponse, error) {
	if len(request.Audio) == 0 {
		return nil, fmt.Errorf("audio data cannot be empty")
	}
	modelConfig, err := provider.GetModelConfig(request.Model)
	if err != nil {
		return nil, err
	}

	audioRequest := openai.AudioRequest{
		Model:    modelConfig.ModelId,
		FilePath: "audio.wav",
		Reader:   bytes.NewReader(request.Audio),
		Language: request.Language,
		Format:   openai.AudioResponseFormatJSON,
	}

	startTime := time.Now()
	response, err := c.client.CreateTranscription(ctx, audioRequest)
	if err != nil {
		return nil, fmt.Errorf("whisper transcription failed: %w", err)
	}
	generationMetrics := &aipb.GenerationMetrics{
		Ttlb: durationpb.New(time.Since(startTime)),
	}

	duration, err := audio.CalculateWAVDuration(request.Audio)
	if err != nil {
		return nil, fmt.Errorf("getting wav duration: %v", err)
	}

	modelUsage := &aipb.ModelUsage{
		Provider: c.Provider(),
		Model:    request.Model,
		InputSecond: &aipb.ResourceConsumption{
			Quantity: int32(duration.Round(time.Second).Seconds()),
		},
	}

	return &aiservicepb.SpeechToTextResponse{
		Transcript:        strings.TrimSpace(response.Text),
		ModelUsage:        modelUsage,
		GenerationMetrics: generationMetrics,
	}, nil
}

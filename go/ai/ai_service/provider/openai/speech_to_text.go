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
	"github.com/malonaz/core/go/audio"
)

func (c *Client) SpeechToText(ctx context.Context, request *aiservicepb.SpeechToTextRequest) (*aiservicepb.SpeechToTextResponse, error) {
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return nil, err
	}

	wavBytes := audio.PCMToWAV(request.AudioChunk.Data, request.AudioFormat)

	audioRequest := openai.AudioRequest{
		Model:    model.ProviderModelId,
		FilePath: "audio.wav",
		Reader:   bytes.NewReader(wavBytes),
		Language: request.LanguageCode,
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

	modelUsage := &aipb.ModelUsage{
		Model: request.Model,
		InputSecond: &aipb.ResourceConsumption{
			Quantity: int32(request.AudioChunk.Duration.AsDuration().Round(time.Second).Seconds()),
		},
	}

	return &aiservicepb.SpeechToTextResponse{
		Transcript:        strings.TrimSpace(response.Text),
		ModelUsage:        modelUsage,
		GenerationMetrics: generationMetrics,
	}, nil
}

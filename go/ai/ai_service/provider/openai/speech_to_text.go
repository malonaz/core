package openai

import (
	"bytes"
	"context"
	"strings"
	"time"

	openai "github.com/sashabaranov/go-openai"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/audio"
	"github.com/malonaz/core/go/grpc/status"
)

func (c *Client) SpeechToText(ctx context.Context, request *aiservicepb.SpeechToTextRequest) (*aiservicepb.SpeechToTextResponse, error) {
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return nil, err
	}

	if request.GetAudioFormat() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "audio_format is required").Err()
	}
	if encoding := request.GetAudioFormat().GetEncoding(); encoding != audiopb.Encoding_ENCODING_UNSPECIFIED {
		return nil, status.Errorf(codes.InvalidArgument, "openai speech to text takes linear PCM, not %s", encoding).Err()
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
		return nil, status.FromError(err, "creating transcription").Err()
	}
	generationMetrics := &aipb.GenerationMetrics{
		Ttlb: durationpb.New(time.Since(startTime)),
	}

	modelUsage := &aipb.ModelUsage{
		Model: request.Model,
		InputSecond: &aipb.ResourceConsumption{
			Quantity: int64(request.AudioChunk.Duration.AsDuration().Round(time.Second).Seconds()),
		},
	}

	return &aiservicepb.SpeechToTextResponse{
		Transcript:        strings.TrimSpace(response.Text),
		ModelUsage:        modelUsage,
		GenerationMetrics: generationMetrics,
	}, nil
}

package deepgram

import (
	"errors"
	"slices"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/audio"
	"github.com/malonaz/core/go/grpc/status"
)

// TextToSpeechStream implements TTS via Deepgram Flux (/v2/speak).
func (c *Client) TextToSpeechStream(
	request *aiservicepb.TextToSpeechStreamRequest,
	srv aiservicepb.AiService_TextToSpeechStreamServer,
) error {
	ctx := srv.Context()
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return status.FromError(err, "getting model").Err()
	}

	// Flux embeds the voice in the model string (flux-{voice}-{language}), so
	// the provider voice id *is* the Flux model string. Fall back to the
	// model's provider id when no voice is specified.
	fluxModel := request.GetProviderVoiceId()
	if fluxModel == "" {
		fluxModel = model.ProviderModelId
	}

	// Resolve output format; honor the preferred sample rate when supported.
	audioFormat := model.Tts.AudioFormat
	preferredSampleRate := request.GetConfiguration().GetPreferredSampleRate()
	if preferredSampleRate > 0 && slices.Contains(model.Tts.SupportedSampleRates, preferredSampleRate) {
		audioFormat.SampleRate = preferredSampleRate
	}

	// Optional provider settings forwarded as connection query parameters.
	speakOptions := &SpeakOptions{
		Model:      fluxModel,
		Encoding:   EncodingLinear16,
		SampleRate: int(audioFormat.SampleRate),
	}
	if providerSettings := request.GetConfiguration().GetProviderSettings(); providerSettings != nil {
		fields := providerSettings.GetFields()
		if v, ok := fields["speed"]; ok {
			speakOptions.Speed = v.GetStringValue()
		}
		if v, ok := fields["expressivity"]; ok {
			speakOptions.Expressivity = v.GetStringValue()
		}
	}

	startTime := time.Now()
	generationMetrics := &aipb.GenerationMetrics{}

	conn, err := c.Speak(ctx, speakOptions)
	if err != nil {
		return status.FromError(err, "connecting to deepgram speak").Err()
	}
	defer conn.Close()

	if err := srv.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_AudioFormat{AudioFormat: audioFormat},
	}); err != nil {
		return err
	}

	// Single-shot synthesis: send the full text, flush the turn, then close.
	// The server streams audio, then SpeechMetadata, then SessionMetadata.
	if err := conn.SendText(ctx, request.Text); err != nil {
		return status.FromError(err, "sending text").Err()
	}
	if err := conn.Flush(ctx); err != nil {
		return status.FromError(err, "flushing").Err()
	}
	if err := conn.SendClose(ctx); err != nil {
		return status.FromError(err, "closing session").Err()
	}

	var totalDuration time.Duration
	var billableCharacterCount int
	var chunkIndex uint32
	for {
		message, err := conn.ReceiveMessage(ctx)
		if err != nil {
			// A normal close after Close is the end-of-session signal.
			var closeError websocket.CloseError
			if errors.As(err, &closeError) && closeError.Code == websocket.StatusNormalClosure {
				break
			}
			return status.FromError(err, "receiving speak message").Err()
		}

		// Binary frames carry raw PCM audio.
		if message.Audio != nil {
			if generationMetrics.Ttfb == nil {
				generationMetrics.Ttfb = durationpb.New(time.Since(startTime))
			}
			chunkDuration, err := audio.CalculatePCMDuration(audioFormat, len(message.Audio))
			if err != nil {
				return status.Errorf(codes.Internal, "calculating pcm duration: %v", err).Err()
			}
			totalDuration += chunkDuration

			chunkIndex++
			var captureTime *timestamppb.Timestamp
			if chunkIndex == 1 {
				captureTime = timestamppb.Now()
			}
			if err := srv.Send(&aiservicepb.TextToSpeechStreamResponse{
				Content: &aiservicepb.TextToSpeechStreamResponse_AudioChunk{
					AudioChunk: &audiopb.Chunk{
						Index:       chunkIndex,
						CaptureTime: captureTime,
						Duration:    durationpb.New(chunkDuration),
						Data:        message.Audio,
					},
				},
			}); err != nil {
				return err
			}
			continue
		}

		switch message.Type {
		case SpeakMessageTypeError:
			return status.Errorf(codes.Internal, "%v", message.AsError()).Err()
		case SpeakMessageTypeSpeechMetadata:
			billableCharacterCount += message.BillableCharacterCount
		case SpeakMessageTypeSessionMetadata:
			// Final message of the session; prefer server-side billing totals.
			billableCharacterCount = message.TotalBillableCharacterCount
		default:
			// Connected, SpeechStarted, Flushed, Warning: no client action needed.
		}
		if message.Type == SpeakMessageTypeSessionMetadata {
			break
		}
	}

	generationMetrics.Ttlb = durationpb.New(time.Since(startTime))

	modelUsage := &aipb.ModelUsage{
		Model: request.Model,
		InputCharacter: &aipb.ResourceConsumption{
			Quantity: int32(billableCharacterCount),
		},
		OutputSecond: &aipb.ResourceConsumption{
			Quantity: int32(totalDuration.Seconds()),
		},
	}
	if err := srv.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_ModelUsage{ModelUsage: modelUsage},
	}); err != nil {
		return err
	}
	return srv.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_GenerationMetrics{GenerationMetrics: generationMetrics},
	})
}

var _ provider.TextToSpeechClient = (*Client)(nil)

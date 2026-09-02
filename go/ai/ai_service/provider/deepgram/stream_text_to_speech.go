package deepgram

import (
	"context"
	"errors"
	"io"
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

// StreamTextToSpeech implements bidirectional TTS via Deepgram Flux
// (/v2/speak): text deltas stream in, audio streams out, turn by turn.
func (c *Client) StreamTextToSpeech(
	configuration *aiservicepb.StreamTextToSpeechConfiguration,
	srv aiservicepb.AiService_StreamTextToSpeechServer,
) error {
	ctx := srv.Context()
	getModelRequest := &aiservicepb.GetModelRequest{Name: configuration.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return status.FromError(err, "getting model").Err()
	}

	// Flux embeds the voice in the model string (flux-{voice}-{language}), so
	// the provider voice id *is* the Flux model string.
	fluxModel := configuration.GetProviderVoiceId()
	if fluxModel == "" {
		fluxModel = model.ProviderModelId
	}

	// Resolve output format; honor the preferred sample rate when supported.
	audioFormat := model.Tts.AudioFormat
	preferredSampleRate := configuration.GetConfiguration().GetPreferredSampleRate()
	if preferredSampleRate > 0 && slices.Contains(model.Tts.SupportedSampleRates, preferredSampleRate) {
		audioFormat.SampleRate = preferredSampleRate
	}

	speakOptions := &SpeakOptions{
		Model:      fluxModel,
		Encoding:   EncodingLinear16,
		SampleRate: int(audioFormat.SampleRate),
	}
	if providerSettings := configuration.GetConfiguration().GetProviderSettings(); providerSettings != nil {
		fields := providerSettings.GetFields()
		if v, ok := fields["speed"]; ok {
			speakOptions.Speed = v.GetStringValue()
		}
		if v, ok := fields["expressivity"]; ok {
			speakOptions.Expressivity = v.GetStringValue()
		}
	}

	conn, err := c.Speak(ctx, speakOptions)
	if err != nil {
		return status.FromError(err, "connecting to deepgram speak").Err()
	}
	defer conn.Close()

	if err := srv.Send(&aiservicepb.StreamTextToSpeechResponse{
		Content: &aiservicepb.StreamTextToSpeechResponse_AudioFormat{AudioFormat: audioFormat},
	}); err != nil {
		return err
	}

	// Forward text/flush messages to Deepgram in the background; the main
	// goroutine owns the receive loop so it can emit usage after it ends.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	recvErrChan := make(chan error, 1)
	go func() { recvErrChan <- c.recvText(ctx, srv, conn) }()

	startTime := time.Now()
	generationMetrics := &aipb.GenerationMetrics{}
	var totalDuration time.Duration
	var billableCharacterCount int
	var chunkIndex uint32
	for {
		message, err := conn.ReceiveMessage(ctx)
		if err != nil {
			// The server closes normally after our Close message.
			var closeError websocket.CloseError
			if errors.As(err, &closeError) && closeError.Code == websocket.StatusNormalClosure {
				break
			}
			return status.FromError(err, "receiving speak message").Err()
		}

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
			if err := srv.Send(&aiservicepb.StreamTextToSpeechResponse{
				Content: &aiservicepb.StreamTextToSpeechResponse_AudioChunk{
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

		isDone := false
		switch message.Type {
		case SpeakMessageTypeError:
			return status.Errorf(codes.Internal, "%v", message.AsError()).Err()
		case SpeakMessageTypeSpeechMetadata:
			// End of a turn: forward billing/timing to the client.
			billableCharacterCount += message.BillableCharacterCount
			if err := srv.Send(&aiservicepb.StreamTextToSpeechResponse{
				Content: &aiservicepb.StreamTextToSpeechResponse_TurnComplete{
					TurnComplete: &aiservicepb.StreamTextToSpeechTurnComplete{
						AudioDuration:          durationpb.New(time.Duration(message.AudioDurationMs) * time.Millisecond),
						BillableCharacterCount: int32(message.BillableCharacterCount),
					},
				},
			}); err != nil {
				return err
			}
		case SpeakMessageTypeSpeechInterrupted:
			// Turn cut short: report exactly what was spoken so the caller
			// can reconcile its transcript.
			if err := srv.Send(&aiservicepb.StreamTextToSpeechResponse{
				Content: &aiservicepb.StreamTextToSpeechResponse_TurnInterrupted{
					TurnInterrupted: &aiservicepb.StreamTextToSpeechTurnInterrupted{
						TextSpoken:    message.TextSpoken,
						TextRemaining: message.TextRemaining,
					},
				},
			}); err != nil {
				return err
			}
		case SpeakMessageTypeSessionMetadata:
			// Final message of the session; prefer server-side billing totals.
			billableCharacterCount = message.TotalBillableCharacterCount
			isDone = true
		default:
			// Connected, SpeechStarted, Flushed, Warning: no client action needed.
		}
		if isDone {
			break
		}
	}

	// Surface any text-forwarding failure (a completed recv loop sends nil).
	cancel()
	if err := <-recvErrChan; err != nil && !errors.Is(err, context.Canceled) {
		return status.FromError(err, "forwarding text").Err()
	}

	generationMetrics.Ttlb = durationpb.New(time.Since(startTime))
	modelUsage := &aipb.ModelUsage{
		Model: configuration.Model,
		InputCharacter: &aipb.ResourceConsumption{
			Quantity: int32(billableCharacterCount),
		},
		OutputSecond: &aipb.ResourceConsumption{
			Quantity: int32(totalDuration.Seconds()),
		},
	}
	if err := srv.Send(&aiservicepb.StreamTextToSpeechResponse{
		Content: &aiservicepb.StreamTextToSpeechResponse_ModelUsage{ModelUsage: modelUsage},
	}); err != nil {
		return err
	}
	return srv.Send(&aiservicepb.StreamTextToSpeechResponse{
		Content: &aiservicepb.StreamTextToSpeechResponse_GenerationMetrics{GenerationMetrics: generationMetrics},
	})
}

// recvText forwards client text and flush messages to Deepgram. When the
// client half-closes, it asks Deepgram to end the session, which triggers
// SessionMetadata and unblocks the receive loop.
func (c *Client) recvText(ctx context.Context, srv aiservicepb.AiService_StreamTextToSpeechServer, conn *SpeakConnection) error {
	for {
		request, err := srv.Recv()
		if err == io.EOF {
			return conn.SendClose(ctx)
		}
		if err != nil {
			return err
		}
		switch content := request.Content.(type) {
		case *aiservicepb.StreamTextToSpeechRequest_Text:
			if err := conn.SendText(ctx, content.Text); err != nil {
				return err
			}
		case *aiservicepb.StreamTextToSpeechRequest_Flush:
			if err := conn.Flush(ctx); err != nil {
				return err
			}
		case *aiservicepb.StreamTextToSpeechRequest_Interrupt:
			if err := conn.Interrupt(ctx); err != nil {
				return err
			}
		}
	}
}

var _ provider.StreamTextToSpeechClient = (*Client)(nil)

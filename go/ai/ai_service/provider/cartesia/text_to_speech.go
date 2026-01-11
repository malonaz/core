package cartesia

import (
	"encoding/base64"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/malonaz/core/go/grpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/audio"
)

type TextToSpeechRequest struct {
	ModelID          string            `json:"model_id"`
	Transcript       string            `json:"transcript"`
	Voice            VoiceSpecifier    `json:"voice"`
	Language         string            `json:"language,omitempty"`
	OutputFormat     OutputFormat      `json:"output_format"`
	GenerationConfig *GenerationConfig `json:"generation_config,omitempty"`
	Continue         bool              `json:"continue,omitempty"`
	AddTimestamps    bool              `json:"add_timestamps,omitempty"`
}

type VoiceSpecifier struct {
	Mode string `json:"mode"`
	ID   string `json:"id"`
}

type OutputFormat struct {
	Container  string `json:"container"`
	Encoding   string `json:"encoding,omitempty"`
	SampleRate int32  `json:"sample_rate"`
}

type GenerationConfig struct {
	Volume  float32 `json:"volume,omitempty"`
	Speed   float32 `json:"speed,omitempty"`
	Emotion string  `json:"emotion,omitempty"`
}

type TextToSpeechResponse struct {
	Type           string          `json:"type"`
	Data           string          `json:"data,omitempty"`
	Done           bool            `json:"done"`
	StatusCode     int             `json:"status_code"`
	StepTime       float64         `json:"step_time,omitempty"`
	Error          string          `json:"error,omitempty"`
	FlushDone      bool            `json:"flush_done,omitempty"`
	FlushID        int             `json:"flush_id,omitempty"`
	WordTimestamps *WordTimestamps `json:"word_timestamps,omitempty"`
}

type WordTimestamps struct {
	Words []string  `json:"words"`
	Start []float64 `json:"start"`
	End   []float64 `json:"end"`
}

// TextToSpeechStream implements the exact gRPC server streaming interface using WebSocket
func (c *Client) TextToSpeechStream(
	request *aiservicepb.TextToSpeechStreamRequest,
	srv aiservicepb.AiService_TextToSpeechStreamServer,
) error {
	ctx := srv.Context()
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}

	// Send generation metrics
	startTime := time.Now()
	generationMetrics := &aipb.GenerationMetrics{}
	eg := errgroup.Group{}

	// Build the generation request
	audioFormat := model.Tts.AudioFormat
	preferredSampleRate := request.GetConfiguration().GetPreferredSampleRate()
	if preferredSampleRate > 0 {
		for _, supportedSampleRate := range model.Tts.SupportedSampleRates {
			if supportedSampleRate == preferredSampleRate {
				audioFormat.SampleRate = preferredSampleRate
				break
			}
		}
	}

	eg.Go(func() error {
		return srv.Send(&aiservicepb.TextToSpeechStreamResponse{
			Content: &aiservicepb.TextToSpeechStreamResponse_AudioFormat{
				AudioFormat: audioFormat,
			},
		})
	})

	textToSpeechRequest := &TextToSpeechRequest{
		ModelID:    model.ProviderModelId,
		Transcript: request.Text,
		Language:   request.GetConfiguration().GetLanguageCode(),
		Voice: VoiceSpecifier{
			Mode: "id",
			ID:   request.ProviderVoiceId,
		},
		OutputFormat: OutputFormat{
			Container:  "raw",
			Encoding:   "pcm_s16le", // 16-bit PCM
			SampleRate: audioFormat.SampleRate,
		},
		GenerationConfig: &GenerationConfig{
			Volume: 1,
			Speed:  1,
		},
		Continue:      false, // Single shot generation
		AddTimestamps: false, // Can be enabled if needed
	}

	stream := c.NewTextToSpeechStream()
	defer stream.Close()

	eg.Go(func() error { return stream.Send(textToSpeechRequest) })

	if err := eg.Wait(); err != nil {
		return err
	}

	// Process responses
	var totalDuration time.Duration
	var chunkIndex uint32
	var isDone bool
	for !isDone {
		// Handles context cancellation cleanly :).
		textToSpeechResponse, err := stream.Recv(ctx)
		if err != nil {
			return grpc.Errorf(codes.Internal, "reading from stream: %v", err).Err()
		}
		if textToSpeechResponse.Done {
			isDone = true
		}

		// Handle different response types
		switch textToSpeechResponse.Type {
		case "chunk":
			// Set TTFB on first chunk
			if generationMetrics.Ttfb == nil {
				generationMetrics.Ttfb = durationpb.New(time.Since(startTime))
			}

			// Decode base64 audio data
			audioData, err := base64.StdEncoding.DecodeString(textToSpeechResponse.Data)
			if err != nil {
				return fmt.Errorf("failed to decode audio data: %w", err)
			}
			if len(audioData) == 0 {
				continue
			}

			// Update total duration
			chunkDuration, err := audio.CalculatePCMDuration(audioFormat, len(audioData))
			if err != nil {
				return err
			}
			totalDuration += chunkDuration

			// Send audio chunk
			chunkIndex++
			var captureTime *timestamppb.Timestamp
			if chunkIndex == 1 {
				captureTime = timestamppb.Now()
			}
			audioChunk := &audiopb.Chunk{
				Index:       chunkIndex,
				CaptureTime: captureTime,
				Duration:    durationpb.New(chunkDuration),
				Data:        audioData,
			}

			if err := srv.Send(&aiservicepb.TextToSpeechStreamResponse{
				Content: &aiservicepb.TextToSpeechStreamResponse_AudioChunk{
					AudioChunk: audioChunk,
				},
			}); err != nil {
				return err
			}
		case "done":
			isDone = true

		case "error":
			return fmt.Errorf("cartesia error (status %d): %s",
				textToSpeechResponse.StatusCode, textToSpeechResponse.Error)

		case "timestamps":
			// Optionally handle word timestamps if needed
			continue

		case "flush_done":
			// Flush acknowledgment, continue reading
			continue

		default:
			// Unknown response type, log and continue
			fmt.Printf("Unknown WebSocket response type: %s\n", textToSpeechResponse.Type)
		}
	}

	generationMetrics.Ttlb = durationpb.New(time.Since(startTime))

	// Send model usage
	modelUsage := &aipb.ModelUsage{
		Model: request.Model,
		InputCharacter: &aipb.ResourceConsumption{
			Quantity: int32(utf8.RuneCountInString(request.Text)),
		},
		OutputSecond: &aipb.ResourceConsumption{
			Quantity: int32(totalDuration.Seconds()),
		},
	}
	if err := srv.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_ModelUsage{
			ModelUsage: modelUsage,
		},
	}); err != nil {
		return err
	}

	// Send generation metrics
	if err := srv.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_GenerationMetrics{
			GenerationMetrics: generationMetrics,
		},
	}); err != nil {
		return err
	}

	return nil
}

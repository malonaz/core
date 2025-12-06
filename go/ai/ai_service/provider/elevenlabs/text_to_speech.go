package elevenlabs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/audio"
)

type textToSpeechStreamRequest struct {
	Text          string `json:"text"`
	ModelID       string `json:"model_id"`
	LanguageCode  string `json:"language_code,omitempty"`
	VoiceSettings *struct {
		Stability       float32 `json:"stability,omitempty"`
		SimilarityBoost float32 `json:"similarity_boost,omitempty"`
	} `json:"voice_settings,omitempty"`
}

// TextToSpeechStream implements the exact gRPC server streaming interface
func (c *Client) TextToSpeechStream(
	request *aiservicepb.TextToSpeechStreamRequest,
	stream aiservicepb.Ai_TextToSpeechStreamServer,
) error {
	ctx := stream.Context()

	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}
	audioFormat := model.Tts.AudioFormat

	// Send generation metrics
	startTime := time.Now()
	generationMetrics := &aipb.GenerationMetrics{}

	// Build the request body
	textToSpeechStreamRequest := textToSpeechStreamRequest{
		Text:         request.Text,
		ModelID:      model.ProviderModelId,
		LanguageCode: request.GetConfiguration().GetLanguageCode(),
	}

	// Use the preferred sample rate if it is supported.
	preferredSampleRate := request.GetConfiguration().GetPreferredSampleRate()
	if preferredSampleRate > 0 {
		for _, supportedSampleRate := range model.Tts.SupportedSampleRates {
			if supportedSampleRate == preferredSampleRate {
				audioFormat.SampleRate = preferredSampleRate
				break
			}
		}
	}

	requestBody, err := json.Marshal(textToSpeechStreamRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create the HTTP request
	url := fmt.Sprintf("%s/text-to-speech/%s/stream", c.baseURL, request.ProviderVoiceId)

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	httpRequest.Header.Add("Accept", "audio/mpeg")
	httpRequest.Header.Add("Content-Type", "application/json")
	httpRequest.Header.Add("xi-api-key", c.apiKey)

	// Add query parameters for PCM format
	query := httpRequest.URL.Query()
	query.Add("output_format", fmt.Sprintf("pcm_%d", audioFormat.SampleRate))
	httpRequest.URL.RawQuery = query.Encode()

	// Execute the request
	response, err := c.client.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer response.Body.Close()

	// Check for errors
	if response.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(response.Body)
		return fmt.Errorf("unexpected HTTP status %d: %s", response.StatusCode, string(responseBody))
	}

	// Send audio format first
	if err := stream.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_AudioFormat{
			AudioFormat: audioFormat,
		},
	}); err != nil {
		return err
	}

	// Stream audio chunks
	buffer := make([]byte, 4096)
	var totalDuration time.Duration

	for {
		bytesRead, err := response.Body.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading response body: %w", err)
		}
		if bytesRead == 0 {
			continue
		}
		if generationMetrics.Ttfb == nil {
			generationMetrics.Ttfb = durationpb.New(time.Since(startTime))
		}

		// Update total duration
		duration, err := audio.CalculatePCMDuration(
			bytesRead,
			audioFormat.SampleRate,
			audioFormat.Channels,
			audioFormat.BitsPerSample,
		)
		if err != nil {
			return err
		}
		totalDuration += duration

		// Send audio chunk
		audioChunk := &audiopb.Chunk{
			Data: make([]byte, bytesRead),
		}
		copy(audioChunk.Data, buffer[:bytesRead])

		response := &aiservicepb.TextToSpeechStreamResponse{
			Content: &aiservicepb.TextToSpeechStreamResponse_AudioChunk{
				AudioChunk: audioChunk,
			},
		}
		if err := stream.Send(response); err != nil {
			return err
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
	if err := stream.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_ModelUsage{
			ModelUsage: modelUsage,
		},
	}); err != nil {
		return err
	}

	// Send generation metrics
	if err := stream.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_GenerationMetrics{
			GenerationMetrics: generationMetrics,
		},
	}); err != nil {
		return err
	}

	return nil
}

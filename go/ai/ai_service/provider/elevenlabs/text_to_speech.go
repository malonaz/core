package elevenlabs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/audio"
	"github.com/malonaz/core/go/grpc"
)

// Define protected keys that cannot be overridden by provider settings
var protectedKeySet = map[string]struct{}{
	"text":     {},
	"model_id": {},
}

// TextToSpeechStream implements the gRPC server streaming interface for text-to-speech conversion.
func (c *Client) TextToSpeechStream(
	request *aiservicepb.TextToSpeechStreamRequest,
	stream aiservicepb.Ai_TextToSpeechStreamServer,
) error {
	ctx := stream.Context()

	// Get model information
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}
	audioFormat := model.Tts.AudioFormat

	// Initialize metrics tracking
	startTime := time.Now()
	generationMetrics := &aipb.GenerationMetrics{}

	// Use the preferred sample rate if it is supported
	preferredSampleRate := request.GetConfiguration().GetPreferredSampleRate()
	if preferredSampleRate > 0 {
		for _, supportedSampleRate := range model.Tts.SupportedSampleRates {
			if supportedSampleRate == preferredSampleRate {
				audioFormat.SampleRate = preferredSampleRate
				break
			}
		}
	}

	// Build base request with non-overridable fields
	baseRequest := map[string]interface{}{
		"text":     request.Text,
		"model_id": model.ProviderModelId,
	}

	// Add language code if provided (can be overridden by provider settings)
	if languageCode := request.GetConfiguration().GetLanguageCode(); languageCode != "" {
		baseRequest["language_code"] = languageCode
	}

	// Merge provider settings using AsMap()
	if providerSettings := request.GetConfiguration().GetProviderSettings(); providerSettings != nil {
		providerMap := providerSettings.AsMap()
		for key, value := range providerMap {
			if _, ok := protectedKeySet[key]; ok {
				return grpc.Errorf(codes.InvalidArgument, "cannot set protected provider setting key %s", key).Err()
			}
			baseRequest[key] = value
		}
	}

	// Marshal the final request body
	requestBody, err := json.Marshal(baseRequest)
	if err != nil {
		return grpc.Errorf(codes.InvalidArgument, "marshaling request: %v", err).Err()
	}

	// Create the HTTP request
	url := fmt.Sprintf("%s/text-to-speech/%s/stream", c.baseURL, request.ProviderVoiceId)

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(requestBody))
	if err != nil {
		return grpc.Errorf(codes.Internal, "creating HTTP request: %v", err).Err()
	}

	// Set headers
	httpRequest.Header.Add("Accept", "audio/mpeg")
	httpRequest.Header.Add("Content-Type", "application/json")
	httpRequest.Header.Add("xi-api-key", c.apiKey)

	// Add query parameters for PCM format
	query := httpRequest.URL.Query()
	query.Add("output_format", fmt.Sprintf("pcm_%d", audioFormat.SampleRate))
	httpRequest.URL.RawQuery = query.Encode()

	// Execute the HTTP request
	response, err := c.client.Do(httpRequest)
	if err != nil {
		return grpc.Errorf(codes.Unavailable, "executing HTTP request: %v", err).Err()
	}
	defer response.Body.Close()

	// Check for HTTP errors
	if response.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(response.Body)
		code := grpc.CodeFromHTTPStatus(response.StatusCode)
		return grpc.Errorf(code, "HTTP status %d: %s", response.StatusCode, string(responseBody)).Err()
	}

	// Send audio format first
	if err := stream.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_AudioFormat{
			AudioFormat: audioFormat,
		},
	}); err != nil {
		return grpc.Errorf(codes.Internal, "sending audio format: %v", err).Err()
	}

	// Stream audio chunks
	buffer := make([]byte, 4096)
	var totalDuration time.Duration
	var chunkIndex uint32
	for {
		bytesRead, err := response.Body.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return grpc.Errorf(codes.Internal, "reading response body: %v", err).Err()
		}
		if bytesRead == 0 {
			continue
		}

		// Record time to first byte
		if generationMetrics.Ttfb == nil {
			generationMetrics.Ttfb = durationpb.New(time.Since(startTime))
		}

		// Calculate audio duration for this chunk
		chunkDuration, err := audio.CalculatePCMDuration(
			bytesRead,
			audioFormat.SampleRate,
			audioFormat.Channels,
			audioFormat.BitsPerSample,
		)
		if err != nil {
			return err
		}
		totalDuration += chunkDuration

		// Create and send audio chunk
		chunkIndex++
		var captureTime *timestamppb.Timestamp
		if chunkIndex == 1 {
			captureTime = timestamppb.Now()
		}
		audioChunk := &audiopb.Chunk{
			Index:       chunkIndex,
			CaptureTime: captureTime,
			Duration:    durationpb.New(chunkDuration),
			Data:        make([]byte, bytesRead),
		}
		copy(audioChunk.Data, buffer[:bytesRead])

		if err := stream.Send(&aiservicepb.TextToSpeechStreamResponse{
			Content: &aiservicepb.TextToSpeechStreamResponse_AudioChunk{
				AudioChunk: audioChunk,
			},
		}); err != nil {
			return grpc.Errorf(codes.Internal, "sending audio chunk: %v", err).Err()
		}
	}

	// Record time to last byte
	generationMetrics.Ttlb = durationpb.New(time.Since(startTime))

	// Send model usage metrics
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
		return grpc.Errorf(codes.Internal, "sending model usage: %v", err).Err()
	}

	// Send generation metrics
	if err := stream.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_GenerationMetrics{
			GenerationMetrics: generationMetrics,
		},
	}); err != nil {
		return grpc.Errorf(codes.Internal, "sending generation metrics: %v", err).Err()
	}

	return nil
}

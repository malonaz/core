package openai

import (
	"fmt"
	"io"
	"time"

	openai "github.com/sashabaranov/go-openai"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/audio"
	"unicode/utf8"
)

// TextToSpeechStream implements the exact gRPC server streaming interface
func (c *Client) TextToSpeechStream(request *aiservicepb.TextToSpeechStreamRequest, stream aiservicepb.Ai_TextToSpeechStreamServer) error {
	ctx := stream.Context()
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}

	// Send generation metrics
	startTime := time.Now()
	generationMetrics := &aipb.GenerationMetrics{}

	// Try to use PCM => fallback to WAV.
	responseFormat := openai.SpeechResponseFormatPcm
	if !c.config.PcmSupport {
		responseFormat = openai.SpeechResponseFormatWav
	}

	createSpeechRequest := openai.CreateSpeechRequest{
		Model:          openai.SpeechModel(model.ProviderModelId),
		Input:          request.Text,
		Voice:          openai.SpeechVoice(request.ProviderVoiceId),
		ResponseFormat: responseFormat,
	}

	response, err := c.client.CreateSpeech(ctx, createSpeechRequest)
	if err != nil {
		return fmt.Errorf("TTS request failed: %w", err)
	}
	defer response.Close()

	// Send audio format first
	audioFormat := model.Tts.AudioFormat
	if err := stream.Send(&aiservicepb.TextToSpeechStreamResponse{
		Content: &aiservicepb.TextToSpeechStreamResponse_AudioFormat{
			AudioFormat: audioFormat,
		},
	}); err != nil {
		return err
	}

	// Skip WAV header if needed
	skipWavHeader := !c.config.PcmSupport
	if skipWavHeader {
		header := make([]byte, 44)
		if _, err := io.ReadFull(response, header); err != nil {
			return fmt.Errorf("error reading WAV header: %w", err)
		}
	}

	// Stream audio chunks
	buffer := make([]byte, 4096)
	var remainder []byte
	var totalDuration time.Duration
	var chunkIndex uint32

	for {
		bytesRead, err := response.Read(buffer)
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

		// Combine with remainder
		totalData := append(remainder, buffer[:bytesRead]...)
		completeBytes := (len(totalData) / 2) * 2
		if completeBytes > 0 {
			chunkDuration, err := audio.CalculatePCMDuration(
				completeBytes, audioFormat.SampleRate, audioFormat.Channels, audioFormat.BitsPerSample,
			)
			if err != nil {
				return err
			}
			totalDuration += chunkDuration

			chunkIndex++
			var captureTime *timestamppb.Timestamp
			if chunkIndex == 1 {
				captureTime = timestamppb.Now()
			}
			audioChunk := &audiopb.Chunk{
				Index:       chunkIndex,
				CaptureTime: captureTime,
				Duration:    durationpb.New(chunkDuration),
				Data:        make([]byte, completeBytes),
			}
			copy(audioChunk.Data, totalData[:completeBytes])
			remainder = totalData[completeBytes:]

			if err := stream.Send(&aiservicepb.TextToSpeechStreamResponse{
				Content: &aiservicepb.TextToSpeechStreamResponse_AudioChunk{
					AudioChunk: audioChunk,
				},
			}); err != nil {
				return err
			}
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

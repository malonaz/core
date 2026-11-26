package deepgram

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/audio"
	"github.com/malonaz/core/go/grpc/status"
)

const defaultRESTBaseURL = "https://api.deepgram.com"

// prerecordedResponse is the subset of the /v1/listen response read here.
type prerecordedResponse struct {
	Metadata struct {
		Duration float64 `json:"duration"`
	} `json:"metadata"`
	Results struct {
		Channels []struct {
			Alternatives []struct {
				Transcript string `json:"transcript"`
			} `json:"alternatives"`
		} `json:"channels"`
		Utterances []struct {
			Start      float64 `json:"start"`
			End        float64 `json:"end"`
			Transcript string  `json:"transcript"`
			Speaker    *int32  `json:"speaker"`
		} `json:"utterances"`
	} `json:"results"`
}

// SpeechToText transcribes a whole recording with the pre-recorded API (/v1/listen), with speakers
// and utterance offsets.
func (c *Client) SpeechToText(ctx context.Context, request *aiservicepb.SpeechToTextRequest) (*aiservicepb.SpeechToTextResponse, error) {
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.GetModel()}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return nil, status.FromError(err, "getting model").Err()
	}

	audioFormat := request.GetAudioFormat()
	if audioFormat == nil {
		return nil, status.Errorf(codes.InvalidArgument, "audio_format is required").Err()
	}
	var body []byte
	var mimeType string
	switch encoding := audioFormat.GetEncoding(); encoding {
	case audiopb.Encoding_ENCODING_MP3:
		body, mimeType = request.GetAudioChunk().GetData(), "audio/mpeg"
	case audiopb.Encoding_ENCODING_UNSPECIFIED:
		body, mimeType = audio.PCMToWAV(request.GetAudioChunk().GetData(), audioFormat), "audio/wav"
	default:
		return nil, status.Errorf(codes.InvalidArgument, "deepgram speech to text does not take %s", encoding).Err()
	}

	params := url.Values{}
	params.Set("model", model.GetProviderModelId())
	params.Set("diarize", "true")
	params.Set("utterances", "true")
	params.Set("punctuate", "true")
	params.Set("smart_format", "true")
	if request.GetLanguageCode() != "" {
		params.Set("language", request.GetLanguageCode())
	}
	for _, keyterm := range request.GetKeyterms() {
		params.Add("keyterm", keyterm)
	}
	// Deepgram documents %20 for spaces in a keyterm; Encode writes '+' and escapes a literal one.
	listenURL := fmt.Sprintf("%s/v1/listen?%s", c.restBaseURL, strings.ReplaceAll(params.Encode(), "+", "%20"))
	headers := map[string]string{"Authorization": fmt.Sprintf("token %s", c.apiKey)}

	startTime := time.Now()
	var prerecorded prerecordedResponse
	if err := provider.Post(ctx, listenURL, mimeType, headers, bytes.NewReader(body), &prerecorded); err != nil {
		return nil, err
	}

	response := &aiservicepb.SpeechToTextResponse{
		ModelUsage: &aipb.ModelUsage{
			Model: request.GetModel(),
			InputSecond: &aipb.ResourceConsumption{
				Quantity: int64(math.Round(prerecorded.Metadata.Duration)),
			},
		},
		GenerationMetrics: &aipb.GenerationMetrics{Ttlb: durationpb.New(time.Since(startTime))},
	}
	if len(prerecorded.Results.Channels) > 0 && len(prerecorded.Results.Channels[0].Alternatives) > 0 {
		response.Transcript = strings.TrimSpace(prerecorded.Results.Channels[0].Alternatives[0].Transcript)
	}
	for _, utterance := range prerecorded.Results.Utterances {
		response.Segments = append(response.Segments, &aiservicepb.SpeechToTextSegment{
			Speaker:     utterance.Speaker,
			StartOffset: durationpb.New(secondsToDuration(utterance.Start)),
			EndOffset:   durationpb.New(secondsToDuration(utterance.End)),
			Text:        strings.TrimSpace(utterance.Transcript),
		})
	}
	return response, nil
}

func secondsToDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

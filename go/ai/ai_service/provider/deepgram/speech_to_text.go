package deepgram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/audio"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	defaultRESTBaseURL = "https://api.deepgram.com"
	maxErrorBodyBytes  = 512
)

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
	listenURL := fmt.Sprintf("%s/v1/listen?%s", c.restBaseURL, params.Encode())
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, listenURL, bytes.NewReader(body))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "creating deepgram request: %v", err).Err()
	}
	httpRequest.Header.Set("Authorization", fmt.Sprintf("token %s", c.apiKey))
	httpRequest.Header.Set("Content-Type", mimeType)

	startTime := time.Now()
	httpResponse, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		return nil, status.FromError(err, "calling deepgram").Err()
	}
	defer httpResponse.Body.Close()
	responseBody, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		return nil, status.FromError(err, "reading deepgram response").Err()
	}
	if httpResponse.StatusCode != http.StatusOK {
		return nil, status.Errorf(httpStatusToCode(httpResponse.StatusCode), "deepgram returned %d: %s", httpResponse.StatusCode, truncate(responseBody, maxErrorBodyBytes)).Err()
	}
	var prerecorded prerecordedResponse
	if err := json.Unmarshal(responseBody, &prerecorded); err != nil {
		return nil, status.Errorf(codes.Internal, "parsing deepgram response: %v", err).Err()
	}

	response := &aiservicepb.SpeechToTextResponse{
		ModelUsage: &aipb.ModelUsage{
			Model: request.GetModel(),
			InputSecond: &aipb.ResourceConsumption{
				Quantity: int64(time.Duration(prerecorded.Metadata.Duration * float64(time.Second)).Round(time.Second).Seconds()),
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
			StartOffset: durationpb.New(time.Duration(utterance.Start * float64(time.Second))),
			EndOffset:   durationpb.New(time.Duration(utterance.End * float64(time.Second))),
			Text:        strings.TrimSpace(utterance.Transcript),
		})
	}
	return response, nil
}

func truncate(body []byte, limit int) string {
	if len(body) <= limit {
		return string(body)
	}
	return string(body[:limit]) + "..."
}

// httpStatusToCode maps a Deepgram failure to the caller's view of it: a rejected key or plan is our
// configuration, not the caller's.
func httpStatusToCode(httpStatus int) codes.Code {
	switch {
	case httpStatus == http.StatusBadRequest, httpStatus == http.StatusRequestEntityTooLarge:
		return codes.InvalidArgument
	case httpStatus == http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case httpStatus >= 500:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}

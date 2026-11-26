package deepgram

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

const prerecordedBody = `{
  "metadata": {"duration": 12.4},
  "results": {
    "channels": [{"alternatives": [{"transcript": "Thanks for calling. Hi there."}]}],
    "utterances": [
      {"start": 0.5, "end": 2.25, "transcript": "Thanks for calling.", "speaker": 0},
      {"start": 3.0, "end": 4.5, "transcript": "Hi there.", "speaker": 1}
    ]
  }
}`

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	modelService, err := provider.NewModelService()
	require.NoError(t, err)
	client := NewClient("test-key", modelService)
	client.restBaseURL = server.URL
	require.NoError(t, modelService.RegisterProvider(context.Background(), client))
	return client
}

func TestSpeechToText(t *testing.T) {
	var gotQuery, gotAuthorization, gotContentType string
	var gotBody []byte
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery, gotAuthorization, gotContentType = r.URL.RawQuery, r.Header.Get("Authorization"), r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(prerecordedBody))
	})

	request := &aiservicepb.SpeechToTextRequest{
		Model:        "providers/deepgram/models/nova-3",
		EncodedAudio: &aiservicepb.SpeechToTextEncodedAudio{MimeType: "audio/mpeg", Data: []byte("mp3-bytes")},
	}
	response, err := client.SpeechToText(context.Background(), request)
	require.NoError(t, err)

	expected := &aiservicepb.SpeechToTextResponse{
		Transcript: "Thanks for calling. Hi there.",
		ModelUsage: &aipb.ModelUsage{
			Model:       "providers/deepgram/models/nova-3",
			InputSecond: &aipb.ResourceConsumption{Quantity: 12},
		},
		GenerationMetrics: &aipb.GenerationMetrics{},
		Segments: []*aiservicepb.SpeechToTextSegment{
			{Speaker: proto.Int32(0), StartOffset: durationpb.New(500_000_000), EndOffset: durationpb.New(2_250_000_000), Text: "Thanks for calling."},
			{Speaker: proto.Int32(1), StartOffset: durationpb.New(3_000_000_000), EndOffset: durationpb.New(4_500_000_000), Text: "Hi there."},
		},
	}
	grpcrequire.Equal(t, expected, response, protocmp.IgnoreFields(&aipb.GenerationMetrics{}, "ttlb"))
	require.Equal(t, "diarize=true&model=nova-3&punctuate=true&smart_format=true&utterances=true", gotQuery)
	require.Equal(t, "token test-key", gotAuthorization)
	require.Equal(t, "audio/mpeg", gotContentType)
	require.Equal(t, []byte("mp3-bytes"), gotBody)
}

func TestSpeechToTextError(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad audio", http.StatusBadRequest)
	})
	request := &aiservicepb.SpeechToTextRequest{
		Model:        "providers/deepgram/models/nova-3",
		EncodedAudio: &aiservicepb.SpeechToTextEncodedAudio{MimeType: "audio/mpeg", Data: []byte("x")},
	}
	_, err := client.SpeechToText(context.Background(), request)
	grpcrequire.Error(t, codes.InvalidArgument, err)
}

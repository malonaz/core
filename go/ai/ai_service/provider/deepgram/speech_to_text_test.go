package deepgram

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/audio"
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
		Model:       "providers/deepgram/models/nova-3",
		AudioFormat: &audiopb.Format{Encoding: audiopb.Encoding_ENCODING_MP3},
		AudioChunk:  &audiopb.Chunk{Index: 1, CaptureTime: timestamppb.Now(), Duration: durationpb.New(12400 * time.Millisecond), Data: []byte("mp3-bytes")},
		Keyterms:    []string{"Hearth", "Beatriz Tello", "C++"},
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
			{Speaker: proto.Int32(0), StartOffset: durationpb.New(500 * time.Millisecond), EndOffset: durationpb.New(2250 * time.Millisecond), Text: "Thanks for calling."},
			{Speaker: proto.Int32(1), StartOffset: durationpb.New(3 * time.Second), EndOffset: durationpb.New(4500 * time.Millisecond), Text: "Hi there."},
		},
	}
	grpcrequire.Equal(t, expected, response, protocmp.IgnoreFields(&aipb.GenerationMetrics{}, "ttlb"))
	require.Equal(t, "diarize=true&keyterm=Hearth&keyterm=Beatriz%20Tello&keyterm=C%2B%2B&model=nova-3&punctuate=true&smart_format=true&utterances=true", gotQuery)
	require.Equal(t, "token test-key", gotAuthorization)
	require.Equal(t, "audio/mpeg", gotContentType)
	require.Equal(t, []byte("mp3-bytes"), gotBody)
}

func TestSpeechToTextError(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad audio", http.StatusBadRequest)
	})
	request := &aiservicepb.SpeechToTextRequest{
		Model:       "providers/deepgram/models/nova-3",
		AudioFormat: &audiopb.Format{Encoding: audiopb.Encoding_ENCODING_MP3},
		AudioChunk:  &audiopb.Chunk{Index: 1, CaptureTime: timestamppb.Now(), Duration: durationpb.New(time.Second), Data: []byte("x")},
	}
	_, err := client.SpeechToText(context.Background(), request)
	grpcrequire.Error(t, codes.InvalidArgument, err)
}

func TestSpeechToTextPCM(t *testing.T) {
	var gotContentType string
	var gotBody []byte
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(prerecordedBody))
	})
	audioFormat := &audiopb.Format{SampleRate: 8000, Channels: 1, BitsPerSample: 16}
	pcm := []byte{1, 2, 3, 4}
	request := &aiservicepb.SpeechToTextRequest{
		Model:       "providers/deepgram/models/nova-3",
		AudioFormat: audioFormat,
		AudioChunk:  &audiopb.Chunk{Index: 1, CaptureTime: timestamppb.Now(), Duration: durationpb.New(time.Second), Data: pcm},
	}
	_, err := client.SpeechToText(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "audio/wav", gotContentType)
	require.Equal(t, audio.PCMToWAV(pcm, audioFormat), gotBody)
}

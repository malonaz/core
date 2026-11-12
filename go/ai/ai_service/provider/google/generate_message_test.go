package google

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
)

const testModel = "providers/google-vertex/models/gemini-3.8-flash"

// fakeVertex answers every request with the given stream chunks (each the JSON of one
// GenerateContentResponse, as Vertex sends them) and keeps the request's body.
type fakeVertex struct {
	chunks      []string
	requestBody []byte
}

func (f *fakeVertex) RoundTrip(request *http.Request) (*http.Response, error) {
	requestBody, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	f.requestBody = requestBody
	var body strings.Builder
	for _, chunk := range f.chunks {
		body.WriteString("data: " + chunk + "\n\n")
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body.String())),
		Request:    request,
	}, nil
}

type testMessageStream struct {
	ctx       context.Context
	mutex     sync.Mutex
	responses []*aiservicepb.StreamGenerateMessageResponse
}

func (s *testMessageStream) Send(response *aiservicepb.StreamGenerateMessageResponse) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.responses = append(s.responses, response)
	return nil
}

func (s *testMessageStream) Context() context.Context { return s.ctx }

// generate runs one generation against the fake and returns what the provider streamed out.
func generate(t *testing.T, vertex *fakeVertex, tools ...*aipb.Tool) ([]*aiservicepb.StreamGenerateMessageResponse, error) {
	t.Helper()
	ctx := context.Background()
	modelService, err := provider.NewModelService()
	require.NoError(t, err)
	client := NewVertexClient(&Opts{Project: "project", Location: "global"}, modelService)
	client.config.HTTPClient = &http.Client{Transport: vertex}
	require.NoError(t, client.Start(ctx))
	require.NoError(t, modelService.RegisterProvider(ctx, client))

	request := &aiservicepb.GenerateMessageRequest{
		Model:         testModel,
		Tools:         tools,
		Configuration: &aiservicepb.MessageGenerationConfiguration{StreamPartialToolCalls: true},
	}
	stream := &testMessageStream{ctx: ctx}
	sender := provider.NewAsyncMessageContentSender(stream, 100)
	generationError := client.StreamGenerateMessage(ctx, request, []*aipb.Message{ai.NewUserMessage(ai.NewTextBlock("hello"))}, sender)
	sender.Close()
	require.NoError(t, sender.Wait(ctx))
	return stream.responses, generationError
}

func TestToolSchemaKeywords(t *testing.T) {
	tool := &aipb.Tool{
		Name: "CreateReport",
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
				"title": {Type: "string", MaxLength: 256},
				"tags":  {Type: "array", Items: &jsonpb.Schema{Type: "string"}, MinItems: 1},
				"labels": {
					Type:                 "object",
					AdditionalProperties: &jsonpb.Schema{Type: "string", MaxLength: 63},
				},
			},
		},
	}
	vertex := &fakeVertex{chunks: []string{`{"candidates":[{"content":{"role":"model","parts":[{"text":"hi"}]},"finishReason":"STOP"}]}`}}
	_, err := generate(t, vertex, tool)
	require.NoError(t, err)

	var request struct {
		Tools []struct {
			FunctionDeclarations []struct {
				ParametersJSONSchema map[string]any `json:"parametersJsonSchema"`
			} `json:"functionDeclarations"`
		} `json:"tools"`
	}
	require.NoError(t, json.Unmarshal(vertex.requestBody, &request))
	properties := request.Tools[0].FunctionDeclarations[0].ParametersJSONSchema["properties"].(map[string]any)
	require.Equal(t, map[string]any{"type": "string", "maxLength": float64(256)}, properties["title"])
	require.Equal(t, map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "minItems": float64(1)}, properties["tags"])
	require.Equal(t, map[string]any{
		"type":                 "object",
		"additionalProperties": map[string]any{"type": "string", "maxLength": float64(63)},
	}, properties["labels"])
}

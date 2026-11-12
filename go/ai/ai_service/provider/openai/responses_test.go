package openai

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/openai/openai-go/v3/responses"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func TestNewResponseParams(t *testing.T) {
	model := &aipb.Model{
		ProviderModelId: "gpt-6-sol",
		Ttt:             &aipb.TttModelConfig{Reasoning: true, ToolCall: true},
		ProviderSettings: &structpb.Struct{Fields: map[string]*structpb.Value{
			"REASONING_EFFORT_UNSPECIFIED": structpb.NewStringValue("none"),
			"REASONING_EFFORT_XHIGH":       structpb.NewStringValue("xhigh"),
		}},
	}
	arguments, err := structpb.NewStruct(map[string]any{"unit": "celsius", "city": "Paris"})
	require.NoError(t, err)
	structuredContent, err := structpb.NewValue(map[string]any{"temperature": 21})
	require.NoError(t, err)
	toolCall := &aipb.ToolCall{Id: "call_1", Name: "get_weather", Arguments: arguments}
	messages := []*aipb.Message{
		ai.NewSystemMessage(ai.NewTextBlock("You are terse."), ai.NewTextBlock("Answer in French.")),
		ai.NewUserMessage(
			ai.NewTextBlock("What is the weather where this was taken?"),
			ai.NewImageBlock(&aipb.Image{Source: &aipb.Image_Data{Data: []byte("png")}, MediaType: "image/png", Quality: aipb.ImageQuality_IMAGE_QUALITY_LOW}),
			ai.NewImageBlock(ai.NewImageFromURL("https://example.com/photo.jpg")),
			ai.NewDocumentBlock(&aipb.Document{Source: &aipb.Document_Data{Data: []byte("pdf")}, MediaType: "application/pdf", Title: "trip.pdf"}),
			ai.NewDocumentBlock(ai.NewDocumentFromURL("https://example.com/trip.pdf", "application/pdf")),
		),
		// A turn generated here: the reasoning item goes back with its encrypted content and no summary.
		ai.NewAssistantMessage(
			&aipb.Block{Signature: "encrypted-reasoning", ExtraFields: newExtraFields(extraFieldID, "rs_1"), Content: &aipb.Block_Thought{Thought: "Looking up Paris."}},
			&aipb.Block{ExtraFields: newExtraFields(extraFieldPhase, "commentary"), Content: &aipb.Block_Text{Text: "Let me check."}},
			ai.NewToolCallBlock(toolCall),
		),
		ai.NewToolMessage(ai.NewToolResultBlock(ai.NewStructuredToolResult("get_weather", "call_1", structuredContent))),
		// A turn generated through Chat Completions or by another provider: no reasoning to send back.
		ai.NewAssistantMessage(ai.NewThoughtBlock("It is sunny."), ai.NewTextBlock("Il fait 21 degrés.")),
		ai.NewUserMessage(ai.NewTextBlock("Et à Lyon ?")),
	}
	request := &aiservicepb.GenerateMessageRequest{
		Tools: []*aipb.Tool{{
			Name:        "get_weather",
			Description: "Returns the weather of a city.",
			JsonSchema: &jsonpb.Schema{
				Type:       "object",
				Properties: map[string]*jsonpb.Schema{"city": {Type: "string"}},
				Required:   []string{"city"},
			},
		}},
		Configuration: &aiservicepb.MessageGenerationConfiguration{
			MaxTokens:       4096,
			ReasoningEffort: aipb.ReasoningEffort_REASONING_EFFORT_XHIGH,
			ToolChoice:      &aipb.ToolChoice{Choice: &aipb.ToolChoice_ToolName{ToolName: "get_weather"}},
		},
	}

	params, err := newResponseParams(model, request, messages)
	require.NoError(t, err)
	paramsBytes, err := json.Marshal(params)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"model": "gpt-6-sol",
		"store": false,
		"include": ["reasoning.encrypted_content"],
		"max_output_tokens": 4096,
		"reasoning": {"effort": "xhigh", "summary": "auto"},
		"input": [
			{"role": "system", "content": "You are terse.\nAnswer in French."},
			{"role": "user", "content": [
				{"type": "input_text", "text": "What is the weather where this was taken?"},
				{"type": "input_image", "image_url": "data:image/png;base64,cG5n", "detail": "low"},
				{"type": "input_image", "image_url": "https://example.com/photo.jpg", "detail": "auto"},
				{"type": "input_file", "file_data": "data:application/pdf;base64,cGRm", "filename": "trip.pdf"},
				{"type": "input_file", "file_url": "https://example.com/trip.pdf"}
			]},
			{"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "encrypted-reasoning"},
			{"role": "assistant", "content": "Let me check.", "phase": "commentary"},
			{"type": "function_call", "call_id": "call_1", "name": "get_weather", "arguments": "{\"city\":\"Paris\",\"unit\":\"celsius\"}"},
			{"type": "function_call_output", "call_id": "call_1", "output": "{\"temperature\":21}"},
			{"role": "assistant", "content": "Il fait 21 degrés."},
			{"role": "user", "content": [{"type": "input_text", "text": "Et à Lyon ?"}]}
		],
		"tools": [{
			"type": "function",
			"name": "get_weather",
			"description": "Returns the weather of a city.",
			"parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
			"strict": false
		}],
		"tool_choice": {"type": "function", "name": "get_weather"}
	}`, string(paramsBytes))

	// Unspecified runs at the effort the model's settings give it, and a level they leave out at the API's.
	request = &aiservicepb.GenerateMessageRequest{Configuration: &aiservicepb.MessageGenerationConfiguration{Temperature: 0.5}}
	params, err = newResponseParams(model, request, nil)
	require.NoError(t, err)
	paramsBytes, err = json.Marshal(params)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"model": "gpt-6-sol",
		"store": false,
		"include": ["reasoning.encrypted_content"],
		"temperature": 0.5,
		"reasoning": {"effort": "none", "summary": "auto"}
	}`, string(paramsBytes))

	request.Configuration = &aiservicepb.MessageGenerationConfiguration{ReasoningEffort: aipb.ReasoningEffort_REASONING_EFFORT_LOW}
	params, err = newResponseParams(model, request, nil)
	require.NoError(t, err)
	require.Empty(t, params.Reasoning.Effort)

	// A model that does not reason is sent no reasoning settings.
	params, err = newResponseParams(&aipb.Model{ProviderModelId: "gpt-plain", Ttt: &aipb.TttModelConfig{}}, request, nil)
	require.NoError(t, err)
	paramsBytes, err = json.Marshal(params)
	require.NoError(t, err)
	require.JSONEq(t, `{"model": "gpt-plain", "store": false, "include": ["reasoning.encrypted_content"]}`, string(paramsBytes))
}

// messageStream collects what a provider sends.
type messageStream struct {
	responses []*aiservicepb.StreamGenerateMessageResponse
}

func (s *messageStream) Send(response *aiservicepb.StreamGenerateMessageResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func (s *messageStream) Context() context.Context { return context.Background() }

// readResponse reads the events of a streamed response as the provider does.
func readResponse(t *testing.T, request *aiservicepb.GenerateMessageRequest, events ...string) (*messageStream, error) {
	t.Helper()
	ctx := context.Background()
	stream := &messageStream{}
	sender := provider.NewAsyncMessageContentSender(stream, 0)
	reader := newResponseReader(request, sender)
	var readError error
	for _, eventJSON := range events {
		var event responses.ResponseStreamEventUnion
		require.NoError(t, json.Unmarshal([]byte(eventJSON), &event))
		if readError = reader.read(ctx, event); readError != nil {
			break
		}
	}
	sender.Close()
	require.NoError(t, sender.Wait(ctx))
	return stream, readError
}

func TestResponseReader(t *testing.T) {
	request := &aiservicepb.GenerateMessageRequest{
		Model:         "providers/openai/models/gpt-6-sol",
		Configuration: &aiservicepb.MessageGenerationConfiguration{StreamPartialToolCalls: true},
	}
	stream, err := readResponse(t, request,
		`{"type": "response.created", "sequence_number": 0, "response": {"id": "resp_1", "status": "in_progress"}}`,
		`{"type": "response.output_item.added", "output_index": 0, "item": {"id": "rs_1", "type": "reasoning", "summary": []}}`,
		`{"type": "response.reasoning_summary_part.added", "item_id": "rs_1", "output_index": 0, "summary_index": 0, "part": {"type": "summary_text", "text": ""}}`,
		`{"type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "output_index": 0, "summary_index": 0, "delta": "Looking up "}`,
		`{"type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "output_index": 0, "summary_index": 0, "delta": "Paris."}`,
		`{"type": "response.reasoning_summary_part.added", "item_id": "rs_1", "output_index": 0, "summary_index": 1, "part": {"type": "summary_text", "text": ""}}`,
		`{"type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "output_index": 0, "summary_index": 1, "delta": "Then Tokyo."}`,
		`{"type": "response.output_item.done", "output_index": 0, "item": {"id": "rs_1", "type": "reasoning", "encrypted_content": "encrypted-reasoning", "summary": [{"type": "summary_text", "text": "Looking up Paris."}, {"type": "summary_text", "text": "Then Tokyo."}]}}`,
		`{"type": "response.output_item.added", "output_index": 1, "item": {"id": "msg_1", "type": "message", "status": "in_progress", "content": [], "phase": "commentary", "role": "assistant"}}`,
		`{"type": "response.output_text.delta", "item_id": "msg_1", "output_index": 1, "content_index": 0, "delta": "Let me "}`,
		`{"type": "response.output_text.delta", "item_id": "msg_1", "output_index": 1, "content_index": 0, "delta": "check."}`,
		`{"type": "response.output_item.done", "output_index": 1, "item": {"id": "msg_1", "type": "message", "status": "completed", "content": [{"type": "output_text", "text": "Let me check."}], "phase": "commentary", "role": "assistant"}}`,
		`{"type": "response.output_item.added", "output_index": 2, "item": {"id": "fc_1", "type": "function_call", "status": "in_progress", "arguments": "", "call_id": "call_1", "name": "get_weather"}}`,
		`{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "output_index": 2, "delta": "{\"city\":"}`,
		`{"type": "response.output_item.added", "output_index": 3, "item": {"id": "fc_2", "type": "function_call", "status": "in_progress", "arguments": "", "call_id": "call_2", "name": "get_weather"}}`,
		`{"type": "response.function_call_arguments.delta", "item_id": "fc_2", "output_index": 3, "delta": "{\"city\":\"Tokyo\"}"}`,
		`{"type": "response.function_call_arguments.delta", "item_id": "fc_1", "output_index": 2, "delta": "\"Paris\"}"}`,
		`{"type": "response.output_item.done", "output_index": 2, "item": {"id": "fc_1", "type": "function_call", "status": "completed", "arguments": "{\"city\":\"Paris\"}", "call_id": "call_1", "name": "get_weather"}}`,
		`{"type": "response.output_item.done", "output_index": 3, "item": {"id": "fc_2", "type": "function_call", "status": "completed", "arguments": "{\"city\":\"Tokyo\"}", "call_id": "call_2", "name": "get_weather"}}`,
		`{"type": "response.completed", "response": {"id": "resp_1", "status": "completed", "usage": {
			"input_tokens": 2000,
			"input_tokens_details": {"cached_tokens": 1024, "cache_write_tokens": 512},
			"output_tokens": 300,
			"output_tokens_details": {"reasoning_tokens": 256},
			"total_tokens": 2300
		}}}`,
	)
	require.NoError(t, err)

	accumulator := ai.NewMessageAccumulator()
	var partialToolCallCount int
	for _, response := range stream.responses {
		if response.GetBlock().GetPartialToolCall() != nil {
			partialToolCallCount++
		}
		require.NoError(t, accumulator.Add(response))
	}
	// A partial per tool call as it starts, and per arguments delta.
	require.Equal(t, 5, partialToolCallCount)

	parisArguments, err := structpb.NewStruct(map[string]any{"city": "Paris"})
	require.NoError(t, err)
	tokyoArguments, err := structpb.NewStruct(map[string]any{"city": "Tokyo"})
	require.NoError(t, err)
	grpcrequire.Equal(t, &aipb.Message{
		Role: aipb.Role_ROLE_ASSISTANT,
		Blocks: []*aipb.Block{
			{Index: 0, Signature: "encrypted-reasoning", ExtraFields: newExtraFields(extraFieldID, "rs_1"), Content: &aipb.Block_Thought{Thought: "Looking up Paris.\n\nThen Tokyo."}},
			{Index: 1, ExtraFields: newExtraFields(extraFieldPhase, "commentary"), Content: &aipb.Block_Text{Text: "Let me check."}},
			{Index: 2, Content: &aipb.Block_ToolCall{ToolCall: &aipb.ToolCall{Id: "call_1", Name: "get_weather", Arguments: parisArguments}}},
			{Index: 3, Content: &aipb.Block_ToolCall{ToolCall: &aipb.ToolCall{Id: "call_2", Name: "get_weather", Arguments: tokyoArguments}}},
		},
	}, accumulator.Message)
	require.Equal(t, aiservicepb.StopReason_STOP_REASON_TOOL_CALL, accumulator.StopReason)

	// Cache reads, cache writes and reasoning are counted apart from the totals that include them.
	grpcrequire.Equal(t, &aipb.ModelUsage{
		Model:                "providers/openai/models/gpt-6-sol",
		InputToken:           &aipb.ResourceConsumption{Quantity: 464},
		InputTokenCacheRead:  &aipb.ResourceConsumption{Quantity: 1024},
		InputTokenCacheWrite: &aipb.ResourceConsumption{Quantity: 512},
		OutputToken:          &aipb.ResourceConsumption{Quantity: 44},
		OutputReasoningToken: &aipb.ResourceConsumption{Quantity: 256},
	}, accumulator.ModelUsage)
}

func TestResponseReaderEnd(t *testing.T) {
	request := &aiservicepb.GenerateMessageRequest{}
	textDelta := `{"type": "response.output_text.delta", "output_index": 0, "delta": "The sea"}`
	for _, testCase := range []struct {
		name       string
		events     []string
		stopReason aiservicepb.StopReason
	}{
		{
			name:       "completed",
			events:     []string{textDelta, `{"type": "response.completed", "response": {"status": "completed"}}`},
			stopReason: aiservicepb.StopReason_STOP_REASON_END_TURN,
		},
		{
			name:       "max output tokens",
			events:     []string{textDelta, `{"type": "response.incomplete", "response": {"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}}}`},
			stopReason: aiservicepb.StopReason_STOP_REASON_MAX_TOKENS,
		},
		{
			name:       "content filter",
			events:     []string{`{"type": "response.incomplete", "response": {"status": "incomplete", "incomplete_details": {"reason": "content_filter"}}}`},
			stopReason: aiservicepb.StopReason_STOP_REASON_REFUSAL,
		},
		{
			name: "refusal",
			events: []string{
				`{"type": "response.refusal.delta", "output_index": 0, "delta": "I cannot help with that."}`,
				`{"type": "response.completed", "response": {"status": "completed"}}`,
			},
			stopReason: aiservicepb.StopReason_STOP_REASON_REFUSAL,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stream, err := readResponse(t, request, testCase.events...)
			require.NoError(t, err)
			accumulator := ai.NewMessageAccumulator()
			for _, response := range stream.responses {
				require.NoError(t, accumulator.Add(response))
			}
			require.Equal(t, testCase.stopReason, accumulator.StopReason)
		})
	}

	_, err := readResponse(t, request, `{"type": "response.failed", "response": {"status": "failed", "error": {"code": "server_error", "message": "The model failed."}}}`)
	grpcrequire.Error(t, codes.Unknown, err)
	require.ErrorContains(t, err, "server_error: The model failed.")

	_, err = readResponse(t, request, `{"type": "error", "code": "rate_limit_exceeded", "message": "Slow down."}`)
	grpcrequire.Error(t, codes.Unknown, err)
	require.ErrorContains(t, err, "rate_limit_exceeded: Slow down.")
}

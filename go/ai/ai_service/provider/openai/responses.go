package openai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/grpc/status"
)

// What a block keeps of the output item it was generated as, in its extra fields.
const (
	// The id of a reasoning item, which must accompany its encrypted content.
	extraFieldID = "id"
	// The phase of an assistant message: commentary or final_answer.
	extraFieldPhase = "phase"
)

// streamResponse generates a message through the Responses API. OpenAI stores
// nothing: reasoning comes back encrypted, is kept on the thought block, and
// goes back with the history on the next turn.
func (c *Client) streamResponse(
	ctx context.Context,
	request *aiservicepb.GenerateMessageRequest,
	messages []*aipb.Message,
	sender *provider.AsyncMessageContentSender,
) error {
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return status.FromError(err, "getting model").Err()
	}
	params, err := newResponseParams(model, request, messages)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}

	startTime := time.Now()
	stream := c.client2.Responses.NewStreaming(ctx, params)
	reader := newResponseReader(request, sender)
	var sentTtfb bool
	for stream.Next() && sender.Err() == nil {
		if !sentTtfb {
			sender.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{Ttfb: durationpb.New(time.Since(startTime))})
			sentTtfb = true
		}
		if err := reader.read(ctx, stream.Current()); err != nil {
			return err
		}
	}
	if err := stream.Err(); err != nil {
		return apiError(err, "reading stream")
	}
	if !reader.ended && sender.Err() == nil {
		return status.Errorf(codes.Unavailable, "stream closed before the response ended").Err()
	}
	sender.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{Ttlb: durationpb.New(time.Since(startTime))})
	return nil
}

func newResponseParams(model *aipb.Model, request *aiservicepb.GenerateMessageRequest, messages []*aipb.Message) (responses.ResponseNewParams, error) {
	params := responses.ResponseNewParams{
		Model:   model.ProviderModelId,
		Store:   openai.Bool(false),
		Include: []responses.ResponseIncludable{responses.ResponseIncludableReasoningEncryptedContent},
	}
	for i, message := range messages {
		items, err := pbMessageToResponseItems(message)
		if err != nil {
			return params, fmt.Errorf("message [%d]: %w", i, err)
		}
		params.Input.OfInputItemList = append(params.Input.OfInputItemList, items...)
	}

	configuration := request.GetConfiguration()
	if configuration.GetMaxTokens() > 0 {
		params.MaxOutputTokens = openai.Int(int64(configuration.GetMaxTokens()))
	}
	if configuration.GetTemperature() > 0 {
		params.Temperature = openai.Float(configuration.GetTemperature())
	}
	if model.GetTtt().GetReasoning() {
		// The model's settings name the effort it runs each level at. A level they leave out is the API's to choose.
		effort := model.GetProviderSettings().GetFields()[configuration.GetReasoningEffort().String()].GetStringValue()
		params.Reasoning = shared.ReasoningParam{
			Effort:  shared.ReasoningEffort(effort),
			Summary: shared.ReasoningSummaryAuto,
		}
	}

	for i, tool := range request.GetTools() {
		responseTool, err := pbToolToResponseTool(tool)
		if err != nil {
			return params, fmt.Errorf("tool [%d]: %w", i, err)
		}
		params.Tools = append(params.Tools, responseTool)
	}
	if configuration.GetToolChoice() != nil {
		toolChoice, err := pbToolChoiceToResponseToolChoice(configuration.GetToolChoice())
		if err != nil {
			return params, fmt.Errorf("tool choice: %w", err)
		}
		params.ToolChoice = toolChoice
	}
	return params, nil
}

func pbMessageToResponseItems(message *aipb.Message) ([]responses.ResponseInputItemUnionParam, error) {
	switch message.Role {
	case aipb.Role_ROLE_SYSTEM:
		var texts []string
		for i, block := range message.Blocks {
			switch content := block.Content.(type) {
			case *aipb.Block_Text:
				texts = append(texts, content.Text)
			default:
				return nil, fmt.Errorf("block [%d]: unexpected block type %T for SYSTEM role", i, content)
			}
		}
		return []responses.ResponseInputItemUnionParam{
			responses.ResponseInputItemParamOfMessage(strings.Join(texts, "\n"), responses.EasyInputMessageRoleSystem),
		}, nil

	case aipb.Role_ROLE_USER:
		var contentParts responses.ResponseInputMessageContentListParam
		for i, block := range message.Blocks {
			switch content := block.Content.(type) {
			case *aipb.Block_Text:
				contentParts = append(contentParts, responses.ResponseInputContentUnionParam{
					OfInputText: &responses.ResponseInputTextParam{Text: content.Text},
				})
			case *aipb.Block_Image:
				url, detail, err := imageURL(content.Image)
				if err != nil {
					return nil, fmt.Errorf("block [%d]: %w", i, err)
				}
				contentParts = append(contentParts, responses.ResponseInputContentUnionParam{
					OfInputImage: &responses.ResponseInputImageParam{
						ImageURL: openai.String(url),
						Detail:   responses.ResponseInputImageDetail(detail),
					},
				})
			case *aipb.Block_Document:
				inputFile, err := pbDocumentToInputFile(content.Document)
				if err != nil {
					return nil, fmt.Errorf("block [%d]: %w", i, err)
				}
				contentParts = append(contentParts, responses.ResponseInputContentUnionParam{OfInputFile: inputFile})
			default:
				return nil, fmt.Errorf("block [%d]: unexpected block type %T for USER role", i, content)
			}
		}
		return []responses.ResponseInputItemUnionParam{
			responses.ResponseInputItemParamOfMessage(contentParts, responses.EasyInputMessageRoleUser),
		}, nil

	case aipb.Role_ROLE_ASSISTANT:
		var items []responses.ResponseInputItemUnionParam
		for i, block := range message.Blocks {
			extraFields := block.GetExtraFields().GetFields()
			switch content := block.Content.(type) {
			case *aipb.Block_Text:
				item := responses.ResponseInputItemParamOfMessage(content.Text, responses.EasyInputMessageRoleAssistant)
				item.OfMessage.Phase = responses.EasyInputMessagePhase(extraFields[extraFieldPhase].GetStringValue())
				items = append(items, item)
			case *aipb.Block_Thought:
				// Only a reasoning item goes back: a thought holding its id and encrypted content.
				id := extraFields[extraFieldID].GetStringValue()
				if id == "" || block.Signature == "" {
					continue
				}
				// The summary is left out: the model reads the encrypted content alone.
				item := responses.ResponseInputItemParamOfReasoning(id, []responses.ResponseReasoningItemSummaryParam{})
				item.OfReasoning.EncryptedContent = openai.String(block.Signature)
				items = append(items, item)
			case *aipb.Block_ToolCall:
				arguments, err := ai.JSONMarshal(content.ToolCall.Arguments)
				if err != nil {
					return nil, fmt.Errorf("block [%d]: marshaling tool call arguments: %w", i, err)
				}
				items = append(items, responses.ResponseInputItemParamOfFunctionCall(string(arguments), content.ToolCall.Id, content.ToolCall.Name))
			case *aipb.Block_Image:
				return nil, fmt.Errorf("block [%d]: images not supported in assistant messages", i)
			default:
				return nil, fmt.Errorf("block [%d]: unexpected block type %T for ASSISTANT role", i, content)
			}
		}
		return items, nil

	case aipb.Role_ROLE_TOOL:
		var items []responses.ResponseInputItemUnionParam
		for i, block := range message.Blocks {
			switch content := block.Content.(type) {
			case *aipb.Block_ToolResult:
				output, err := ai.ParseToolResult(content.ToolResult)
				if err != nil {
					return nil, fmt.Errorf("block [%d]: converting tool result: %w", i, err)
				}
				item := responses.ResponseInputItemParamOfFunctionCallOutput(output)
				item.OfFunctionCallOutput.CallID = openai.String(content.ToolResult.ToolCallId)
				items = append(items, item)
			default:
				return nil, fmt.Errorf("block [%d]: unexpected block type %T for TOOL role", i, content)
			}
		}
		return items, nil

	default:
		return nil, fmt.Errorf("unexpected role: %v", message.Role)
	}
}

func pbDocumentToInputFile(document *aipb.Document) (*responses.ResponseInputFileParam, error) {
	switch source := document.Source.(type) {
	case *aipb.Document_Url:
		return &responses.ResponseInputFileParam{FileURL: openai.String(source.Url)}, nil
	case *aipb.Document_Data:
		if document.MediaType == "" {
			return nil, fmt.Errorf("media_type required for document")
		}
		filename := document.Title
		if filename == "" {
			filename = "document"
		}
		return &responses.ResponseInputFileParam{
			FileData: openai.String(dataURL(document.MediaType, source.Data)),
			Filename: openai.String(filename),
		}, nil
	default:
		return nil, fmt.Errorf("unknown document source type %T", source)
	}
}

func pbToolToResponseTool(tool *aipb.Tool) (responses.ToolUnionParam, error) {
	bytes, err := ai.JSONMarshal(tool.JsonSchema)
	if err != nil {
		return responses.ToolUnionParam{}, fmt.Errorf("marshaling json schema: %w", err)
	}
	var parameters map[string]any
	if err := json.Unmarshal(bytes, &parameters); err != nil {
		return responses.ToolUnionParam{}, fmt.Errorf("unmarshaling parameters: %w", err)
	}
	// Strict mode, the default here, rewrites the schema and rejects optional properties.
	functionTool := responses.ToolParamOfFunction(tool.Name, parameters, false)
	functionTool.OfFunction.Description = openai.String(tool.Description)
	return functionTool, nil
}

func pbToolChoiceToResponseToolChoice(toolChoice *aipb.ToolChoice) (responses.ResponseNewParamsToolChoiceUnion, error) {
	switch choice := toolChoice.Choice.(type) {
	case *aipb.ToolChoice_Mode:
		mode, ok := toolChoiceModeToResponseToolChoice[choice.Mode]
		if !ok {
			return responses.ResponseNewParamsToolChoiceUnion{}, fmt.Errorf("unknown mode: %s", choice.Mode)
		}
		return responses.ResponseNewParamsToolChoiceUnion{OfToolChoiceMode: openai.Opt(mode)}, nil
	case *aipb.ToolChoice_ToolName:
		return responses.ResponseNewParamsToolChoiceUnion{
			OfFunctionTool: &responses.ToolChoiceFunctionParam{Name: choice.ToolName},
		}, nil
	default:
		return responses.ResponseNewParamsToolChoiceUnion{}, fmt.Errorf("unknown choice type: %T", choice)
	}
}

var toolChoiceModeToResponseToolChoice = map[aipb.ToolChoiceMode]responses.ToolChoiceOptions{
	aipb.ToolChoiceMode_TOOL_CHOICE_MODE_NONE:     responses.ToolChoiceOptionsNone,
	aipb.ToolChoiceMode_TOOL_CHOICE_MODE_AUTO:     responses.ToolChoiceOptionsAuto,
	aipb.ToolChoiceMode_TOOL_CHOICE_MODE_REQUIRED: responses.ToolChoiceOptionsRequired,
}

// responseReader turns the events of a streamed response into message content.
// Each output item is one block, at the item's index.
type responseReader struct {
	request             *aiservicepb.GenerateMessageRequest
	sender              *provider.AsyncMessageContentSender
	toolCallAccumulator *provider.ToolCallAccumulator
	calledTool          bool
	refused             bool
	ended               bool
}

func newResponseReader(request *aiservicepb.GenerateMessageRequest, sender *provider.AsyncMessageContentSender) *responseReader {
	return &responseReader{
		request:             request,
		sender:              sender,
		toolCallAccumulator: provider.NewToolCallAccumulator(),
	}
}

func (r *responseReader) read(ctx context.Context, event responses.ResponseStreamEventUnion) error {
	switch event := event.AsAny().(type) {
	case responses.ResponseOutputItemAddedEvent:
		if event.Item.Type == "function_call" {
			r.toolCallAccumulator.Start(event.OutputIndex, event.Item.CallID, event.Item.Name)
			return r.sendPartialToolCall(ctx, event.OutputIndex)
		}

	case responses.ResponseTextDeltaEvent:
		r.sender.SendBlocks(ctx, &aipb.Block{Index: event.OutputIndex, Content: &aipb.Block_Text{Text: event.Delta}})

	case responses.ResponseRefusalDeltaEvent:
		r.refused = true
		r.sender.SendBlocks(ctx, &aipb.Block{Index: event.OutputIndex, Content: &aipb.Block_Text{Text: event.Delta}})

	case responses.ResponseReasoningSummaryPartAddedEvent:
		// A summary comes in parts, which make one thought.
		if event.SummaryIndex > 0 {
			r.sender.SendBlocks(ctx, &aipb.Block{Index: event.OutputIndex, Content: &aipb.Block_Thought{Thought: "\n\n"}})
		}

	case responses.ResponseReasoningSummaryTextDeltaEvent:
		r.sender.SendBlocks(ctx, &aipb.Block{Index: event.OutputIndex, Content: &aipb.Block_Thought{Thought: event.Delta}})

	case responses.ResponseFunctionCallArgumentsDeltaEvent:
		r.toolCallAccumulator.AppendArgs(event.OutputIndex, event.Delta)
		return r.sendPartialToolCall(ctx, event.OutputIndex)

	case responses.ResponseOutputItemDoneEvent:
		return r.readItem(ctx, event.OutputIndex, event.Item)

	case responses.ResponseCompletedEvent:
		return r.end(ctx, event.Response)

	case responses.ResponseIncompleteEvent:
		return r.end(ctx, event.Response)

	case responses.ResponseFailedEvent:
		return status.Errorf(codes.Unknown, "response failed: %s: %s", event.Response.Error.Code, event.Response.Error.Message).Err()

	case responses.ResponseErrorEvent:
		return status.Errorf(codes.Unknown, "response failed: %s: %s", event.Code, event.Message).Err()
	}
	return nil
}

func (r *responseReader) sendPartialToolCall(ctx context.Context, index int64) error {
	if !r.request.GetConfiguration().GetStreamPartialToolCalls() {
		return nil
	}
	block, err := r.toolCallAccumulator.BuildPartial(index)
	if err != nil {
		return err
	}
	r.sender.SendBlocks(ctx, block)
	return nil
}

// readItem completes the block of a finished output item with what only the
// whole item holds.
func (r *responseReader) readItem(ctx context.Context, index int64, item responses.ResponseOutputItemUnion) error {
	switch item.Type {
	case "reasoning":
		r.sender.SendBlocks(ctx, &aipb.Block{
			Index:       index,
			Signature:   item.EncryptedContent,
			ExtraFields: newExtraFields(extraFieldID, item.ID),
			Content:     &aipb.Block_Thought{},
		})
	case "message":
		if item.Phase != "" {
			r.sender.SendBlocks(ctx, &aipb.Block{
				Index:       index,
				ExtraFields: newExtraFields(extraFieldPhase, string(item.Phase)),
				Content:     &aipb.Block_Text{},
			})
		}
	case "function_call":
		block, err := r.toolCallAccumulator.Build(index)
		if err != nil {
			return err
		}
		r.calledTool = true
		r.sender.SendBlocks(ctx, block)
	}
	return nil
}

func (r *responseReader) end(ctx context.Context, response responses.Response) error {
	r.ended = true
	stopReason := aiservicepb.StopReason_STOP_REASON_END_TURN
	switch {
	case response.Status == responses.ResponseStatusIncomplete:
		var ok bool
		stopReason, ok = incompleteReasonToPb[response.IncompleteDetails.Reason]
		if !ok {
			return status.Errorf(codes.Internal, "unknown incomplete reason: %s", response.IncompleteDetails.Reason).Err()
		}
	case r.refused:
		stopReason = aiservicepb.StopReason_STOP_REASON_REFUSAL
	case r.calledTool:
		stopReason = aiservicepb.StopReason_STOP_REASON_TOOL_CALL
	}
	r.sender.SendModelUsage(ctx, responseModelUsage(r.request.Model, response.Usage))
	r.sender.SendStopReason(ctx, stopReason)
	return nil
}

// responseModelUsage splits the usage into disjoint categories: OpenAI counts
// cache reads and writes within the input tokens, and reasoning within the output tokens.
func responseModelUsage(model string, usage responses.ResponseUsage) *aipb.ModelUsage {
	cacheReadTokens := usage.InputTokensDetails.CachedTokens
	cacheWriteTokens := usage.InputTokensDetails.CacheWriteTokens
	reasoningTokens := usage.OutputTokensDetails.ReasoningTokens
	return &aipb.ModelUsage{
		Model:                model,
		InputToken:           ai.NewResourceConsumption(usage.InputTokens - cacheReadTokens - cacheWriteTokens),
		InputTokenCacheRead:  ai.NewResourceConsumption(cacheReadTokens),
		InputTokenCacheWrite: ai.NewResourceConsumption(cacheWriteTokens),
		OutputToken:          ai.NewResourceConsumption(usage.OutputTokens - reasoningTokens),
		OutputReasoningToken: ai.NewResourceConsumption(reasoningTokens),
	}
}

func newExtraFields(key, value string) *structpb.Struct {
	return &structpb.Struct{Fields: map[string]*structpb.Value{key: structpb.NewStringValue(value)}}
}

var incompleteReasonToPb = map[string]aiservicepb.StopReason{
	"max_output_tokens": aiservicepb.StopReason_STOP_REASON_MAX_TOKENS,
	"content_filter":    aiservicepb.StopReason_STOP_REASON_REFUSAL,
}

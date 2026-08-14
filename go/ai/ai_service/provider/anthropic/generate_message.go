package anthropic

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/shared/constant"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

func (c *Client) StreamGenerateMessage(
	ctx context.Context,
	request *aiservicepb.GenerateMessageRequest,
	requestMessages []*aipb.Message,
	sender *provider.AsyncMessageContentSender,
) error {
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}

	var systemBlocks []anthropic.TextBlockParam
	messages := make([]anthropic.MessageParam, 0, len(requestMessages))

	for i, msg := range requestMessages {
		switch msg.Role {
		case aipb.Role_ROLE_SYSTEM:
			for j, block := range msg.Blocks {
				switch content := block.Content.(type) {
				case *aipb.Block_Text:
					systemBlocks = append(systemBlocks, anthropic.TextBlockParam{Text: content.Text})
				default:
					return status.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unexpected block type %T for SYSTEM role", i, j, content).Err()
				}
			}

		case aipb.Role_ROLE_USER:
			var contentBlocks []anthropic.ContentBlockParamUnion
			for j, block := range msg.Blocks {
				switch content := block.Content.(type) {
				case *aipb.Block_Text:
					contentBlocks = append(contentBlocks, anthropic.NewTextBlock(content.Text))
				case *aipb.Block_Image:
					img := content.Image
					switch source := img.Source.(type) {
					case *aipb.Image_Url:
						contentBlocks = append(contentBlocks, anthropic.NewImageBlock(anthropic.URLImageSourceParam{
							URL: source.Url,
						}))
					case *aipb.Image_Data:
						mediaType := anthropic.Base64ImageSourceMediaType(img.MediaType)
						if _, ok := imageSourceMediaTypeSet[mediaType]; !ok {
							return status.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unsupported media type %s", i, j, img.MediaType).Err()
						}
						contentBlocks = append(contentBlocks, anthropic.NewImageBlock(anthropic.Base64ImageSourceParam{
							Data:      base64.StdEncoding.EncodeToString(source.Data),
							MediaType: mediaType,
						}))
					default:
						return status.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unexpected image source type %T", i, j, source).Err()
					}
				default:
					return status.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unexpected block type %T for USER role", i, j, content).Err()
				}
			}
			messages = append(messages, anthropic.NewUserMessage(contentBlocks...))

		case aipb.Role_ROLE_ASSISTANT:
			var contentBlocks []anthropic.ContentBlockParamUnion
			for j, block := range msg.Blocks {
				switch content := block.Content.(type) {
				case *aipb.Block_Text:
					contentBlocks = append(contentBlocks, anthropic.NewTextBlock(content.Text))
				case *aipb.Block_Thought:
					contentBlocks = append(contentBlocks, anthropic.NewThinkingBlock(block.Signature, content.Thought))
				case *aipb.Block_ToolCall:
					tc := content.ToolCall
					bytes, err := pbutil.JSONMarshal(tc.Arguments)
					if err != nil {
						return status.Errorf(codes.InvalidArgument, "message [%d] block [%d]: marshaling tool call arguments: %v", i, j, err).Err()
					}
					contentBlocks = append(contentBlocks, anthropic.NewToolUseBlock(tc.Id, json.RawMessage(bytes), tc.Name))
				default:
					return status.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unexpected block type %T for ASSISTANT role", i, j, content).Err()
				}
			}
			messages = append(messages, anthropic.NewAssistantMessage(contentBlocks...))

		case aipb.Role_ROLE_TOOL:
			for j, block := range msg.Blocks {
				switch content := block.Content.(type) {
				case *aipb.Block_ToolResult:
					tr := content.ToolResult
					text, err := ai.ParseToolResult(tr)
					if err != nil {
						return status.Errorf(codes.InvalidArgument, "message [%d] block [%d]: converting tool result: %v", i, j, err).Err()
					}
					toolResultBlock := anthropic.NewToolResultBlock(tr.ToolCallId, text, tr.GetError() != nil)
					messages = append(messages, anthropic.NewUserMessage(toolResultBlock))
				default:
					return status.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unexpected block type %T for TOOL role", i, j, content).Err()
				}
			}

		default:
			return status.Errorf(codes.InvalidArgument, "message [%d]: unexpected role %v", i, msg.Role).Err()
		}
	}

	messageParams := anthropic.MessageNewParams{
		Model:     anthropic.Model(model.ProviderModelId),
		Messages:  messages,
		MaxTokens: int64(request.Configuration.GetMaxTokens()),
	}
	if request.Configuration.GetTemperature() > 0 {
		// Newer models (opus-4.7+, opus-5, sonnet-5, fable-5) reject non-default sampling parameters on every request.
		if model.GetProviderSettings().GetFields()["sampling_parameters_unsupported"].GetBoolValue() {
			return status.Errorf(codes.InvalidArgument, "%s does not support temperature", request.Model).Err()
		}
		messageParams.Temperature = anthropic.Float(request.Configuration.GetTemperature())
	}
	if len(systemBlocks) > 0 {
		messageParams.System = systemBlocks
	}

	if model.Ttt.Reasoning {
		providerSettings := model.GetProviderSettings().GetFields()
		effortValue, hasEffort := providerSettings[request.Configuration.GetReasoningEffort().String()]
		effort := effortValue.GetStringValue()

		// `output_config.effort` is supported wherever the model config maps ReasoningEffort levels,
		// including extended-thinking-only models (opus-4.5) where it works alongside budget_tokens.
		if hasEffort && effort != "disabled" {
			messageParams.OutputConfig = anthropic.OutputConfigParam{Effort: anthropic.OutputConfigEffort(effort)}
		}

		if providerSettings["thinking_config_key"].GetStringValue() == "effort" {
			// Adaptive-thinking models (4.6+): thinking.type=enabled is deprecated (4.6) or rejected (4.7+).
			switch {
			case !hasEffort:
				// No mapping for this ReasoningEffort: omit thinking and effort entirely, API defaults apply.
			case effort == "disabled":
				messageParams.Thinking = anthropic.ThinkingConfigParamUnion{OfDisabled: &anthropic.ThinkingConfigDisabledParam{}}
			default:
				messageParams.Thinking = anthropic.ThinkingConfigParamUnion{OfAdaptive: &anthropic.ThinkingConfigAdaptiveParam{
					// display defaults to "omitted" (empty thinking blocks) on newer models; we want the summarized text streamed.
					Display: anthropic.ThinkingConfigAdaptiveDisplaySummarized,
				}}
			}
		} else if budget := pbReasoningEffortToAnthropicBudget(request.Configuration.GetReasoningEffort()); budget > 0 {
			// Extended-thinking models: fixed thinking token budget.
			messageParams.Thinking = anthropic.ThinkingConfigParamOfEnabled(budget)
		}
	}

	if len(request.Tools) > 0 {
		tools := make([]anthropic.ToolUnionParam, 0, len(request.Tools))
		for _, tool := range request.Tools {
			tools = append(tools, pbToolToAnthropic(tool, request.GetConfiguration().GetStreamPartialToolCalls()))
		}
		messageParams.Tools = tools
	}

	if request.GetConfiguration().GetToolChoice() != nil {
		toolChoice, err := pbToolChoiceToAnthropic(request.GetConfiguration().GetToolChoice())
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "tool choice: %v", err).Err()
		}
		messageParams.ToolChoice = toolChoice
	}

	startTime := time.Now()
	messageStream := c.client.Messages.NewStreaming(ctx, messageParams)

	tca := provider.NewToolCallAccumulator()
	redactedThinkingIndexSet := map[int64]struct{}{}
	var sentTtfb bool

	for messageStream.Next() && sender.Err() == nil {
		event := messageStream.Current()

		if !sentTtfb {
			sender.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{Ttfb: durationpb.New(time.Since(startTime))})
			sentTtfb = true
		}

		switch variant := event.AsAny().(type) {
		case anthropic.MessageStartEvent:
			modelUsage := &aipb.ModelUsage{Model: request.Model}
			if variant.Message.Usage.InputTokens > 0 {
				modelUsage.InputToken = &aipb.ResourceConsumption{Quantity: int32(variant.Message.Usage.InputTokens)}
			}
			if variant.Message.Usage.CacheReadInputTokens > 0 {
				modelUsage.InputTokenCacheRead = &aipb.ResourceConsumption{Quantity: int32(variant.Message.Usage.CacheReadInputTokens)}
			}
			if variant.Message.Usage.CacheCreationInputTokens > 0 {
				modelUsage.InputTokenCacheWrite = &aipb.ResourceConsumption{Quantity: int32(variant.Message.Usage.CacheCreationInputTokens)}
			}
			sender.SendModelUsage(ctx, modelUsage)

		case anthropic.ContentBlockStartEvent:
			switch contentBlock := variant.ContentBlock.AsAny().(type) {
			case anthropic.ToolUseBlock:
				tca.Start(variant.Index, contentBlock.ID, contentBlock.Name)
				if request.GetConfiguration().GetStreamPartialToolCalls() {
					block, err := tca.BuildPartial(variant.Index)
					if err != nil {
						return err
					}
					sender.SendBlocks(ctx, block)
				}
			case anthropic.TextBlock:
			case anthropic.ThinkingBlock:
				// Adaptive thinking (effort-based models) can deliver the summarized thought
				// directly in the start event with no subsequent thinking deltas.
				if contentBlock.Thinking != "" {
					sender.SendBlocks(ctx, &aipb.Block{Index: variant.Index, Signature: contentBlock.Signature, Content: &aipb.Block_Thought{Thought: contentBlock.Thinking}})
				} else {
					redactedThinkingIndexSet[variant.Index] = struct{}{}
				}
			case anthropic.RedactedThinkingBlock:
				redactedThinkingIndexSet[variant.Index] = struct{}{}
			case anthropic.ServerToolUseBlock:
			case anthropic.WebSearchToolResultBlock:
			default:
				return status.Errorf(codes.Internal, "unexpected content block type: %T", contentBlock).Err()
			}

		case anthropic.ContentBlockDeltaEvent:
			switch delta := variant.Delta.AsAny().(type) {
			case anthropic.TextDelta:
				sender.SendBlocks(ctx, &aipb.Block{Index: variant.Index, Content: &aipb.Block_Text{Text: delta.Text}})
			case anthropic.ThinkingDelta:
				delete(redactedThinkingIndexSet, variant.Index)
				sender.SendBlocks(ctx, &aipb.Block{Index: variant.Index, Content: &aipb.Block_Thought{Thought: delta.Thinking}})
			case anthropic.SignatureDelta:
				sender.SendBlocks(ctx, &aipb.Block{Index: variant.Index, Signature: delta.Signature})
			case anthropic.InputJSONDelta:
				tca.AppendArgs(variant.Index, delta.PartialJSON)
				if request.GetConfiguration().GetStreamPartialToolCalls() {
					block, err := tca.BuildPartial(variant.Index)
					if err != nil {
						return err
					}
					sender.SendBlocks(ctx, block)
				}
			default:
				return status.Errorf(codes.Internal, "unexpected delta type: %T", delta).Err()
			}

		case anthropic.ContentBlockStopEvent:
			if _, ok := redactedThinkingIndexSet[variant.Index]; ok {
				sender.SendBlocks(ctx, &aipb.Block{Index: variant.Index, Content: &aipb.Block_Thought{Thought: "Thinking... [redacted]"}})
			}
			if tca.Has(variant.Index) {
				block, err := tca.Build(variant.Index)
				if err != nil {
					return err
				}
				sender.SendBlocks(ctx, block)
			}

		case anthropic.MessageDeltaEvent:
			modelUsage := &aipb.ModelUsage{Model: request.Model}
			if variant.Usage.OutputTokens > 0 {
				modelUsage.OutputToken = &aipb.ResourceConsumption{Quantity: int32(variant.Usage.OutputTokens)}
			}
			sender.SendModelUsage(ctx, modelUsage)

			stopReason, ok := anthropicStopReasonToPb[variant.Delta.StopReason]
			if !ok {
				return status.Errorf(codes.Internal, "unknown stop reason: %s", variant.Delta.StopReason).Err()
			}
			sender.SendStopReason(ctx, stopReason)

		case anthropic.MessageStopEvent:
			sender.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{Ttlb: durationpb.New(time.Since(startTime))})

		default:
			return status.Errorf(codes.Internal, "unexpected event type: %T", variant).Err()
		}
	}

	if err := messageStream.Err(); err != nil {
		return status.FromError(err, "streaming from anthropic").Err()
	}
	return nil
}

func pbToolChoiceToAnthropic(toolChoice *aipb.ToolChoice) (anthropic.ToolChoiceUnionParam, error) {
	switch choice := toolChoice.Choice.(type) {
	case *aipb.ToolChoice_Mode:
		switch choice.Mode {
		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_NONE:
			return anthropic.ToolChoiceUnionParam{OfNone: &anthropic.ToolChoiceNoneParam{}}, nil
		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_AUTO:
			return anthropic.ToolChoiceUnionParam{OfAuto: &anthropic.ToolChoiceAutoParam{}}, nil
		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_REQUIRED:
			return anthropic.ToolChoiceUnionParam{OfAny: &anthropic.ToolChoiceAnyParam{}}, nil
		default:
			return anthropic.ToolChoiceUnionParam{}, fmt.Errorf("unknown tool choice mode: %s", choice.Mode)
		}
	case *aipb.ToolChoice_ToolName:
		return anthropic.ToolChoiceUnionParam{OfTool: &anthropic.ToolChoiceToolParam{Name: choice.ToolName}}, nil
	default:
		return anthropic.ToolChoiceUnionParam{}, fmt.Errorf("unknown tool choice type: %T", choice)
	}
}

func pbReasoningEffortToAnthropicBudget(reasoningEffort aipb.ReasoningEffort) int64 {
	switch reasoningEffort {
	case aipb.ReasoningEffort_REASONING_EFFORT_LOW:
		return 1024
	case aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM, aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT:
		return 5000
	case aipb.ReasoningEffort_REASONING_EFFORT_HIGH:
		return 10000
	case aipb.ReasoningEffort_REASONING_EFFORT_XHIGH:
		return 20000
	case aipb.ReasoningEffort_REASONING_EFFORT_MAX:
		// Budgets above 32k are discouraged outside batch processing (timeouts), so cap here.
		return 32000
	default:
		return 0
	}
}

func pbToolToAnthropic(tool *aipb.Tool, eagerInputStreaming bool) anthropic.ToolUnionParam {
	inputSchema := anthropic.ToolInputSchemaParam{
		Type:       "object",
		Properties: map[string]*jsonpb.Schema{},
	}
	description := tool.GetDescription()
	if tool.GetJsonSchema() != nil {
		inputSchema.Type = constant.Object(tool.JsonSchema.Type)
		inputSchema.Properties = tool.GetJsonSchema().GetProperties()
		inputSchema.Required = tool.GetJsonSchema().GetRequired()
		if desc := tool.GetJsonSchema().GetDescription(); desc != "" {
			description += ". Schema description: " + desc
		}
	}
	toolParam := &anthropic.ToolParam{
		Name:        tool.Name,
		Description: anthropic.String(description),
		Type:        anthropic.ToolTypeCustom,
		InputSchema: inputSchema,
	}
	// Eager streaming emits input deltas before the JSON is well-formed, so only opt in when the caller consumes partials.
	if eagerInputStreaming {
		toolParam.EagerInputStreaming = anthropic.Bool(true)
	}
	return anthropic.ToolUnionParam{OfTool: toolParam}
}

var anthropicStopReasonToPb = map[anthropic.StopReason]aiservicepb.StopReason{
	anthropic.StopReasonEndTurn:      aiservicepb.StopReason_STOP_REASON_END_TURN,
	anthropic.StopReasonMaxTokens:    aiservicepb.StopReason_STOP_REASON_MAX_TOKENS,
	anthropic.StopReasonToolUse:      aiservicepb.StopReason_STOP_REASON_TOOL_CALL,
	anthropic.StopReasonStopSequence: aiservicepb.StopReason_STOP_REASON_STOP_SEQUENCE,
	anthropic.StopReasonPauseTurn:    aiservicepb.StopReason_STOP_REASON_PAUSE_TURN,
	anthropic.StopReasonRefusal:      aiservicepb.StopReason_STOP_REASON_REFUSAL,
}

var imageSourceMediaTypeSet = map[anthropic.Base64ImageSourceMediaType]struct{}{
	anthropic.Base64ImageSourceMediaTypeImageJPEG: {},
	anthropic.Base64ImageSourceMediaTypeImagePNG:  {},
	anthropic.Base64ImageSourceMediaTypeImageGIF:  {},
	anthropic.Base64ImageSourceMediaTypeImageWebP: {},
}

package anthropic

import (
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
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/pbutil"
)

func (c *Client) TextToTextStream(request *aiservicepb.TextToTextStreamRequest, srv aiservicepb.AiService_TextToTextStreamServer) error {
	ctx := srv.Context()

	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}

	var systemBlocks []anthropic.TextBlockParam
	messages := make([]anthropic.MessageParam, 0, len(request.Messages))

	for i, msg := range request.Messages {
		switch msg.Role {
		case aipb.Role_ROLE_SYSTEM:
			for j, block := range msg.Blocks {
				switch content := block.Content.(type) {
				case *aipb.Block_Text:
					systemBlocks = append(systemBlocks, anthropic.TextBlockParam{Text: content.Text})
				default:
					return grpc.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unexpected block type %T for SYSTEM role", i, j, content).Err()
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
							return grpc.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unsupported media type %s", i, j, img.MediaType).Err()
						}
						contentBlocks = append(contentBlocks, anthropic.NewImageBlock(anthropic.Base64ImageSourceParam{
							Data:      base64.StdEncoding.EncodeToString(source.Data),
							MediaType: mediaType,
						}))
					default:
						return grpc.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unexpected image source type %T", i, j, source).Err()
					}
				default:
					return grpc.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unexpected block type %T for USER role", i, j, content).Err()
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
						return grpc.Errorf(codes.InvalidArgument, "message [%d] block [%d]: marshaling tool call arguments: %v", i, j, err).Err()
					}
					contentBlocks = append(contentBlocks, anthropic.NewToolUseBlock(tc.Id, json.RawMessage(bytes), tc.Name))
				default:
					return grpc.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unexpected block type %T for ASSISTANT role", i, j, content).Err()
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
						return grpc.Errorf(codes.InvalidArgument, "message [%d] block [%d]: converting tool result: %v", i, j, err).Err()
					}
					toolResultBlock := anthropic.NewToolResultBlock(tr.ToolCallId, text, tr.GetError() != nil)
					messages = append(messages, anthropic.NewUserMessage(toolResultBlock))
				default:
					return grpc.Errorf(codes.InvalidArgument, "message [%d] block [%d]: unexpected block type %T for TOOL role", i, j, content).Err()
				}
			}

		default:
			return grpc.Errorf(codes.InvalidArgument, "message [%d]: unexpected role %v", i, msg.Role).Err()
		}
	}

	messageParams := anthropic.MessageNewParams{
		Model:     anthropic.Model(model.ProviderModelId),
		Messages:  messages,
		MaxTokens: int64(request.Configuration.GetMaxTokens()),
	}
	if request.Configuration.GetTemperature() > 0 {
		messageParams.Temperature = anthropic.Float(request.Configuration.GetTemperature())
	}
	if len(systemBlocks) > 0 {
		messageParams.System = systemBlocks
	}

	if model.Ttt.Reasoning {
		budget := pbReasoningEffortToAnthropicBudget(request.Configuration.GetReasoningEffort())
		if budget > 0 {
			messageParams.Thinking = anthropic.ThinkingConfigParamOfEnabled(budget)
		}
	}

	if len(request.Tools) > 0 {
		tools := make([]anthropic.ToolUnionParam, 0, len(request.Tools))
		for _, tool := range request.Tools {
			tools = append(tools, pbToolToAnthropic(tool))
		}
		messageParams.Tools = tools
	}

	if request.GetConfiguration().GetToolChoice() != nil {
		toolChoice, err := pbToolChoiceToAnthropic(request.GetConfiguration().GetToolChoice())
		if err != nil {
			return grpc.Errorf(codes.InvalidArgument, "tool choice: %v", err).Err()
		}
		messageParams.ToolChoice = toolChoice
	}

	startTime := time.Now()
	messageStream := c.client.Messages.NewStreaming(ctx, messageParams)

	cs := provider.NewAsyncTextToTextContentSender(srv, 100)
	defer cs.Close()

	tca := provider.NewToolCallAccumulator()
	var sentTtfb bool

	for messageStream.Next() && cs.Err() == nil {
		event := messageStream.Current()

		if !sentTtfb {
			cs.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{Ttfb: durationpb.New(time.Since(startTime))})
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
			cs.SendModelUsage(ctx, modelUsage)

		case anthropic.ContentBlockStartEvent:
			switch contentBlock := variant.ContentBlock.AsAny().(type) {
			case anthropic.ToolUseBlock:
				tca.Start(variant.Index, contentBlock.ID, contentBlock.Name)
				if request.GetConfiguration().GetStreamPartialToolCalls() {
					block, err := tca.BuildPartial(variant.Index)
					if err != nil {
						return err
					}
					cs.SendBlocks(ctx, block)
				}
			case anthropic.TextBlock:
			case anthropic.ThinkingBlock:
			case anthropic.RedactedThinkingBlock:
			case anthropic.ServerToolUseBlock:
			case anthropic.WebSearchToolResultBlock:
			default:
				return grpc.Errorf(codes.Internal, "unexpected content block type: %T", contentBlock).Err()
			}

		case anthropic.ContentBlockDeltaEvent:
			switch delta := variant.Delta.AsAny().(type) {
			case anthropic.TextDelta:
				cs.SendBlocks(ctx, &aipb.Block{Index: variant.Index, Content: &aipb.Block_Text{Text: delta.Text}})
			case anthropic.ThinkingDelta:
				cs.SendBlocks(ctx, &aipb.Block{Index: variant.Index, Content: &aipb.Block_Thought{Thought: delta.Thinking}})
			case anthropic.SignatureDelta:
				cs.SendBlocks(ctx, &aipb.Block{Index: variant.Index, Signature: delta.Signature})
			case anthropic.InputJSONDelta:
				tca.AppendArgs(variant.Index, delta.PartialJSON)
				if request.GetConfiguration().GetStreamPartialToolCalls() {
					block, err := tca.BuildPartial(variant.Index)
					if err != nil {
						return err
					}
					cs.SendBlocks(ctx, block)
				}
			default:
				return grpc.Errorf(codes.Internal, "unexpected delta type: %T", delta).Err()
			}

		case anthropic.ContentBlockStopEvent:
			if tca.Has(variant.Index) {
				block, err := tca.Build(variant.Index)
				if err != nil {
					return err
				}
				cs.SendBlocks(ctx, block)
			}

		case anthropic.MessageDeltaEvent:
			modelUsage := &aipb.ModelUsage{Model: request.Model}
			if variant.Usage.OutputTokens > 0 {
				modelUsage.OutputToken = &aipb.ResourceConsumption{Quantity: int32(variant.Usage.OutputTokens)}
			}
			cs.SendModelUsage(ctx, modelUsage)

			stopReason, ok := anthropicStopReasonToPb[variant.Delta.StopReason]
			if !ok {
				return grpc.Errorf(codes.Internal, "unknown stop reason: %s", variant.Delta.StopReason).Err()
			}
			cs.SendStopReason(ctx, stopReason)

		case anthropic.MessageStopEvent:
			cs.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{Ttlb: durationpb.New(time.Since(startTime))})

		default:
			return grpc.Errorf(codes.Internal, "unexpected event type: %T", variant).Err()
		}
	}

	if err := messageStream.Err(); err != nil {
		return fmt.Errorf("stream error: %w", err)
	}

	cs.Close()
	return cs.Wait(ctx)
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
	default:
		return 0
	}
}

func pbToolToAnthropic(tool *aipb.Tool) anthropic.ToolUnionParam {
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
	return anthropic.ToolUnionParam{
		OfTool: &anthropic.ToolParam{
			Name:        tool.Name,
			Description: anthropic.String(description),
			Type:        anthropic.ToolTypeCustom,
			InputSchema: inputSchema,
		},
	}
}

var anthropicStopReasonToPb = map[anthropic.StopReason]aiservicepb.TextToTextStopReason{
	anthropic.StopReasonEndTurn:      aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
	anthropic.StopReasonMaxTokens:    aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_MAX_TOKENS,
	anthropic.StopReasonToolUse:      aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_TOOL_CALL,
	anthropic.StopReasonStopSequence: aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_STOP_SEQUENCE,
	anthropic.StopReasonPauseTurn:    aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_PAUSE_TURN,
	anthropic.StopReasonRefusal:      aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
}

var imageSourceMediaTypeSet = map[anthropic.Base64ImageSourceMediaType]struct{}{
	anthropic.Base64ImageSourceMediaTypeImageJPEG: {},
	anthropic.Base64ImageSourceMediaTypeImagePNG:  {},
	anthropic.Base64ImageSourceMediaTypeImageGIF:  {},
	anthropic.Base64ImageSourceMediaTypeImageWebP: {},
}

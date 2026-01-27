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
		switch m := msg.Message.(type) {
		case *aipb.Message_System:
			systemBlocks = append(systemBlocks, anthropic.TextBlockParam{
				Text: m.System.Content,
			})

		case *aipb.Message_User:
			var contentBlocks []anthropic.ContentBlockParamUnion
			for _, contentBlock := range m.User.ContentBlocks {
				switch c := contentBlock.GetContent().(type) {
				case *aipb.ContentBlock_Text:
					contentBlocks = append(contentBlocks, anthropic.NewTextBlock(c.Text))
				case *aipb.ContentBlock_Image:
					switch s := c.Image.GetSource().(type) {
					case *aipb.ImageBlock_Url:
						contentBlocks = append(contentBlocks, anthropic.NewImageBlock(anthropic.URLImageSourceParam{
							URL: s.Url,
						}))
					case *aipb.ImageBlock_Data:
						mediaType := anthropic.Base64ImageSourceMediaType(c.Image.MediaType)
						if _, ok := imageSourceMediaTypeSet[mediaType]; !ok {
							return grpc.Errorf(codes.InvalidArgument, "unsupported media type %s", c.Image.MediaType).Err()
						}
						base64ImageSourceParam := anthropic.Base64ImageSourceParam{
							Data:      base64.StdEncoding.EncodeToString(s.Data),
							MediaType: mediaType,
						}
						contentBlocks = append(contentBlocks, anthropic.NewImageBlock(base64ImageSourceParam))
					default:
						return grpc.Errorf(codes.Unimplemented, "unknown image block type %T", s).Err()
					}
				default:
					return grpc.Errorf(codes.Unimplemented, "unknown content block type %T", c).Err()
				}
			}
			messages = append(messages, anthropic.NewUserMessage(contentBlocks...))

		case *aipb.Message_Assistant:
			var contentBlockParamUnions []anthropic.ContentBlockParamUnion
			if m.Assistant.Content != "" {
				contentBlockParamUnions = append(contentBlockParamUnions, anthropic.NewTextBlock(m.Assistant.Content))
			}
			if m.Assistant.StructuredContent != nil {
				bytes, err := pbutil.JSONMarshal(m.Assistant.StructuredContent)
				if err != nil {
					return grpc.Errorf(codes.InvalidArgument, "message [%d]: marshaling structured content: %v", i, err).Err()
				}
				contentBlockParamUnions = append(contentBlockParamUnions, anthropic.NewTextBlock(string(bytes)))
			}
			for j, tc := range m.Assistant.ToolCalls {
				bytes, err := pbutil.JSONMarshal(tc.Arguments)
				if err != nil {
					return grpc.Errorf(codes.InvalidArgument, "message [%d]: marshaling tool call [%d] arguments: %v", i, j, err).Err()
				}
				contentBlockParamUnions = append(contentBlockParamUnions, anthropic.NewToolUseBlock(tc.Id, json.RawMessage(bytes), tc.Name))
			}
			messages = append(messages, anthropic.NewAssistantMessage(contentBlockParamUnions...))

		case *aipb.Message_Tool:
			content, err := ai.ParseToolResult(m.Tool.Result)
			if err != nil {
				return grpc.Errorf(codes.InvalidArgument, "message [%d]: converting tool result [%d] to text: %v", i, err).Err()
			}
			toolResultBlock := anthropic.NewToolResultBlock(m.Tool.ToolCallId, content, m.Tool.Result.GetError() != nil)
			messages = append(messages, anthropic.NewUserMessage(toolResultBlock)) // Anthropic passes tool results with role user.
		}
	}

	// Build the request.
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

	// Add thinking configuration for reasoning models.
	if model.Ttt.Reasoning {
		budget := pbReasoningEffortToAnthropicBudget(request.Configuration.GetReasoningEffort())
		if budget > 0 {
			messageParams.Thinking = anthropic.ThinkingConfigParamOfEnabled(budget)
		}
	}

	// Add tools if provided.
	if len(request.Tools) > 0 {
		tools := make([]anthropic.ToolUnionParam, 0, len(request.Tools))
		for _, tool := range request.Tools {
			anthropicTool := pbToolToAnthropic(tool)
			tools = append(tools, anthropicTool)
		}
		messageParams.Tools = tools
	}

	// Add tool choice if provided.
	if request.GetConfiguration().GetToolChoice() != nil {
		toolChoice, err := pbToolChoiceToAnthropic(request.GetConfiguration().GetToolChoice())
		if err != nil {
			return grpc.Errorf(codes.InvalidArgument, "tool choice: %v", err).Err()
		}
		messageParams.ToolChoice = toolChoice
	}

	// Create streaming request
	startTime := time.Now()
	messageStream := c.client.Messages.NewStreaming(ctx, messageParams)

	cs := provider.NewAsyncTextToTextContentSender(srv, 100)
	defer cs.Close()

	// Track active tool_use blocks by content block index
	tca := provider.NewToolCallAccumulator()
	var sentTtfb bool

	for messageStream.Next() && cs.Err() == nil {
		event := messageStream.Current()

		if !sentTtfb {
			// Set TTFB on first response
			generationMetrics := &aipb.GenerationMetrics{Ttfb: durationpb.New(time.Since(startTime))}
			cs.SendGenerationMetrics(ctx, generationMetrics)
			sentTtfb = true
		}

		switch variant := event.AsAny().(type) {
		case anthropic.MessageStartEvent:
			// Send initial usage metrics (input tokens)
			modelUsage := &aipb.ModelUsage{
				Model: request.Model,
			}
			if variant.Message.Usage.InputTokens > 0 {
				modelUsage.InputToken = &aipb.ResourceConsumption{
					Quantity: int32(variant.Message.Usage.InputTokens),
				}
			}
			if variant.Message.Usage.CacheReadInputTokens > 0 {
				modelUsage.InputCacheReadToken = &aipb.ResourceConsumption{
					Quantity: int32(variant.Message.Usage.CacheReadInputTokens),
				}
			}
			if variant.Message.Usage.CacheCreationInputTokens > 0 {
				modelUsage.InputCacheWriteToken = &aipb.ResourceConsumption{
					Quantity: int32(variant.Message.Usage.CacheCreationInputTokens),
				}
			}
			cs.SendModelUsage(ctx, modelUsage)

		case anthropic.ContentBlockStartEvent:
			switch contentBlockVariant := variant.ContentBlock.AsAny().(type) {
			case anthropic.ToolUseBlock:
				// If this is a tool_use block, start accumulating its arguments
				tca.Start(variant.Index, contentBlockVariant.ID, contentBlockVariant.Name)
				if request.GetConfiguration().GetStreamPartialToolCalls() {
					partialToolCall, err := tca.BuildPartial(variant.Index)
					if err != nil {
						return err
					}
					cs.SendPartialToolCall(ctx, partialToolCall)
				}

			case anthropic.TextBlock:
			case anthropic.ThinkingBlock:
			case anthropic.RedactedThinkingBlock:
			case anthropic.ServerToolUseBlock:
			case anthropic.WebSearchToolResultBlock:
			default:
				return grpc.Errorf(codes.Internal, "unknown variant type: %T", contentBlockVariant).Err()
			}

		case anthropic.ContentBlockDeltaEvent:
			switch delta := variant.Delta.AsAny().(type) {
			case anthropic.TextDelta:
				cs.SendContentChunk(ctx, delta.Text)

			case anthropic.ThinkingDelta:
				cs.SendReasoningChunk(ctx, delta.Thinking)

			case anthropic.InputJSONDelta:
				// Accumulate tool_use input JSON by content block index.
				tca.AppendArgs(variant.Index, delta.PartialJSON, nil)
				if request.GetConfiguration().GetStreamPartialToolCalls() {
					partialToolCall, err := tca.BuildPartial(variant.Index)
					if err != nil {
						return err
					}
					cs.SendPartialToolCall(ctx, partialToolCall)
				}
			}

		case anthropic.ContentBlockStopEvent:
			if tca.Has(variant.Index) {
				// If this content block was a tool_use, emit it now (with complete arguments)
				toolCall, err := tca.Build(variant.Index)
				if err != nil {
					return err
				}
				cs.SendToolCall(ctx, toolCall)
			}

		case anthropic.MessageDeltaEvent:
			// Output model usage.
			modelUsage := &aipb.ModelUsage{
				Model: request.Model,
			}
			if variant.Usage.OutputTokens > 0 {
				modelUsage.OutputToken = &aipb.ResourceConsumption{
					Quantity: int32(variant.Usage.OutputTokens),
				}
			}
			cs.SendModelUsage(ctx, modelUsage)

			// Stop reason.
			stopReason, ok := anthropicStopReasonToPb[variant.Delta.StopReason]
			if !ok {
				return grpc.Errorf(codes.Internal, "unknown stop reason: %s", variant.Delta.StopReason).Err()
			}
			cs.SendStopReason(ctx, stopReason)
		case anthropic.MessageStopEvent:
			generationMetrics := &aipb.GenerationMetrics{
				Ttlb: durationpb.New(time.Since(startTime)),
			}
			cs.SendGenerationMetrics(ctx, generationMetrics)

		default:
			return grpc.Errorf(codes.Internal, "unknown variant type: %T", variant).Err()
		}
	}

	if err := messageStream.Err(); err != nil {
		return fmt.Errorf("stream error: %w", err)
	}

	cs.Close()
	if err := cs.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func pbToolChoiceToAnthropic(toolChoice *aipb.ToolChoice) (anthropic.ToolChoiceUnionParam, error) {
	switch choice := toolChoice.Choice.(type) {
	case *aipb.ToolChoice_Mode:
		switch choice.Mode {
		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_NONE:
			return anthropic.ToolChoiceUnionParam{
				OfNone: &anthropic.ToolChoiceNoneParam{},
			}, nil

		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_AUTO:
			return anthropic.ToolChoiceUnionParam{
				OfAuto: &anthropic.ToolChoiceAutoParam{},
			}, nil

		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_REQUIRED:
			return anthropic.ToolChoiceUnionParam{
				OfAny: &anthropic.ToolChoiceAnyParam{},
			}, nil

		default:
			return anthropic.ToolChoiceUnionParam{}, fmt.Errorf("unknown tool choice mode: %s", choice.Mode)
		}

	case *aipb.ToolChoice_ToolName:
		return anthropic.ToolChoiceUnionParam{
			OfTool: &anthropic.ToolChoiceToolParam{
				Name: choice.ToolName,
			},
		}, nil

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

package anthropic

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/shared/constant"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/grpc"
)

func (c *Client) TextToTextStream(request *aiservicepb.TextToTextStreamRequest, srv aiservicepb.Ai_TextToTextStreamServer) error {
	ctx := srv.Context()

	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}

	// Extract system message if present
	var systemBlocks []anthropic.TextBlockParam
	messages := make([]anthropic.MessageParam, 0, len(request.Messages))

	for _, msg := range request.Messages {
		switch msg.Role {
		case aipb.Role_ROLE_SYSTEM:
			systemBlocks = append(systemBlocks, anthropic.TextBlockParam{
				Text: msg.Content,
			})

		case aipb.Role_ROLE_USER:
			messages = append(messages, anthropic.NewUserMessage(
				anthropic.NewTextBlock(msg.Content),
			))

		case aipb.Role_ROLE_ASSISTANT:
			var contentBlockParamUnions []anthropic.ContentBlockParamUnion
			if msg.Content != "" {
				contentBlockParamUnions = append(contentBlockParamUnions, anthropic.NewTextBlock(msg.Content))
			}
			// Add tool use content if present
			for _, tc := range msg.ToolCalls {
				contentBlockParamUnions = append(contentBlockParamUnions, anthropic.NewToolUseBlock(tc.Id, json.RawMessage(tc.Arguments), tc.Name))
			}
			messages = append(messages, anthropic.NewAssistantMessage(contentBlockParamUnions...))

		case aipb.Role_ROLE_TOOL:
			toolResultBlock := anthropic.NewToolResultBlock(msg.ToolCallId, msg.Content, false)
			// Anthropic passes tool results with role USER.
			messages = append(messages, anthropic.NewUserMessage(toolResultBlock))
		}
	}

	// Build the request
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

	// Add thinking configuration for reasoning models
	if model.Ttt.Reasoning {
		budget := pbReasoningEffortToAnthropicBudget(request.Configuration.GetReasoningEffort())
		if budget > 0 {
			messageParams.Thinking = anthropic.ThinkingConfigParamOfEnabled(budget)
		}
	}

	// Add tools if provided
	if len(request.Tools) > 0 {
		tools := make([]anthropic.ToolUnionParam, 0, len(request.Tools))
		for _, tool := range request.Tools {
			anthropicTool := pbToolToAnthropic(tool)
			tools = append(tools, anthropicTool)
		}
		messageParams.Tools = tools
	}

	startTime := time.Now()

	// Create streaming request
	messageStream := c.client.Messages.NewStreaming(ctx, messageParams)

	cs := provider.NewAsyncTextToTextContentSender(srv, 100)
	defer cs.Close()

	// Track active tool_use blocks by content block index
	type toolUseAcc struct {
		id   string
		name string
		args strings.Builder
	}
	toolUses := make(map[int64]*toolUseAcc)

	// Process stream events (consumer)
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
			// If this is a tool_use block, start accumulating its arguments
			switch contentBlockVariant := variant.ContentBlock.AsAny().(type) {
			case anthropic.ToolUseBlock:
				toolUses[variant.Index] = &toolUseAcc{
					id:   contentBlockVariant.ID,
					name: contentBlockVariant.Name,
				}
			case anthropic.TextBlock: // Nothing to do on these event types for now.
			case anthropic.ThinkingBlock:
			case anthropic.RedactedThinkingBlock:
			case anthropic.ServerToolUseBlock:
			case anthropic.WebSearchToolResultBlock:
			default:
				return grpc.Errorf(codes.Internal, "unknown variant type: %T", contentBlockVariant).Err()
			}

		case anthropic.ContentBlockDeltaEvent:
			// Handle different types of content deltas
			switch delta := variant.Delta.AsAny().(type) {
			case anthropic.TextDelta:
				cs.SendContentChunk(ctx, delta.Text)

			case anthropic.ThinkingDelta:
				cs.SendReasoningChunk(ctx, delta.Thinking)

			case anthropic.InputJSONDelta:
				// Accumulate tool_use input JSON by content block index
				if acc, ok := toolUses[variant.Index]; ok {
					acc.args.WriteString(delta.PartialJSON)
				}
			}

		case anthropic.ContentBlockStopEvent:
			// If this content block was a tool_use, emit it now (with complete arguments)
			if acc, ok := toolUses[variant.Index]; ok {
				toolCall := &aipb.ToolCall{
					Id:        acc.id,
					Name:      acc.name,
					Arguments: acc.args.String(),
				}
				cs.SendToolCall(ctx, toolCall)
				delete(toolUses, variant.Index)
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

func pbRoleToAnthropic(role aipb.Role) (anthropic.MessageParamRole, error) {
	switch role {
	case aipb.Role_ROLE_ASSISTANT:
		return anthropic.MessageParamRoleAssistant, nil
	case aipb.Role_ROLE_USER:
		return anthropic.MessageParamRoleUser, nil
	case aipb.Role_ROLE_TOOL:
		// Tool results are sent as user messages in Anthropic
		return anthropic.MessageParamRoleUser, nil
	default:
		return "", fmt.Errorf("unsupported role for Anthropic: %s", role)
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
	// Convert protobuf Struct to input schema
	toolParams := &anthropic.ToolParam{
		Name:        tool.Name,
		Description: anthropic.String(tool.Description),
		Type:        anthropic.ToolTypeCustom,
	}
	if tool.JsonSchema != nil {
		toolParams.InputSchema = anthropic.ToolInputSchemaParam{
			Properties: tool.JsonSchema.Properties,
			Required:   tool.JsonSchema.Required,
			Type:       constant.Object(tool.JsonSchema.Type),
		}
	} else {
		// Anthropic requires an input schema with a type and properties set.
		toolParams.InputSchema = anthropic.ToolInputSchemaParam{
			Type:       "object",
			Properties: map[string]*aipb.JsonSchema{},
		}
	}
	return anthropic.ToolUnionParam{
		OfTool: toolParams,
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

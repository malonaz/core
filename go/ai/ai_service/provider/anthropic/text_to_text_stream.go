package anthropic

import (
	"fmt"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
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
				contentBlockParamUnions = append(contentBlockParamUnions, anthropic.NewToolUseBlock(tc.Id, tc.Arguments, tc.Name))
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
		MaxTokens: request.Configuration.GetMaxTokens(),
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

	// Track usage metrics
	generationMetrics := &aipb.GenerationMetrics{}
	var inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens int64

	// Process stream events
	for messageStream.Next() {
		event := messageStream.Current()

		// Set TTFB on first response
		if generationMetrics.Ttfb == nil {
			generationMetrics.Ttfb = durationpb.New(time.Since(startTime))
		}

		switch variant := event.AsAny().(type) {
		case anthropic.MessageStartEvent:
			// Track initial usage from message start
			if variant.Message.Usage.InputTokens > 0 {
				inputTokens = variant.Message.Usage.InputTokens
			}
			if variant.Message.Usage.CacheReadInputTokens > 0 {
				cacheReadTokens = variant.Message.Usage.CacheReadInputTokens
			}
			if variant.Message.Usage.CacheCreationInputTokens > 0 {
				cacheWriteTokens = variant.Message.Usage.CacheCreationInputTokens
			}

		case anthropic.ContentBlockStartEvent:
			// No action needed for block start

		case anthropic.ContentBlockDeltaEvent:
			// Handle different types of content deltas
			switch delta := variant.Delta.AsAny().(type) {
			case anthropic.TextDelta:
				if err := srv.Send(&aiservicepb.TextToTextStreamResponse{
					Content: &aiservicepb.TextToTextStreamResponse_ContentChunk{
						ContentChunk: delta.Text,
					},
				}); err != nil {
					return fmt.Errorf("failed to send content chunk: %w", err)
				}

			case anthropic.ThinkingDelta:
				if err := srv.Send(&aiservicepb.TextToTextStreamResponse{
					Content: &aiservicepb.TextToTextStreamResponse_ReasoningChunk{
						ReasoningChunk: delta.Thinking,
					},
				}); err != nil {
					return fmt.Errorf("failed to send reasoning chunk: %w", err)
				}

			case anthropic.InputJSONDelta:
				// Tool use argument deltas - we'll send complete tool calls in ContentBlockStopEvent
				// Skip streaming partial JSON for now
			}

		case anthropic.ContentBlockStopEvent:
			// No action needed - tool calls are sent in MessageDeltaEvent

		case anthropic.MessageDeltaEvent:
			// Update output token count
			if variant.Usage.OutputTokens > 0 {
				outputTokens = variant.Usage.OutputTokens
			}

			// Handle stop reason if present
			if variant.Delta.StopReason != "" {
				// Message completed
			}

		case anthropic.MessageStopEvent:
			// Message stream ended - send final metrics
		}
	}

	if err := messageStream.Err(); err != nil {
		return fmt.Errorf("stream error: %w", err)
	}

	generationMetrics.Ttlb = durationpb.New(time.Since(startTime))

	// Send model usage metrics
	modelUsage := &aipb.ModelUsage{
		Model: request.Model,
		InputToken: &aipb.ResourceConsumption{
			Quantity: int32(inputTokens),
		},
		OutputToken: &aipb.ResourceConsumption{
			Quantity: int32(outputTokens),
		},
	}

	// Handle cache read tokens
	if cacheReadTokens > 0 {
		modelUsage.InputCacheReadToken = &aipb.ResourceConsumption{
			Quantity: int32(cacheReadTokens),
		}
		// Back out cached tokens from input tokens
		modelUsage.InputToken.Quantity -= int32(cacheReadTokens)
	}

	// Handle cache write tokens
	if cacheWriteTokens > 0 {
		modelUsage.InputCacheWriteToken = &aipb.ResourceConsumption{
			Quantity: int32(cacheWriteTokens),
		}
	}

	if err := srv.Send(&aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_ModelUsage{
			ModelUsage: modelUsage,
		},
	}); err != nil {
		return fmt.Errorf("failed to send model usage: %w", err)
	}

	if err := srv.Send(&aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_GenerationMetrics{
			GenerationMetrics: generationMetrics,
		},
	}); err != nil {
		return fmt.Errorf("failed to send generation metrics: %w", err)
	}

	return nil
}

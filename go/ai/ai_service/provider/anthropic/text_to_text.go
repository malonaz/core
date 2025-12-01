package anthropic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/shared/constant"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
)

func (c *Client) TextToText(ctx context.Context, request *aiservicepb.TextToTextRequest) (*aiservicepb.TextToTextResponse, error) {
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return nil, err
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

	// Add tool choice if provided
	/*
		if request.ToolChoice != "" {
			messageParams.ToolChoice = anthropic.ToolChoiceUnionParam{
				OfToolChoiceTool: &anthropic.ToolChoiceToolParam{
					Name: request.ToolChoice,
				},
			}
		}
	*/

	startTime := time.Now()
	messagesResponse, err := c.client.Messages.New(ctx, messageParams)
	if err != nil {
		return nil, fmt.Errorf("messages creation failed: %w", err)
	}

	// Build response message
	message := &aipb.Message{
		Role: aipb.Role_ROLE_ASSISTANT,
	}

	// Extract content from response
	for _, contentBlock := range messagesResponse.Content {
		switch variant := contentBlock.AsAny().(type) {
		case anthropic.TextBlock:
			if message.Content != "" {
				message.Content += "\n"
			}
			message.Content += variant.Text

		case anthropic.ThinkingBlock:
			message.Reasoning = variant.Thinking

		case anthropic.ToolUseBlock:
			// Convert arguments to JSON string
			argsJSON, err := json.Marshal(variant.Input)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal tool arguments: %w", err)
			}
			message.ToolCalls = append(message.ToolCalls, &aipb.ToolCall{
				Id:        variant.ID,
				Name:      variant.Name,
				Arguments: string(argsJSON),
			})
		}
	}

	// Build metrics
	generationMetrics := &aipb.GenerationMetrics{
		Ttlb: durationpb.New(time.Since(startTime)),
	}

	modelUsage := &aipb.ModelUsage{
		Model: request.Model,
		InputToken: &aipb.ResourceConsumption{
			Quantity: int32(messagesResponse.Usage.InputTokens),
		},
		OutputToken: &aipb.ResourceConsumption{
			Quantity: int32(messagesResponse.Usage.OutputTokens),
		},
	}

	// Handle cache read tokens
	if messagesResponse.Usage.CacheReadInputTokens > 0 {
		modelUsage.InputCacheReadToken = &aipb.ResourceConsumption{
			Quantity: int32(messagesResponse.Usage.CacheReadInputTokens),
		}
		// Back out cached tokens from input tokens
		modelUsage.InputToken.Quantity -= int32(messagesResponse.Usage.CacheReadInputTokens)
	}

	// Handle cache write tokens
	if messagesResponse.Usage.CacheCreationInputTokens > 0 {
		modelUsage.InputCacheWriteToken = &aipb.ResourceConsumption{
			Quantity: int32(messagesResponse.Usage.CacheCreationInputTokens),
		}
	}

	return &aiservicepb.TextToTextResponse{
		Message:           message,
		ModelUsage:        modelUsage,
		GenerationMetrics: generationMetrics,
	}, nil
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

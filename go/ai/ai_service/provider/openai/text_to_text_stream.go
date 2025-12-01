package openai

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	openai "github.com/sashabaranov/go-openai"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil"
)

func (c *Client) TextToTextStream(
	request *aiservicepb.TextToTextStreamRequest,
	stream aiservicepb.Ai_TextToTextStreamServer,
) error {
	ctx := stream.Context()
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}

	messages := make([]openai.ChatCompletionMessage, 0, len(request.Messages))
	for _, msg := range request.Messages {
		role, err := pbRoleToOpenAI(msg.Role)
		if err != nil {
			return err
		}
		message := openai.ChatCompletionMessage{
			Role:       role,
			Content:    msg.Content,
			ToolCallID: msg.ToolCallId,
		}
		for _, tc := range msg.ToolCalls {
			message.ToolCalls = append(message.ToolCalls, pbToolCallToOpenAI(tc))
		}
		messages = append(messages, message)
	}

	chatCompletionRequest := openai.ChatCompletionRequest{
		Model:               model.ProviderModelId,
		Messages:            messages,
		MaxCompletionTokens: int(request.Configuration.GetMaxTokens()),
		Temperature:         float32(request.Configuration.GetTemperature()),
		ReasoningEffort:     providerToReasoningEffortToOpenAI[c.ProviderId()][request.Configuration.GetReasoningEffort()],
		Stream:              true,
		StreamOptions: &openai.StreamOptions{
			IncludeUsage: true,
		},
	}

	if c.ProviderId() == providerIdGroq {
		if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED {
			chatCompletionRequest.ReasoningFormat = "parsed"
		}
	}

	// Add tools if provided
	for _, tool := range request.Tools {
		openaiTool, err := pbToolToOpenAI(tool)
		if err != nil {
			return fmt.Errorf("failed to convert tool %s: %w", tool.Name, err)
		}
		chatCompletionRequest.Tools = append(chatCompletionRequest.Tools, openaiTool)
	}

	// Add tool choice if provided
	if request.ToolChoice != "" {
		chatCompletionRequest.ToolChoice = openai.ToolChoice{
			Type: openai.ToolTypeFunction,
			Function: openai.ToolFunction{
				Name: request.ToolChoice,
			},
		}
	}

	startTime := time.Now()
	chatStream, err := c.client.CreateChatCompletionStream(ctx, chatCompletionRequest)
	if err != nil {
		return fmt.Errorf("chat completion stream failed: %w", err)
	}
	defer chatStream.Close()

	generationMetrics := &aipb.GenerationMetrics{}
	var promptTokens, completionTokens, reasoningTokens, cachedTokens int

	for {
		response, err := chatStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading stream: %w", err)
		}

		// Set TTFB on first response
		if generationMetrics.Ttfb == nil {
			generationMetrics.Ttfb = durationpb.New(time.Since(startTime))
		}

		// Accumulate usage stats if present
		if response.Usage != nil {
			promptTokens = response.Usage.PromptTokens
			completionTokens = response.Usage.CompletionTokens

			if response.Usage.CompletionTokensDetails != nil {
				reasoningTokens = response.Usage.CompletionTokensDetails.ReasoningTokens
			}

			if response.Usage.PromptTokensDetails != nil {
				cachedTokens = response.Usage.PromptTokensDetails.CachedTokens
			}
		}

		if len(response.Choices) == 0 {
			continue
		}
		choice := response.Choices[0]

		// Send content chunk
		if choice.Delta.Content != "" {
			if err := stream.Send(&aiservicepb.TextToTextStreamResponse{
				Content: &aiservicepb.TextToTextStreamResponse_ContentChunk{
					ContentChunk: choice.Delta.Content,
				},
			}); err != nil {
				return err
			}
		}

		// Send reasoning chunk
		if choice.Delta.ReasoningContent != "" {
			if err := stream.Send(&aiservicepb.TextToTextStreamResponse{
				Content: &aiservicepb.TextToTextStreamResponse_ReasoningChunk{
					ReasoningChunk: choice.Delta.ReasoningContent,
				},
			}); err != nil {
				return err
			}
		}

		// Handle Groq reasoning format
		if choice.Delta.Reasoning != "" {
			if err := stream.Send(&aiservicepb.TextToTextStreamResponse{
				Content: &aiservicepb.TextToTextStreamResponse_ReasoningChunk{
					ReasoningChunk: choice.Delta.Reasoning,
				},
			}); err != nil {
				return err
			}
		}
	}

	generationMetrics.Ttlb = durationpb.New(time.Since(startTime))

	// Send model usage
	modelUsage := &aipb.ModelUsage{
		Model: request.Model,
		InputToken: &aipb.ResourceConsumption{
			Quantity: int32(promptTokens),
		},
		OutputToken: &aipb.ResourceConsumption{
			Quantity: int32(completionTokens),
		},
	}

	if reasoningTokens > 0 {
		modelUsage.OutputReasoningToken = &aipb.ResourceConsumption{
			Quantity: int32(reasoningTokens),
		}
		// Back out reasoning tokens from output tokens
		modelUsage.OutputToken.Quantity -= int32(reasoningTokens)
	}

	if cachedTokens > 0 {
		modelUsage.InputCacheReadToken = &aipb.ResourceConsumption{
			Quantity: int32(cachedTokens),
		}
		// Back out cached tokens from input tokens
		modelUsage.InputToken.Quantity -= int32(cachedTokens)
	}

	if err := stream.Send(&aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_ModelUsage{
			ModelUsage: modelUsage,
		},
	}); err != nil {
		return err
	}

	// Send generation metrics
	if err := stream.Send(&aiservicepb.TextToTextStreamResponse{
		Content: &aiservicepb.TextToTextStreamResponse_GenerationMetrics{
			GenerationMetrics: generationMetrics,
		},
	}); err != nil {
		return err
	}

	return nil
}

func pbRoleToOpenAI(role aipb.Role) (string, error) {
	switch role {
	case aipb.Role_ROLE_SYSTEM:
		return openai.ChatMessageRoleSystem, nil
	case aipb.Role_ROLE_ASSISTANT:
		return openai.ChatMessageRoleAssistant, nil
	case aipb.Role_ROLE_USER:
		return openai.ChatMessageRoleUser, nil
	case aipb.Role_ROLE_TOOL:
		return openai.ChatMessageRoleTool, nil
	default:
		return "", fmt.Errorf("unknown role: %s", role)
	}
}

func pbToolToOpenAI(tool *aipb.Tool) (openai.Tool, error) {
	bytes, err := pbutil.JSONMarshal(tool.JsonSchema)
	if err != nil {
		return openai.Tool{}, fmt.Errorf("marshaling tool %s: %w", tool.Name, err)
	}
	return openai.Tool{
		Type: openai.ToolTypeFunction,
		Function: &openai.FunctionDefinition{
			Name:        tool.Name,
			Description: tool.Description,
			Parameters:  json.RawMessage(bytes),
		},
	}, nil
}

func pbToolCallToOpenAI(toolCall *aipb.ToolCall) openai.ToolCall {
	return openai.ToolCall{
		ID:   toolCall.Id,
		Type: openai.ToolTypeFunction,
		Function: openai.FunctionCall{
			Name:      toolCall.Name,
			Arguments: toolCall.Arguments,
		},
	}
}

var providerToReasoningEffortToOpenAI = map[string]map[aipb.ReasoningEffort]string{
	providerIdOpenai: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: "medium",
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     "low",
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  "medium",
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    "high",
	},
	providerIdGroq: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: "default",
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     "default",
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  "default",
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    "default",
	},
}

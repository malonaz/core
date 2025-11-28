package openai

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	openai "github.com/sashabaranov/go-openai"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/pbutil"
)

func (c *Client) TextToText(ctx context.Context, request *aiservicepb.TextToTextRequest) (*aiservicepb.TextToTextResponse, error) {
	if len(request.Messages) == 0 {
		return nil, fmt.Errorf("messages cannot be empty")
	}
	modelConfig, err := provider.GetModelConfig(request.Model)
	if err != nil {
		return nil, err
	}

	messages := make([]openai.ChatCompletionMessage, 0, len(request.Messages))
	for _, msg := range request.Messages {
		role, err := pbRoleToOpenAI(msg.Role)
		if err != nil {
			return nil, err
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
		Model:               modelConfig.ModelId,
		Messages:            messages,
		MaxCompletionTokens: int(request.Configuration.GetMaxTokens()),
		Temperature:         float32(request.Configuration.GetTemperature()),
		ReasoningEffort:     providerToReasoningEffortToOpenAI[c.Provider()][request.Configuration.GetReasoningEffort()],
	}
	if c.Provider() == aipb.Provider_PROVIDER_GROQ {
		if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED {
			chatCompletionRequest.ReasoningFormat = "parsed"
		}
	}

	// Add tools if provided
	for _, tool := range request.Tools {
		openaiTool, err := pbToolToOpenAI(tool)
		if err != nil {
			return nil, fmt.Errorf("failed to convert tool %s: %w", tool.Name, err)
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
	chatCompletionResponse, err := c.client.CreateChatCompletion(ctx, chatCompletionRequest)
	if err != nil {
		return nil, fmt.Errorf("chat completion failed: %w", err)
	}
	if len(chatCompletionResponse.Choices) == 0 {
		return nil, fmt.Errorf("no response choices returned")
	}
	choice := chatCompletionResponse.Choices[0]

	message := &aipb.Message{
		Role:      aipb.Role_ROLE_ASSISTANT,
		Content:   choice.Message.Content,
		Reasoning: choice.Message.ReasoningContent,
	}
	if c.Provider() == aipb.Provider_PROVIDER_GROQ {
		if choice.Message.Reasoning != "" {
			message.Reasoning = choice.Message.Reasoning
		}
	}

	// Handle tool calls in the response
	if len(choice.Message.ToolCalls) > 0 {
		toolCalls := make([]*aipb.ToolCall, 0, len(choice.Message.ToolCalls))
		for _, tc := range choice.Message.ToolCalls {
			toolCalls = append(toolCalls, &aipb.ToolCall{
				Id:        tc.ID,
				Name:      tc.Function.Name,
				Arguments: tc.Function.Arguments,
			})
		}
		message.ToolCalls = toolCalls
	}

	generationMetrics := &aipb.GenerationMetrics{
		Ttlb: durationpb.New(time.Since(startTime)),
	}
	modelUsage := &aipb.ModelUsage{
		Provider: c.Provider(),
		Model:    request.Model,
		InputToken: &aipb.ResourceConsumption{
			Quantity: int32(chatCompletionResponse.Usage.PromptTokens),
		},
		OutputToken: &aipb.ResourceConsumption{
			Quantity: int32(chatCompletionResponse.Usage.CompletionTokens),
		},
	}

	if chatCompletionResponse.Usage.CompletionTokensDetails != nil {
		reasoningTokens := int32(chatCompletionResponse.Usage.CompletionTokensDetails.ReasoningTokens)
		if reasoningTokens > 0 {
			// Store the rasoning token.
			modelUsage.OutputReasoningToken = &aipb.ResourceConsumption{
				Quantity: reasoningTokens,
			}
			// Back out the reasoning tokens from the output tokens.
			modelUsage.OutputToken.Quantity -= reasoningTokens
		}
	}

	if chatCompletionResponse.Usage.PromptTokensDetails != nil {
		cachedTokens := int32(chatCompletionResponse.Usage.PromptTokensDetails.CachedTokens)
		if cachedTokens > 0 {
			// Store the cached tokens.
			modelUsage.InputCacheReadToken = &aipb.ResourceConsumption{
				Quantity: cachedTokens,
			}
			// Back out the cached tokens from the input tokens.
			modelUsage.InputToken.Quantity -= cachedTokens
		}
	}

	return &aiservicepb.TextToTextResponse{
		Message:           message,
		ModelUsage:        modelUsage,
		GenerationMetrics: generationMetrics,
	}, nil
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

var providerToReasoningEffortToOpenAI = map[aipb.Provider]map[aipb.ReasoningEffort]string{
	aipb.Provider_PROVIDER_OPENAI: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: "medium",
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     "low",
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  "medium",
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    "high",
	},
	aipb.Provider_PROVIDER_GROQ: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: "default",
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     "default",
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  "default",
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    "default",
	},
}

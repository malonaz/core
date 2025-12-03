package openai

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	openai "github.com/sashabaranov/go-openai"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/grpc"
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
		if request.ToolChoice == "auto" {
			chatCompletionRequest.ToolChoice = "auto"
		} else {
			chatCompletionRequest.ToolChoice = openai.ToolChoice{
				Type: openai.ToolTypeFunction,
				Function: openai.ToolFunction{
					Name: request.ToolChoice,
				},
			}
		}
	}

	startTime := time.Now()
	chatStream, err := c.client.CreateChatCompletionStream(ctx, chatCompletionRequest)
	if err != nil {
		return fmt.Errorf("chat completion stream failed: %w", err)
	}
	defer chatStream.Close()

	cs := provider.NewAsyncTextToTextContentSender(stream, 100)
	defer cs.Close()

	var sentTtfb bool

	// Track active tool calls being accumulated
	type toolCallAcc struct {
		id       string
		name     string
		args     string
		function openai.FunctionCall
	}
	toolCalls := make(map[int]*toolCallAcc)

	var stopReason aiservicepb.TextToTextStopReason
	for cs.Err() == nil {
		response, err := chatStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading stream: %w", err)
		}

		// Set TTFB on first response
		if !sentTtfb {
			generationMetrics := &aipb.GenerationMetrics{Ttfb: durationpb.New(time.Since(startTime))}
			cs.SendGenerationMetrics(ctx, generationMetrics)
			sentTtfb = true
		}

		// Accumulate usage stats if present
		if response.Usage != nil {
			modelUsage := &aipb.ModelUsage{
				Model: request.Model,
			}
			if response.Usage.PromptTokens > 0 {
				modelUsage.InputToken = &aipb.ResourceConsumption{
					Quantity: int32(response.Usage.PromptTokens),
				}
			}
			if response.Usage.CompletionTokens > 0 {
				modelUsage.OutputToken = &aipb.ResourceConsumption{
					Quantity: int32(response.Usage.CompletionTokens),
				}
			}
			if response.Usage.CompletionTokensDetails != nil && response.Usage.CompletionTokensDetails.ReasoningTokens > 0 {
				modelUsage.OutputReasoningToken = &aipb.ResourceConsumption{
					Quantity: int32(response.Usage.CompletionTokensDetails.ReasoningTokens),
				}
			}
			if response.Usage.PromptTokensDetails != nil && response.Usage.PromptTokensDetails.CachedTokens > 0 {
				modelUsage.InputCacheReadToken = &aipb.ResourceConsumption{
					Quantity: int32(response.Usage.PromptTokensDetails.CachedTokens),
				}
			}
			cs.SendModelUsage(ctx, modelUsage)
		}

		if len(response.Choices) == 0 {
			continue
		}
		choice := response.Choices[0]

		// Send content chunk
		if choice.Delta.Content != "" {
			cs.SendContentChunk(ctx, choice.Delta.Content)
		}

		// Send reasoning chunk (OpenAI reasoning models)
		if choice.Delta.ReasoningContent != "" {
			cs.SendReasoningChunk(ctx, choice.Delta.ReasoningContent)
		}

		// Handle Groq reasoning format
		if choice.Delta.Reasoning != "" {
			cs.SendReasoningChunk(ctx, choice.Delta.Reasoning)
		}

		// Handle tool calls
		for _, toolCall := range choice.Delta.ToolCalls {
			acc, exists := toolCalls[*toolCall.Index]
			if !exists {
				acc = &toolCallAcc{
					id:   toolCall.ID,
					name: toolCall.Function.Name,
				}
				toolCalls[*toolCall.Index] = acc
			}

			// Accumulate function arguments
			acc.args += toolCall.Function.Arguments
		}

		// Handle finish reason / stop reason
		if choice.FinishReason != "" {
			var ok bool
			stopReason, ok = openAIFinishReasonToPb[choice.FinishReason]
			if !ok {
				return grpc.Errorf(codes.Internal, "unknown finish reason: %s", choice.FinishReason).Err()
			}
		}
	}

	// Send any accumulated tool calls
	for _, acc := range toolCalls {
		toolCall := &aipb.ToolCall{
			Id:        acc.id,
			Name:      acc.name,
			Arguments: acc.args,
		}
		cs.SendToolCall(ctx, toolCall)
	}

	// Send stop reason
	if stopReason != aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_UNSPECIFIED {
		cs.SendStopReason(ctx, stopReason)
	}

	// Send final generation metrics
	generationMetrics := &aipb.GenerationMetrics{
		Ttlb: durationpb.New(time.Since(startTime)),
	}
	cs.SendGenerationMetrics(ctx, generationMetrics)

	cs.Close()
	if err := cs.Wait(ctx); err != nil {
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

var openAIFinishReasonToPb = map[openai.FinishReason]aiservicepb.TextToTextStopReason{
	openai.FinishReasonStop:          aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
	openai.FinishReasonLength:        aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_MAX_TOKENS,
	openai.FinishReasonToolCalls:     aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_TOOL_CALL,
	openai.FinishReasonContentFilter: aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
}

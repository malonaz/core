package openai

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/shared"
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

	// Build messages
	messages := make([]openai.ChatCompletionMessageParamUnion, 0, len(request.Messages))
	for _, msg := range request.Messages {
		message, err := pbMessageToOpenAIV2(msg)
		if err != nil {
			return err
		}
		messages = append(messages, message)
	}

	// Build the request params
	params := openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(model.ProviderModelId),
		Messages: messages,
		StreamOptions: openai.ChatCompletionStreamOptionsParam{
			IncludeUsage: openai.Bool(true),
		},
	}

	// Set max tokens if provided
	if request.GetConfiguration().GetMaxTokens() > 0 {
		if c.ProviderId() == providerIdOpenai {
			params.MaxCompletionTokens = openai.Int(int64(request.GetConfiguration().GetMaxTokens()))
		} else {
			params.MaxTokens = openai.Int(int64(request.GetConfiguration().GetMaxTokens()))
		}
	}

	// Set temperature if provided
	if request.GetConfiguration().GetTemperature() > 0 {
		params.Temperature = openai.Float(request.GetConfiguration().GetTemperature())
	}

	// Set reasoning effort if provided (for reasoning models)
	if request.GetConfiguration().GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED {
		reasoningEffort, err := pbReasoningEffortToOpenAIV2(c.ProviderId(), request.GetConfiguration().GetReasoningEffort())
		if err != nil {
			return grpc.Errorf(codes.Internal, "parsing reasoning effort: %v", err).Err()
		}
		if reasoningEffort != "" {
			params.ReasoningEffort = shared.ReasoningEffort(reasoningEffort)
		}
	}

	// Groq Reasoning.
	if c.ProviderId() == providerIdGroq {
		if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED {
			params.SetExtraFields(map[string]any{
				"reasoning_format": "parsed",
			})
		}
	}

	// Google reasoning.
	if c.ProviderId() == providerIdGoogle {
		thinkingConfig, err := buildGoogleThinkingConfig(model, request.Configuration.GetReasoningEffort())
		if err != nil {
			return grpc.Errorf(codes.Internal, "building Google thinking config: %v", err).Err()
		}
		if thinkingConfig != nil {
			params.SetExtraFields(map[string]any{
				"extra_body": map[string]any{
					"google": map[string]any{
						"thinking_config": thinkingConfig,
					},
				},
			})
		}
	}

	// Add tools if provided
	if len(request.Tools) > 0 {
		params.Tools = make([]openai.ChatCompletionToolUnionParam, 0, len(request.Tools))
		for _, tool := range request.Tools {
			openaiTool, err := pbToolToOpenAIV2(tool)
			if err != nil {
				return grpc.Errorf(codes.InvalidArgument, "tool: %v", err).Err()
			}
			params.Tools = append(params.Tools, openaiTool)
		}
	}

	// Add tool choice if provided
	if request.GetConfiguration().GetToolChoice() != nil {
		toolChoice, err := pbToolChoiceToOpenAIV2(request.GetConfiguration().GetToolChoice())
		if err != nil {
			return grpc.Errorf(codes.InvalidArgument, "tool choice: %v", err).Err()

		}
		params.ToolChoice = toolChoice
	}

	startTime := time.Now()
	chatStream := c.client2.Chat.Completions.NewStreaming(ctx, params)

	cs := provider.NewAsyncTextToTextContentSender(stream, 100)
	defer cs.Close()

	var sentTtfb bool

	// Track active tool calls being accumulated
	type toolCallAcc struct {
		id   string
		name string
		args string
	}
	toolCalls := make(map[int64]*toolCallAcc)

	var stopReason aiservicepb.TextToTextStopReason

	for chatStream.Next() {
		if cs.Err() != nil {
			break
		}

		chunk := chatStream.Current()

		// Set TTFB on first response
		if !sentTtfb {
			generationMetrics := &aipb.GenerationMetrics{Ttfb: durationpb.New(time.Since(startTime))}
			cs.SendGenerationMetrics(ctx, generationMetrics)
			sentTtfb = true
		}

		// Handle usage stats if present
		if chunk.Usage.PromptTokens > 0 || chunk.Usage.CompletionTokens > 0 {
			modelUsage := &aipb.ModelUsage{
				Model: request.Model,
			}
			if chunk.Usage.PromptTokens > 0 {
				modelUsage.InputToken = &aipb.ResourceConsumption{
					Quantity: int32(chunk.Usage.PromptTokens),
				}
			}
			if chunk.Usage.CompletionTokens > 0 {
				modelUsage.OutputToken = &aipb.ResourceConsumption{
					Quantity: int32(chunk.Usage.CompletionTokens),
				}
			}
			if chunk.Usage.CompletionTokensDetails.ReasoningTokens > 0 {
				modelUsage.OutputReasoningToken = &aipb.ResourceConsumption{
					Quantity: int32(chunk.Usage.CompletionTokensDetails.ReasoningTokens),
				}
			}
			if chunk.Usage.PromptTokensDetails.CachedTokens > 0 {
				modelUsage.InputCacheReadToken = &aipb.ResourceConsumption{
					Quantity: int32(chunk.Usage.PromptTokensDetails.CachedTokens),
				}
			}
			cs.SendModelUsage(ctx, modelUsage)
		}

		if len(chunk.Choices) == 0 {
			continue
		}
		choice := chunk.Choices[0]

		// Send content chunk
		if choice.Delta.Content != "" {
			var sendAsReasoningChunk bool
			// Check for extra_content field
			if extraContentRaw := choice.Delta.JSON.ExtraFields["extra_content"].Raw(); extraContentRaw != "" {
				var extraContent map[string]any
				if err := json.Unmarshal([]byte(extraContentRaw), &extraContent); err != nil {
					return grpc.Errorf(codes.Internal, "unmarshaling extra_content: %v", err).Err()
				}

				// Check if this is a Google thought/reasoning chunk
				if c.ProviderId() == providerIdGoogle {
					if google, ok := extraContent["google"].(map[string]any); ok {
						if thought, ok := google["thought"].(bool); ok && thought {
							sendAsReasoningChunk = true
						}
					}
				}
			}
			if sendAsReasoningChunk {
				content := strings.TrimPrefix(choice.Delta.Content, "<thought>")
				cs.SendReasoningChunk(ctx, content)
			} else {
				content := strings.TrimPrefix(choice.Delta.Content, "</thought>")
				cs.SendContentChunk(ctx, content)
			}
		}

		if reasoningChunk := choice.Delta.JSON.ExtraFields["reasoning"].Raw(); reasoningChunk != "" {
			unquoted, err := strconv.Unquote(reasoningChunk)
			if err != nil {
				return grpc.Errorf(codes.Internal, "unquoting chunk: %v", err).Err()
			}
			cs.SendReasoningChunk(ctx, unquoted)
		}
		if reasoningChunk := choice.Delta.JSON.ExtraFields["reasoning_content"].Raw(); reasoningChunk != "" {
			cs.SendReasoningChunk(ctx, reasoningChunk)
		}

		// Handle tool calls
		for _, toolCall := range choice.Delta.ToolCalls {
			idx := toolCall.Index
			acc, exists := toolCalls[idx]
			if !exists {
				acc = &toolCallAcc{
					id:   toolCall.ID,
					name: toolCall.Function.Name,
				}
				toolCalls[idx] = acc
			}

			// Update ID and name if provided (they may come in first chunk)
			if toolCall.ID != "" {
				acc.id = toolCall.ID
			}
			if toolCall.Function.Name != "" {
				acc.name = toolCall.Function.Name
			}

			// Accumulate function arguments
			acc.args += toolCall.Function.Arguments
		}

		// Handle finish reason / stop reason
		if choice.FinishReason != "" {
			var ok bool
			stopReason, ok = openAIFinishReasonToPbV2[string(choice.FinishReason)]
			if !ok {
				return grpc.Errorf(codes.Internal, "unknown finish reason: %s", choice.FinishReason).Err()
			}
		}
	}

	if err := chatStream.Err(); err != nil {
		return fmt.Errorf("error reading stream: %w", err)
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

func pbMessageToOpenAIV2(msg *aipb.Message) (openai.ChatCompletionMessageParamUnion, error) {
	switch msg.Role {
	case aipb.Role_ROLE_SYSTEM:
		return openai.SystemMessage(msg.Content), nil

	case aipb.Role_ROLE_USER:
		return openai.UserMessage(msg.Content), nil

	case aipb.Role_ROLE_ASSISTANT:
		params := &openai.ChatCompletionAssistantMessageParam{}
		if msg.Content != "" {
			params.Content = openai.ChatCompletionAssistantMessageParamContentUnion{
				OfString: openai.String(msg.Content),
			}
		}

		// For assistant messages with tool calls, we need to build the full param
		if len(msg.ToolCalls) > 0 {
			params.ToolCalls = make([]openai.ChatCompletionMessageToolCallUnionParam, 0, len(msg.ToolCalls))
			for _, tc := range msg.ToolCalls {
				params.ToolCalls = append(params.ToolCalls, openai.ChatCompletionMessageToolCallUnionParam{
					OfFunction: &openai.ChatCompletionMessageFunctionToolCallParam{
						ID: tc.Id,
						Function: openai.ChatCompletionMessageFunctionToolCallFunctionParam{
							Name:      tc.Name,
							Arguments: tc.Arguments,
						},
					},
				})
			}
		}
		return openai.AssistantMessage(msg.Content), nil

	case aipb.Role_ROLE_TOOL:
		return openai.ToolMessage(msg.Content, msg.ToolCallId), nil

	default:
		return openai.ChatCompletionMessageParamUnion{}, fmt.Errorf("unknown role: %s", msg.Role)
	}
}

func pbToolToOpenAIV2(tool *aipb.Tool) (openai.ChatCompletionToolUnionParam, error) {
	bytes, err := pbutil.JSONMarshal(tool.JsonSchema)
	if err != nil {
		return openai.ChatCompletionToolUnionParam{}, fmt.Errorf("marshaling tool %s: %w", tool.Name, err)
	}

	var parameters shared.FunctionParameters
	if err := json.Unmarshal(bytes, &parameters); err != nil {
		return openai.ChatCompletionToolUnionParam{}, fmt.Errorf("unmarshaling tool parameters %s: %w", tool.Name, err)
	}

	return openai.ChatCompletionToolUnionParam{
		OfFunction: &openai.ChatCompletionFunctionToolParam{
			Function: shared.FunctionDefinitionParam{
				Name:        tool.Name,
				Description: openai.String(tool.Description),
				Parameters:  parameters,
			},
		},
	}, nil
}

func pbToolChoiceToOpenAIV2(toolChoice *aipb.ToolChoice) (openai.ChatCompletionToolChoiceOptionUnionParam, error) {
	switch choice := toolChoice.Choice.(type) {
	case *aipb.ToolChoice_Mode:
		switch choice.Mode {
		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_NONE:
			return openai.ChatCompletionToolChoiceOptionUnionParam{
				OfAuto: openai.String(string(openai.ChatCompletionToolChoiceOptionAutoNone)),
			}, nil
		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_AUTO:
			return openai.ChatCompletionToolChoiceOptionUnionParam{
				OfAuto: openai.String(string(openai.ChatCompletionToolChoiceOptionAutoAuto)),
			}, nil
		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_REQUIRED:
			return openai.ChatCompletionToolChoiceOptionUnionParam{
				OfAuto: openai.String(string(openai.ChatCompletionToolChoiceOptionAutoRequired)),
			}, nil
		default:
			// TOOL_CHOICE_MODE_UNSPECIFIED or unknown - return empty/none
			return openai.ChatCompletionToolChoiceOptionUnionParam{}, fmt.Errorf("unknown mode: %s", choice.Mode)
		}

	case *aipb.ToolChoice_ToolName:
		// Specific function name
		return openai.ChatCompletionToolChoiceOptionUnionParam{
			OfFunctionToolChoice: &openai.ChatCompletionNamedToolChoiceParam{
				Function: openai.ChatCompletionNamedToolChoiceFunctionParam{
					Name: choice.ToolName,
				},
			},
		}, nil

	default:
		// No choice set
		return openai.ChatCompletionToolChoiceOptionUnionParam{}, fmt.Errorf("unknown choice:  %T", choice)
	}
}

func pbReasoningEffortToOpenAIV2(providerId string, effort aipb.ReasoningEffort) (shared.ReasoningEffort, error) {
	reasoningEffortToOpenAIV2, ok := providerToReasoningEffortToOpenAIV2[providerId]
	if !ok {
		return "", fmt.Errorf("unknown provider: %s", providerId)
	}
	reasoningEffort, ok := reasoningEffortToOpenAIV2[effort]
	if !ok {
		return "", fmt.Errorf("unknown reasoning effort: %s", effort)
	}
	return reasoningEffort, nil
}

// TODO(malon): map the new ones ( 'none', 'minimal', 'xhigh').
var providerToReasoningEffortToOpenAIV2 = map[string]map[aipb.ReasoningEffort]shared.ReasoningEffort{
	providerIdOpenai: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: shared.ReasoningEffortMedium,
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     shared.ReasoningEffortLow,
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  shared.ReasoningEffortMedium,
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    shared.ReasoningEffortHigh,
	},
	providerIdGoogle: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: "", // Google has its own custom fields.
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     "",
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  "",
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    "",
	},
	providerIdGroq: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: "default",
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     "default",
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  "default",
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    "default",
	},
	providerIdCerebras: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: "",
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     "",
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  "",
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    "",
	},
	providerIdXai: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: shared.ReasoningEffortMedium,
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     shared.ReasoningEffortLow,
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  shared.ReasoningEffortMedium,
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    shared.ReasoningEffortHigh,
	},
}

var openAIFinishReasonToPbV2 = map[string]aiservicepb.TextToTextStopReason{
	"stop":           aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
	"length":         aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_MAX_TOKENS,
	"tool_calls":     aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_TOOL_CALL,
	"function_call":  aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_TOOL_CALL, // Deprecated.
	"content_filter": aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
}

// buildGoogleThinkingConfig constructs the Google-specific thinking configuration
// based on the model's reasoning effort type and the requested reasoning effort level.
func buildGoogleThinkingConfig(model *aipb.Model, reasoningEffort aipb.ReasoningEffort) (map[string]any, error) {
	if reasoningEffort == aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED {
		return nil, nil
	}

	thinkingConfig := map[string]any{
		"include_thoughts": true,
	}

	// Determine if this model uses thinking_level or thinking_budget
	reasoningEffortType := model.GetProviderSettings().GetFields()[providerGoogleReasoningEffortType].GetStringValue()

	switch reasoningEffort {
	case aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT:
		if reasoningEffortType == providerGoogleReasoningEffortTypeThinkingLevel {
			thinkingConfig["thinking_level"] = providerGoogleThinkingLevelHigh
		} else if reasoningEffortType == providerGoogleReasoningEffortTypeThinkingBudget {
			thinkingConfig["thinking_budget"] = providerGoogleThinkingBudgetMedium
		}

	case aipb.ReasoningEffort_REASONING_EFFORT_LOW:
		if reasoningEffortType == providerGoogleReasoningEffortTypeThinkingLevel {
			thinkingConfig["thinking_level"] = providerGoogleThinkingLevelLow
		} else if reasoningEffortType == providerGoogleReasoningEffortTypeThinkingBudget {
			thinkingConfig["thinking_budget"] = providerGoogleThinkingBudgetLow
		}

	case aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:
		if reasoningEffortType == providerGoogleReasoningEffortTypeThinkingLevel {
			thinkingConfig["thinking_level"] = providerGoogleThinkingLevelHigh
		} else if reasoningEffortType == providerGoogleReasoningEffortTypeThinkingBudget {
			thinkingConfig["thinking_budget"] = providerGoogleThinkingBudgetMedium
		}

	case aipb.ReasoningEffort_REASONING_EFFORT_HIGH:
		if reasoningEffortType == providerGoogleReasoningEffortTypeThinkingLevel {
			thinkingConfig["thinking_level"] = providerGoogleThinkingLevelHigh
		} else if reasoningEffortType == providerGoogleReasoningEffortTypeThinkingBudget {
			thinkingConfig["thinking_budget"] = providerGoogleThinkingBudgetHigh
		}

	default:
		return nil, fmt.Errorf("unknown reasoning effort: %v", reasoningEffort)
	}

	return thinkingConfig, nil
}

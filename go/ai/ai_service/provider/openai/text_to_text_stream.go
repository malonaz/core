package openai

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/respjson"
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

	messages := make([]openai.ChatCompletionMessageParamUnion, 0, len(request.Messages))
	for i, msg := range request.Messages {
		message, err := pbMessageToOpenAI(msg)
		if err != nil {
			return grpc.Errorf(codes.InvalidArgument, "message [%d]: %v", i, err).Err()
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
		if c.ProviderId() == provider.Openai {
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
		reasoningEffort, err := pbReasoningEffortToOpenAI(c.ProviderId(), request.GetConfiguration().GetReasoningEffort())
		if err != nil {
			return grpc.Errorf(codes.Internal, "parsing reasoning effort: %v", err).Err()
		}
		if reasoningEffort != "" {
			params.ReasoningEffort = shared.ReasoningEffort(reasoningEffort)
		}
	}

	// Groq Reasoning.
	if c.ProviderId() == provider.Groq {
		if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED {
			params.SetExtraFields(map[string]any{
				"reasoning_format": "parsed",
			})
		}
	}

	// Google reasoning.
	if c.ProviderId() == provider.Google {
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
		for i, tool := range request.Tools {
			openaiTool, err := pbToolToOpenAI(tool)
			if err != nil {
				return grpc.Errorf(codes.InvalidArgument, "tool [%d]: %v", i, err).Err()
			}
			params.Tools = append(params.Tools, openaiTool)
		}
	}

	if request.GetConfiguration().GetToolChoice() != nil {
		toolChoice, err := pbToolChoiceToOpenAI(request.GetConfiguration().GetToolChoice())
		if err != nil {
			return grpc.Errorf(codes.InvalidArgument, "tool choice: %v", err).Err()
		}
		params.ToolChoice = toolChoice
	}

	startTime := time.Now()
	chatStream := c.client2.Chat.Completions.NewStreaming(ctx, params)

	cs := provider.NewAsyncTextToTextContentSender(stream, 100)
	defer cs.Close()

	// Track active tool_use blocks by content block index
	tca := provider.NewToolCallAccumulator()
	var sentTtfb bool
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

		// Extract choice (if any).
		var choice *openai.ChatCompletionChunkChoice
		if len(chunk.Choices) != 0 {
			choice = &chunk.Choices[0]
		}

		// Extract extra content if any.
		if choice != nil {
			var extraContent map[string]any
			if extraContentRaw := choice.Delta.JSON.ExtraFields["extra_content"].Raw(); extraContentRaw != "" {
				if err := json.Unmarshal([]byte(extraContentRaw), &extraContent); err != nil {
					return grpc.Errorf(codes.Internal, "unmarshaling extra_content: %v", err).Err()
				}
			}

			// Merge reasoning field into reasoning content.
			if reasoningChunk := choice.Delta.JSON.ExtraFields["reasoning"].Raw(); reasoningChunk != "" {
				unquoted, err := strconv.Unquote(reasoningChunk)
				if err != nil {
					return grpc.Errorf(codes.Internal, "unquoting reasoning chunk: %v", err).Err()
				}
				choice.Delta.JSON.ExtraFields["reasoning_content"] = respjson.NewField(unquoted)
			}

			// Handle google thought format.
			if c.ProviderId() == provider.Google {
				if google, ok := extraContent["google"].(map[string]any); ok {
					// Correct choice content.
					if thought, ok := google["thought"].(bool); ok && thought {
						content := strings.TrimPrefix(choice.Delta.Content, "<thought>")
						choice.Delta.Content = ""
						choice.Delta.JSON.ExtraFields["reasoning_content"] = respjson.NewField(content)
					}
				}
			}
		}

		// Handle usage stats if present
		if chunk.Usage.PromptTokens > 0 || chunk.Usage.PromptTokensDetails.CachedTokens > 0 || chunk.Usage.CompletionTokens > 0 || chunk.Usage.CompletionTokensDetails.ReasoningTokens > 0 {
			modelUsage := &aipb.ModelUsage{
				Model: request.Model,
			}

			// Input tokens (excluding cached).
			if chunk.Usage.PromptTokens > 0 {
				inputTokens := chunk.Usage.PromptTokens
				if chunk.Usage.PromptTokensDetails.CachedTokens > 0 {
					inputTokens -= chunk.Usage.PromptTokensDetails.CachedTokens
				}
				if inputTokens > 0 {
					modelUsage.InputToken = &aipb.ResourceConsumption{
						Quantity: int32(inputTokens),
					}
				}
			}

			// Input cached tokens.
			if chunk.Usage.PromptTokensDetails.CachedTokens > 0 {
				modelUsage.InputCacheReadToken = &aipb.ResourceConsumption{
					Quantity: int32(chunk.Usage.PromptTokensDetails.CachedTokens),
				}
			}

			// Output tokens (excluding reasoning)
			if chunk.Usage.CompletionTokens > 0 {
				outputTokens := chunk.Usage.CompletionTokens
				if chunk.Usage.CompletionTokensDetails.ReasoningTokens > 0 {
					outputTokens -= chunk.Usage.CompletionTokensDetails.ReasoningTokens
				}
				if outputTokens > 0 {
					modelUsage.OutputToken = &aipb.ResourceConsumption{
						Quantity: int32(outputTokens),
					}
				}
			}

			// Reasoning tokens.
			inferredReasoningTokens := int32(chunk.Usage.TotalTokens) - modelUsage.GetInputToken().GetQuantity() - modelUsage.GetOutputToken().GetQuantity()
			if chunk.Usage.CompletionTokensDetails.ReasoningTokens > 0 {
				if int32(chunk.Usage.CompletionTokensDetails.ReasoningTokens) != inferredReasoningTokens {
					return grpc.Errorf(
						codes.Internal, "reasoning tokens doesn't match inferred value: inferred %d, got %d",
						inferredReasoningTokens, chunk.Usage.CompletionTokensDetails.ReasoningTokens,
					).Err()
				}
			}
			if inferredReasoningTokens > 0 {
				modelUsage.OutputReasoningToken = &aipb.ResourceConsumption{
					Quantity: int32(inferredReasoningTokens),
				}
			}

			cs.SendModelUsage(ctx, modelUsage)
		}

		if choice == nil {
			continue
		}

		// Send content chunk
		if choice.Delta.Content != "" {
			content := strings.TrimPrefix(choice.Delta.Content, "</thought>")
			cs.SendContentChunk(ctx, content)
		}

		if reasoningChunk := choice.Delta.JSON.ExtraFields["reasoning_content"].Raw(); reasoningChunk != "" {
			cs.SendReasoningChunk(ctx, reasoningChunk)
		}

		// Handle tool calls.
		for _, toolCall := range choice.Delta.ToolCalls {
			tca.StartOrUpdate(toolCall.Index, toolCall.ID, toolCall.Function.Name)
			tca.AppendArgs(toolCall.Index, toolCall.Function.Arguments)
		}

		// Send complete tool calls.
		toolCalls, err := tca.BuildComplete()
		if err != nil {
			return err
		}
		cs.SendToolCall(ctx, toolCalls...)

		// Handle stop reason.
		if choice.FinishReason != "" {
			var ok bool
			stopReason, ok = openAIFinishReasonToPb[string(choice.FinishReason)]
			if !ok {
				return grpc.Errorf(codes.Internal, "unknown finish reason: %s", choice.FinishReason).Err()
			}
		}
	}

	if err := chatStream.Err(); err != nil {
		return fmt.Errorf("reading stream: %w", err)
	}

	// Send remaining tool calls.
	toolCalls, err := tca.BuildRemaining()
	if err != nil {
		return err
	}
	cs.SendToolCall(ctx, toolCalls...)

	// Send stop reason.
	if stopReason != aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_UNSPECIFIED {
		cs.SendStopReason(ctx, stopReason)
	}

	// Send final generation metrics.
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

func pbMessageToOpenAI(msg *aipb.Message) (openai.ChatCompletionMessageParamUnion, error) {
	switch m := msg.Message.(type) {
	case *aipb.Message_System:
		return openai.SystemMessage(m.System.Content), nil

	case *aipb.Message_User:
		return openai.UserMessage(m.User.Content), nil

	case *aipb.Message_Assistant:
		content := m.Assistant.Content
		if m.Assistant.StructuredContent != nil {
			bytes, err := pbutil.JSONMarshalStruct(m.Assistant.StructuredContent)
			if err != nil {
				return openai.ChatCompletionMessageParamUnion{}, fmt.Errorf("marshaling structured content: %w", err)
			}
			content = string(bytes)
		}
		if len(m.Assistant.ToolCalls) == 0 {
			return openai.AssistantMessage(content), nil
		}
		toolCalls := make([]openai.ChatCompletionMessageToolCallUnionParam, 0, len(m.Assistant.ToolCalls))
		for _, tc := range m.Assistant.ToolCalls {
			argsBytes, err := pbutil.JSONMarshalStruct(tc.Arguments)
			if err != nil {
				return openai.ChatCompletionMessageParamUnion{}, fmt.Errorf("marshaling tool call arguments: %w", err)
			}
			toolCalls = append(toolCalls, openai.ChatCompletionMessageToolCallUnionParam{
				OfFunction: &openai.ChatCompletionMessageFunctionToolCallParam{
					ID: tc.Id,
					Function: openai.ChatCompletionMessageFunctionToolCallFunctionParam{
						Name:      tc.Name,
						Arguments: string(argsBytes),
					},
				},
			})
		}
		return openai.ChatCompletionMessageParamUnion{
			OfAssistant: &openai.ChatCompletionAssistantMessageParam{
				Content: openai.ChatCompletionAssistantMessageParamContentUnion{
					OfString: openai.String(content),
				},
				ToolCalls: toolCalls,
			},
		}, nil

	case *aipb.Message_Tool:
		content, err := toolResultToContent(m.Tool.Result)
		if err != nil {
			return openai.ChatCompletionMessageParamUnion{}, fmt.Errorf("converting tool result: %w", err)
		}
		return openai.ToolMessage(content, m.Tool.ToolCallId), nil

	default:
		return openai.ChatCompletionMessageParamUnion{}, fmt.Errorf("unknown message type: %T", m)
	}
}

func toolResultToContent(result *aipb.ToolResult) (string, error) {
	switch r := result.Result.(type) {
	case *aipb.ToolResult_Content:
		return r.Content, nil
	case *aipb.ToolResult_StructuredContent:
		jsonBytes, err := pbutil.JSONMarshalStruct(r.StructuredContent)
		if err != nil {
			return "", err
		}
		return string(jsonBytes), nil
	case *aipb.ToolResult_Error:
		return r.Error, nil
	default:
		return "", fmt.Errorf("unknown tool result type: %T", r)
	}
}

func pbToolToOpenAI(tool *aipb.Tool) (openai.ChatCompletionToolUnionParam, error) {
	bytes, err := pbutil.JSONMarshal(tool.JsonSchema)
	if err != nil {
		return openai.ChatCompletionToolUnionParam{}, fmt.Errorf("marshaling json schema: %w", err)
	}

	var parameters shared.FunctionParameters
	if err := json.Unmarshal(bytes, &parameters); err != nil {
		return openai.ChatCompletionToolUnionParam{}, fmt.Errorf("unmarshaling parameters: %w", err)
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

func pbToolChoiceToOpenAI(toolChoice *aipb.ToolChoice) (openai.ChatCompletionToolChoiceOptionUnionParam, error) {
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
			return openai.ChatCompletionToolChoiceOptionUnionParam{}, fmt.Errorf("unknown mode: %s", choice.Mode)
		}

	case *aipb.ToolChoice_ToolName:
		return openai.ChatCompletionToolChoiceOptionUnionParam{
			OfFunctionToolChoice: &openai.ChatCompletionNamedToolChoiceParam{
				Function: openai.ChatCompletionNamedToolChoiceFunctionParam{
					Name: choice.ToolName,
				},
			},
		}, nil

	default:
		return openai.ChatCompletionToolChoiceOptionUnionParam{}, fmt.Errorf("unknown choice type: %T", choice)
	}
}

func pbReasoningEffortToOpenAI(providerId string, effort aipb.ReasoningEffort) (shared.ReasoningEffort, error) {
	reasoningEffortMap, ok := providerToReasoningEffortMap[providerId]
	if !ok {
		return "", fmt.Errorf("unknown provider: %s", providerId)
	}
	reasoningEffort, ok := reasoningEffortMap[effort]
	if !ok {
		return "", fmt.Errorf("unknown reasoning effort: %s", effort)
	}
	return reasoningEffort, nil
}

var providerToReasoningEffortMap = map[string]map[aipb.ReasoningEffort]shared.ReasoningEffort{
	provider.Openai: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: shared.ReasoningEffortMedium,
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     shared.ReasoningEffortLow,
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  shared.ReasoningEffortMedium,
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    shared.ReasoningEffortHigh,
	},
	provider.Google: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: "",
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     "",
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  "",
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    "",
	},
	provider.Groq: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: "default",
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     "default",
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  "default",
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    "default",
	},
	provider.Cerebras: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: "",
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     "",
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  "",
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    "",
	},
	provider.Xai: {
		aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: shared.ReasoningEffortMedium,
		aipb.ReasoningEffort_REASONING_EFFORT_LOW:     shared.ReasoningEffortLow,
		aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  shared.ReasoningEffortMedium,
		aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    shared.ReasoningEffortHigh,
	},
}

var openAIFinishReasonToPb = map[string]aiservicepb.TextToTextStopReason{
	string(openai.CompletionChoiceFinishReasonStop):          aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
	string(openai.CompletionChoiceFinishReasonLength):        aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_MAX_TOKENS,
	string(openai.CompletionChoiceFinishReasonContentFilter): aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
	"tool_calls":    aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_TOOL_CALL,
	"function_call": aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_TOOL_CALL, // Deprecated.
}

func buildGoogleThinkingConfig(model *aipb.Model, reasoningEffort aipb.ReasoningEffort) (map[string]any, error) {
	thinkingConfigKey := model.GetProviderSettings().GetFields()["thinking_config_key"].GetStringValue()
	if thinkingConfigKey == "" {
		return nil, nil
	}
	thinkingConfigValue := model.GetProviderSettings().GetFields()[reasoningEffort.String()].AsInterface()
	if thinkingConfigValue == nil {
		return nil, fmt.Errorf("missing provider config for reasoning effort %s", reasoningEffort)
	}

	return map[string]any{
		"include_thoughts": true,
		thinkingConfigKey:  thinkingConfigValue,
	}, nil
}

package openai

import (
	"encoding/base64"
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
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/pbutil"
)

const (
	blockTypeText    = "text"
	blockTypeThought = "thought"
)

func (c *Client) TextToTextStream(
	request *aiservicepb.TextToTextStreamRequest,
	stream aiservicepb.AiService_TextToTextStreamServer,
) error {
	ctx := stream.Context()
	getModelRequest := &aiservicepb.GetModelRequest{Name: request.Model}
	model, err := c.modelService.GetModel(ctx, getModelRequest)
	if err != nil {
		return err
	}

	var messages []openai.ChatCompletionMessageParamUnion
	for i, msg := range request.Messages {
		converted, err := pbMessageToOpenAI(msg)
		if err != nil {
			return grpc.Errorf(codes.InvalidArgument, "message [%d]: %v", i, err).Err()
		}
		messages = append(messages, converted...)
	}

	params := openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(model.ProviderModelId),
		Messages: messages,
		StreamOptions: openai.ChatCompletionStreamOptionsParam{
			IncludeUsage: openai.Bool(true),
		},
	}

	if request.GetConfiguration().GetMaxTokens() > 0 {
		if c.ProviderId() == provider.Openai {
			params.MaxCompletionTokens = openai.Int(int64(request.GetConfiguration().GetMaxTokens()))
		} else {
			params.MaxTokens = openai.Int(int64(request.GetConfiguration().GetMaxTokens()))
		}
	}

	if request.GetConfiguration().GetTemperature() > 0 {
		params.Temperature = openai.Float(request.GetConfiguration().GetTemperature())
	}

	if request.GetConfiguration().GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED {
		reasoningEffort, err := pbReasoningEffortToOpenAI(c.ProviderId(), request.GetConfiguration().GetReasoningEffort())
		if err != nil {
			return grpc.Errorf(codes.Internal, "parsing reasoning effort: %v", err).Err()
		}
		if reasoningEffort != "" {
			params.ReasoningEffort = shared.ReasoningEffort(reasoningEffort)
		}
	}

	if c.ProviderId() == provider.Groq {
		if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED {
			params.SetExtraFields(map[string]any{
				"reasoning_format": "parsed",
			})
		}
	}

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

	tca := provider.NewToolCallAccumulator()

	// Dynamic block indexing.
	var currentBlockIndex int64 = -1
	var currentBlockType string
	var toolCallIDSet = map[string]struct{}{}

	var sentTtfb bool
	var stopReason aiservicepb.TextToTextStopReason

	for chatStream.Next() {
		if cs.Err() != nil {
			break
		}

		chunk := chatStream.Current()

		if !sentTtfb {
			cs.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{Ttfb: durationpb.New(time.Since(startTime))})
			sentTtfb = true
		}

		var choice *openai.ChatCompletionChunkChoice
		if len(chunk.Choices) != 0 {
			choice = &chunk.Choices[0]
		}

		if choice != nil {
			if reasoningChunk := choice.Delta.JSON.ExtraFields["reasoning"].Raw(); reasoningChunk != "" {
				unquoted, err := strconv.Unquote(reasoningChunk)
				if err != nil {
					return grpc.Errorf(codes.Internal, "unquoting reasoning chunk: %v", err).Err()
				}
				choice.Delta.JSON.ExtraFields["reasoning_content"] = respjson.NewField(unquoted)
			}
		}

		if chunk.Usage.PromptTokens > 0 || chunk.Usage.PromptTokensDetails.CachedTokens > 0 || chunk.Usage.CompletionTokens > 0 || chunk.Usage.CompletionTokensDetails.ReasoningTokens > 0 {
			modelUsage := &aipb.ModelUsage{Model: request.Model}

			if chunk.Usage.PromptTokens > 0 {
				inputTokens := chunk.Usage.PromptTokens
				if chunk.Usage.PromptTokensDetails.CachedTokens > 0 {
					inputTokens -= chunk.Usage.PromptTokensDetails.CachedTokens
				}
				if inputTokens > 0 {
					modelUsage.InputToken = &aipb.ResourceConsumption{Quantity: int32(inputTokens)}
				}
			}

			if chunk.Usage.PromptTokensDetails.CachedTokens > 0 {
				modelUsage.InputTokenCacheRead = &aipb.ResourceConsumption{Quantity: int32(chunk.Usage.PromptTokensDetails.CachedTokens)}
			}

			if chunk.Usage.CompletionTokens > 0 {
				outputTokens := chunk.Usage.CompletionTokens
				if chunk.Usage.CompletionTokensDetails.ReasoningTokens > 0 {
					outputTokens -= chunk.Usage.CompletionTokensDetails.ReasoningTokens
				}
				if outputTokens > 0 {
					modelUsage.OutputToken = &aipb.ResourceConsumption{Quantity: int32(outputTokens)}
				}
			}

			inferredReasoningTokens := int32(chunk.Usage.TotalTokens) - modelUsage.GetInputToken().GetQuantity() - modelUsage.GetInputTokenCacheRead().GetQuantity() - modelUsage.GetOutputToken().GetQuantity()
			if chunk.Usage.CompletionTokensDetails.ReasoningTokens > 0 {
				if int32(chunk.Usage.CompletionTokensDetails.ReasoningTokens) != inferredReasoningTokens {
					return grpc.Errorf(
						codes.Internal, "reasoning tokens doesn't match inferred value: inferred %d, got %d",
						inferredReasoningTokens, chunk.Usage.CompletionTokensDetails.ReasoningTokens,
					).Err()
				}
			}
			if inferredReasoningTokens > 0 {
				modelUsage.OutputReasoningToken = &aipb.ResourceConsumption{Quantity: inferredReasoningTokens}
			}

			cs.SendModelUsage(ctx, modelUsage)
		}

		if choice == nil {
			continue
		}

		if choice.Delta.Content != "" {
			if currentBlockType != blockTypeText {
				currentBlockIndex++
				currentBlockType = blockTypeText
			}
			cs.SendBlocks(ctx, &aipb.Block{
				Index:   currentBlockIndex,
				Content: &aipb.Block_Text{Text: choice.Delta.Content},
			})
		}

		if reasoningChunk := choice.Delta.JSON.ExtraFields["reasoning_content"].Raw(); reasoningChunk != "" {
			if currentBlockType != blockTypeThought {
				currentBlockIndex++
				currentBlockType = blockTypeThought
			}
			cs.SendBlocks(ctx, &aipb.Block{
				Index:   currentBlockIndex,
				Content: &aipb.Block_Thought{Thought: reasoningChunk},
			})
		}

		for _, toolCall := range choice.Delta.ToolCalls {
			if _, ok := toolCallIDSet[toolCall.ID]; !ok {
				currentBlockIndex++
				toolCallIDSet[toolCall.ID] = struct{}{}
			}
			tca.StartOrUpdate(currentBlockIndex, toolCall.ID, toolCall.Function.Name)
			tca.AppendArgs(currentBlockIndex, toolCall.Function.Arguments)
			if request.GetConfiguration().GetStreamPartialToolCalls() {
				block, err := tca.BuildPartial(currentBlockIndex)
				if err != nil {
					return err
				}
				cs.SendBlocks(ctx, block)
			}
		}

		toolCallBlocks, err := tca.BuildComplete()
		if err != nil {
			return err
		}
		cs.SendBlocks(ctx, toolCallBlocks...)

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

	remainingToolCallBlocks, err := tca.BuildRemaining()
	if err != nil {
		return err
	}
	cs.SendBlocks(ctx, remainingToolCallBlocks...)

	if stopReason != aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_UNSPECIFIED {
		cs.SendStopReason(ctx, stopReason)
	}

	cs.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{Ttlb: durationpb.New(time.Since(startTime))})

	cs.Close()
	if err := cs.Wait(ctx); err != nil {
		return err
	}

	return nil
}

func pbMessageToOpenAI(msg *aipb.Message) ([]openai.ChatCompletionMessageParamUnion, error) {
	switch msg.Role {
	case aipb.Role_ROLE_SYSTEM:
		var texts []string
		for i, block := range msg.Blocks {
			switch content := block.Content.(type) {
			case *aipb.Block_Text:
				texts = append(texts, content.Text)
			default:
				return nil, fmt.Errorf("block [%d]: unexpected block type %T for SYSTEM role", i, content)
			}
		}
		return []openai.ChatCompletionMessageParamUnion{openai.SystemMessage(strings.Join(texts, "\n"))}, nil

	case aipb.Role_ROLE_USER:
		var contentParts []openai.ChatCompletionContentPartUnionParam
		for i, block := range msg.Blocks {
			switch content := block.Content.(type) {
			case *aipb.Block_Text:
				contentParts = append(contentParts, openai.TextContentPart(content.Text))
			case *aipb.Block_Image:
				img := content.Image
				var url string
				switch s := img.Source.(type) {
				case *aipb.Image_Url:
					url = s.Url
				case *aipb.Image_Data:
					url = fmt.Sprintf("data:%s;base64,%s", img.MediaType, base64.StdEncoding.EncodeToString(s.Data))
				default:
					return nil, fmt.Errorf("block [%d]: unknown image source type %T", i, s)
				}
				detail, ok := imageQualityToOpenAI[img.Quality]
				if !ok {
					return nil, fmt.Errorf("block [%d]: unknown image quality: %s", i, img.Quality)
				}
				contentParts = append(contentParts, openai.ImageContentPart(openai.ChatCompletionContentPartImageImageURLParam{
					URL:    url,
					Detail: detail,
				}))
			default:
				return nil, fmt.Errorf("block [%d]: unexpected block type %T for USER role", i, content)
			}
		}
		return []openai.ChatCompletionMessageParamUnion{openai.UserMessage(contentParts)}, nil

	case aipb.Role_ROLE_ASSISTANT:
		ofAssistant := &openai.ChatCompletionAssistantMessageParam{}
		var textParts []string
		for i, block := range msg.Blocks {
			switch content := block.Content.(type) {
			case *aipb.Block_Text:
				textParts = append(textParts, content.Text)
			case *aipb.Block_Thought:
				// OpenAI doesn't support thought blocks in input
			case *aipb.Block_ToolCall:
				tc := content.ToolCall
				argsBytes, err := pbutil.JSONMarshal(tc.Arguments)
				if err != nil {
					return nil, fmt.Errorf("block [%d]: marshaling tool call arguments: %w", i, err)
				}
				toolCall := openai.ChatCompletionMessageToolCallUnionParam{
					OfFunction: &openai.ChatCompletionMessageFunctionToolCallParam{
						ID: tc.Id,
						Function: openai.ChatCompletionMessageFunctionToolCallFunctionParam{
							Name:      tc.Name,
							Arguments: string(argsBytes),
						},
					},
				}
				if tc.ExtraFields != nil {
					toolCall.OfFunction.SetExtraFields(map[string]any{"extra_content": tc.ExtraFields.AsMap()})
				}
				ofAssistant.ToolCalls = append(ofAssistant.ToolCalls, toolCall)
			case *aipb.Block_Image:
				return nil, fmt.Errorf("block [%d]: images not supported in assistant messages", i)
			default:
				return nil, fmt.Errorf("block [%d]: unexpected block type %T for ASSISTANT role", i, content)
			}
		}
		if len(textParts) > 0 {
			ofAssistant.Content = openai.ChatCompletionAssistantMessageParamContentUnion{
				OfString: openai.String(strings.Join(textParts, "")),
			}
		}
		return []openai.ChatCompletionMessageParamUnion{{OfAssistant: ofAssistant}}, nil

	case aipb.Role_ROLE_TOOL:
		var messages []openai.ChatCompletionMessageParamUnion
		for i, block := range msg.Blocks {
			switch content := block.Content.(type) {
			case *aipb.Block_ToolResult:
				tr := content.ToolResult
				text, err := ai.ParseToolResult(tr)
				if err != nil {
					return nil, fmt.Errorf("block [%d]: converting tool result: %w", i, err)
				}
				messages = append(messages, openai.ToolMessage(text, tr.ToolCallId))
			default:
				return nil, fmt.Errorf("block [%d]: unexpected block type %T for TOOL role", i, content)
			}
		}
		return messages, nil

	default:
		return nil, fmt.Errorf("unexpected role: %v", msg.Role)
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
	"function_call": aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_TOOL_CALL,
}

var imageQualityToOpenAI = map[aipb.ImageQuality]string{
	aipb.ImageQuality_IMAGE_QUALITY_UNSPECIFIED: "auto",
	aipb.ImageQuality_IMAGE_QUALITY_AUTO:        "auto",
	aipb.ImageQuality_IMAGE_QUALITY_LOW:         "low",
	aipb.ImageQuality_IMAGE_QUALITY_HIGH:        "high",
}

package google

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/genai"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/pbutil"
)

func (c *Client) TextToTextStream(
	request *aiservicepb.TextToTextStreamRequest,
	stream aiservicepb.AiService_TextToTextStreamServer,
) error {
	ctx := stream.Context()

	model, err := c.modelService.GetModel(ctx, &aiservicepb.GetModelRequest{Name: request.Model})
	if err != nil {
		return err
	}

	contents, systemInstruction, err := buildContents(request.Messages)
	if err != nil {
		return grpc.Errorf(codes.InvalidArgument, "building contents: %v", err).Err()
	}

	config := &genai.GenerateContentConfig{}

	if systemInstruction != "" {
		config.SystemInstruction = &genai.Content{
			Parts: []*genai.Part{{Text: systemInstruction}},
		}
	}

	if imageConfig := request.GetConfiguration().GetImageConfig(); imageConfig != nil {
		config.ResponseModalities = []string{string(genai.MediaModalityText), string(genai.MediaModalityImage)}
		config.ImageConfig = &genai.ImageConfig{
			AspectRatio: imageConfig.GetAspectRatio(),
			ImageSize:   imageConfig.GetImageSize(),
		}
	}

	if request.GetConfiguration().GetMaxTokens() > 0 {
		config.MaxOutputTokens = int32(request.GetConfiguration().GetMaxTokens())
	}

	if request.GetConfiguration().GetTemperature() > 0 {
		temp := float32(request.GetConfiguration().GetTemperature())
		config.Temperature = &temp
	}

	if model.GetTtt().GetReasoning() {
		thinkingConfig, err := buildThinkingConfig(model, request.GetConfiguration().GetReasoningEffort())
		if err != nil {
			return grpc.Errorf(codes.InvalidArgument, "building thinking config: %v", err).Err()
		}
		config.ThinkingConfig = thinkingConfig
	}

	if len(request.Tools) > 0 {
		tools, err := buildTools(request.Tools)
		if err != nil {
			return grpc.Errorf(codes.InvalidArgument, "building tools: %v", err).Err()
		}
		config.Tools = tools

		if request.GetConfiguration().GetToolChoice() != nil {
			toolConfig, err := buildToolConfig(request.GetConfiguration().GetToolChoice())
			if err != nil {
				return grpc.Errorf(codes.InvalidArgument, "building tool config: %v", err).Err()
			}
			config.ToolConfig = toolConfig
		}
	}

	startTime := time.Now()
	iter := c.client.Models.GenerateContentStream(ctx, model.ProviderModelId, contents, config)

	cs := provider.NewAsyncTextToTextContentSender(stream, 100)
	defer cs.Close()

	tca := provider.NewToolCallAccumulator()

	var sentTtfb bool
	var stopReason aiservicepb.TextToTextStopReason
	var toolCallIndex int64

	for resp, err := range iter {
		if err != nil {
			return grpc.Errorf(codes.Internal, "reading stream: %v", err).Err()
		}

		if err := cs.Err(); err != nil {
			return grpc.Errorf(codes.Internal, "error sending content: %v", err).Err()
		}

		if !sentTtfb {
			cs.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{
				Ttfb: durationpb.New(time.Since(startTime)),
			})
			sentTtfb = true
		}

		for _, candidate := range resp.Candidates {
			if candidate.Content == nil {
				continue
			}

			for _, part := range candidate.Content.Parts {
				if err := processPart(ctx, cs, tca, part, request.GetConfiguration().GetStreamPartialToolCalls(), &toolCallIndex); err != nil {
					return err
				}
			}

			if candidate.FinishReason != genai.FinishReasonUnspecified {
				var ok bool
				stopReason, ok = finishReasonToPb[candidate.FinishReason]
				if !ok {
					return grpc.Errorf(codes.Internal, "unknown finish reason: %v", candidate.FinishReason).Err()
				}
			}
		}

		if resp.UsageMetadata != nil {
			modelUsage := buildModelUsage(request.Model, resp.UsageMetadata)
			if modelUsage != nil {
				cs.SendModelUsage(ctx, modelUsage)
			}
		}
	}

	toolCalls, err := tca.BuildRemaining()
	if err != nil {
		return err
	}
	cs.SendToolCall(ctx, toolCalls...)

	if stopReason != aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_UNSPECIFIED {
		cs.SendStopReason(ctx, stopReason)
	}

	cs.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{
		Ttlb: durationpb.New(time.Since(startTime)),
	})

	cs.Close()
	return cs.Wait(ctx)
}

func processPart(
	ctx context.Context,
	cs *provider.AsyncTextToTextContentSender,
	tca *provider.ToolCallAccumulator,
	part *genai.Part,
	streamPartialToolCalls bool,
	toolCallIndex *int64,
) error {
	if part.Text != "" {
		if part.Thought {
			cs.SendReasoningChunk(ctx, part.Text)
		} else {
			cs.SendContentChunk(ctx, part.Text)
		}
	}

	// In processPart, handle InlineData:
	if part.InlineData != nil {
		cs.SendGeneratedImage(ctx, &aipb.Image{
			Source:    &aipb.Image_Data{Data: part.InlineData.Data},
			MediaType: part.InlineData.MIMEType,
		})
	}

	if part.FunctionCall != nil {
		argsJSON, err := json.Marshal(part.FunctionCall.Args)
		if err != nil {
			return grpc.Errorf(codes.Internal, "marshaling function call args: %v", err).Err()
		}

		var extraFields *structpb.Struct
		if len(part.ThoughtSignature) > 0 {
			extraFields = &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"thought_signature": structpb.NewStringValue(base64.StdEncoding.EncodeToString(part.ThoughtSignature)),
				},
			}
		}

		toolCallID := fmt.Sprintf("call_%s_%d", part.FunctionCall.Name, time.Now().UnixNano())

		idx := *toolCallIndex
		*toolCallIndex++
		tca.Start(idx, toolCallID, part.FunctionCall.Name)
		tca.AppendArgs(idx, string(argsJSON), extraFields)

		toolCall, err := tca.Build(idx)
		if err != nil {
			return err
		}
		cs.SendToolCall(ctx, toolCall)
	}

	return nil
}

func buildContents(messages []*aipb.Message) ([]*genai.Content, string, error) {
	var contents []*genai.Content
	var systemInstruction string

	for i, msg := range messages {
		switch m := msg.Message.(type) {
		case *aipb.Message_System:
			if systemInstruction != "" {
				systemInstruction += "\n\n"
			}
			systemInstruction += m.System.Content

		case *aipb.Message_User:
			parts, err := buildUserParts(m.User.ContentBlocks)
			if err != nil {
				return nil, "", fmt.Errorf("message [%d]: %w", i, err)
			}
			contents = append(contents, &genai.Content{
				Role:  genai.RoleUser,
				Parts: parts,
			})

		case *aipb.Message_Assistant:
			parts, err := buildAssistantParts(m.Assistant)
			if err != nil {
				return nil, "", fmt.Errorf("message [%d]: %w", i, err)
			}
			contents = append(contents, &genai.Content{
				Role:  genai.RoleModel,
				Parts: parts,
			})

		case *aipb.Message_Tool:
			content, err := buildToolResultContent(m.Tool)
			if err != nil {
				return nil, "", fmt.Errorf("message [%d]: %w", i, err)
			}
			contents = append(contents, content)

		default:
			return nil, "", fmt.Errorf("message [%d]: unknown message type %T", i, m)
		}
	}

	return contents, systemInstruction, nil
}

func buildUserParts(blocks []*aipb.ContentBlock) ([]*genai.Part, error) {
	parts := make([]*genai.Part, 0, len(blocks))

	for j, block := range blocks {
		switch content := block.GetContent().(type) {
		case *aipb.ContentBlock_Text:
			parts = append(parts, &genai.Part{Text: content.Text})

		case *aipb.ContentBlock_Image:
			part, err := buildImagePart(content.Image)
			if err != nil {
				return nil, fmt.Errorf("content block [%d]: %w", j, err)
			}
			parts = append(parts, part)

		default:
			return nil, fmt.Errorf("content block [%d]: unknown type %T", j, content)
		}
	}

	return parts, nil
}

func buildImagePart(img *aipb.Image) (*genai.Part, error) {
	switch source := img.GetSource().(type) {
	case *aipb.Image_Data:
		if img.MediaType == "" {
			return nil, fmt.Errorf("media_type required for image data")
		}
		return &genai.Part{
			InlineData: &genai.Blob{
				MIMEType: img.MediaType,
				Data:     source.Data,
			},
		}, nil

	case *aipb.Image_Url:
		if len(source.Url) > 5 && source.Url[:5] == "data:" {
			return &genai.Part{
				InlineData: &genai.Blob{
					MIMEType: img.MediaType,
					Data:     []byte(source.Url),
				},
			}, nil
		}
		return &genai.Part{
			FileData: &genai.FileData{
				FileURI:  source.Url,
				MIMEType: img.MediaType,
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown image source type: %T", source)
	}
}

func buildAssistantParts(assistant *aipb.AssistantMessage) ([]*genai.Part, error) {
	var parts []*genai.Part

	if assistant.Reasoning != "" {
		parts = append(parts, &genai.Part{
			Text:    assistant.Reasoning,
			Thought: true,
		})
	}

	if assistant.Content != "" {
		parts = append(parts, &genai.Part{Text: assistant.Content})
	}

	if assistant.StructuredContent != nil {
		bytes, err := pbutil.JSONMarshal(assistant.StructuredContent)
		if err != nil {
			return nil, fmt.Errorf("marshaling structured content: %w", err)
		}
		parts = append(parts, &genai.Part{Text: string(bytes)})
	}

	for _, image := range assistant.Images {
		imagePart, err := buildImagePart(image)
		if err != nil {
			return nil, grpc.Errorf(codes.Internal, "building image part: %v", err).Err()
		}
		parts = append(parts, imagePart)
	}
	if assistant.StructuredContent != nil {
		bytes, err := pbutil.JSONMarshal(assistant.StructuredContent)
		if err != nil {
			return nil, fmt.Errorf("marshaling structured content: %w", err)
		}
		parts = append(parts, &genai.Part{Text: string(bytes)})
	}

	for j, tc := range assistant.ToolCalls {
		part := &genai.Part{
			FunctionCall: &genai.FunctionCall{
				Name: tc.Name,
				Args: tc.Arguments.AsMap(),
			},
		}

		if tc.ExtraFields != nil {
			if sig, ok := tc.ExtraFields.Fields["thought_signature"]; ok {
				decoded, err := base64.StdEncoding.DecodeString(sig.GetStringValue())
				if err != nil {
					return nil, fmt.Errorf("decoding thought signature: %v", sig)
				}
				part.ThoughtSignature = []byte(decoded)
			}
		}

		if part.FunctionCall.Name == "" {
			return nil, fmt.Errorf("tool call [%d]: missing name", j)
		}

		parts = append(parts, part)
	}

	return parts, nil
}

func buildToolResultContent(tool *aipb.ToolResultMessage) (*genai.Content, error) {
	content, err := ai.ParseToolResult(tool.Result)
	if err != nil {
		return nil, fmt.Errorf("parsing tool result: %w", err)
	}
	key := "output"
	if tool.Result.GetError() != nil {
		key = "error"
	}
	functionResponse := &genai.FunctionResponse{
		ID:       tool.ToolCallId,
		Name:     tool.ToolName,
		Response: map[string]any{key: content},
	}

	return &genai.Content{
		Role: genai.RoleUser,
		Parts: []*genai.Part{
			{FunctionResponse: functionResponse},
		},
	}, nil
}

func buildTools(tools []*aipb.Tool) ([]*genai.Tool, error) {
	functionDeclarations := make([]*genai.FunctionDeclaration, 0, len(tools))

	for _, tool := range tools {
		fd := &genai.FunctionDeclaration{
			Name:                 tool.Name,
			Description:          tool.Description,
			ParametersJsonSchema: tool.JsonSchema,
		}
		functionDeclarations = append(functionDeclarations, fd)
	}

	return []*genai.Tool{
		{FunctionDeclarations: functionDeclarations},
	}, nil
}

func buildToolConfig(toolChoice *aipb.ToolChoice) (*genai.ToolConfig, error) {
	switch choice := toolChoice.Choice.(type) {
	case *aipb.ToolChoice_Mode:
		switch choice.Mode {
		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_NONE:
			return &genai.ToolConfig{
				FunctionCallingConfig: &genai.FunctionCallingConfig{
					Mode: genai.FunctionCallingConfigModeNone,
				},
			}, nil

		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_AUTO:
			return &genai.ToolConfig{
				FunctionCallingConfig: &genai.FunctionCallingConfig{
					Mode: genai.FunctionCallingConfigModeAuto,
				},
			}, nil

		case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_REQUIRED:
			return &genai.ToolConfig{
				FunctionCallingConfig: &genai.FunctionCallingConfig{
					Mode: genai.FunctionCallingConfigModeAny,
				},
			}, nil

		default:
			return nil, fmt.Errorf("unknown tool choice mode: %v", choice.Mode)
		}

	case *aipb.ToolChoice_ToolName:
		return &genai.ToolConfig{
			FunctionCallingConfig: &genai.FunctionCallingConfig{
				Mode:                 genai.FunctionCallingConfigModeAny,
				AllowedFunctionNames: []string{choice.ToolName},
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown tool choice type: %T", choice)
	}
}

func buildThinkingConfig(model *aipb.Model, reasoningEffort aipb.ReasoningEffort) (*genai.ThinkingConfig, error) {
	providerSettings := model.GetProviderSettings()
	if providerSettings == nil {
		return nil, fmt.Errorf("missing provider_settings for model %s", model.Name)
	}

	thinkingConfigKey := providerSettings.GetFields()["thinking_config_key"].GetStringValue()
	if thinkingConfigKey == "" {
		return nil, fmt.Errorf("missing thinking_config_key in provider_settings for model %s", model.Name)
	}

	configValue := providerSettings.GetFields()[reasoningEffort.String()]
	if configValue == nil {
		return nil, fmt.Errorf("missing provider config for reasoning effort %s", reasoningEffort)
	}

	config := &genai.ThinkingConfig{IncludeThoughts: true}

	switch thinkingConfigKey {
	case "thinking_budget":
		budget := int32(configValue.GetNumberValue())
		config.ThinkingBudget = &budget

	case "thinking_level":
		level := configValue.GetStringValue()
		config.ThinkingLevel = genai.ThinkingLevel(level)

	default:
		return nil, fmt.Errorf("unknown thinking_config_key: %s", thinkingConfigKey)
	}

	return config, nil
}

func buildModelUsage(modelName string, usage *genai.GenerateContentResponseUsageMetadata) *aipb.ModelUsage {
	if usage == nil {
		return nil
	}

	modelUsage := &aipb.ModelUsage{
		Model: modelName,
	}

	if usage.PromptTokenCount > 0 {
		inputTokens := usage.PromptTokenCount
		if usage.CachedContentTokenCount > 0 {
			inputTokens -= usage.CachedContentTokenCount
		}
		if inputTokens > 0 {
			modelUsage.InputToken = &aipb.ResourceConsumption{
				Quantity: int32(inputTokens),
			}
		}
	}

	if usage.CachedContentTokenCount > 0 {
		modelUsage.InputCacheReadToken = &aipb.ResourceConsumption{
			Quantity: int32(usage.CachedContentTokenCount),
		}
	}

	if usage.CandidatesTokenCount > 0 {
		outputTokens := usage.CandidatesTokenCount
		if usage.ThoughtsTokenCount > 0 {
			outputTokens -= usage.ThoughtsTokenCount
		}
		if outputTokens > 0 {
			modelUsage.OutputToken = &aipb.ResourceConsumption{
				Quantity: int32(outputTokens),
			}
		}
	}

	if usage.ThoughtsTokenCount > 0 {
		modelUsage.OutputReasoningToken = &aipb.ResourceConsumption{
			Quantity: int32(usage.ThoughtsTokenCount),
		}
	}

	return modelUsage
}

var reasoningEffortToBudget = map[aipb.ReasoningEffort]int{
	aipb.ReasoningEffort_REASONING_EFFORT_DEFAULT: 4096,
	aipb.ReasoningEffort_REASONING_EFFORT_LOW:     1024,
	aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM:  4096,
	aipb.ReasoningEffort_REASONING_EFFORT_HIGH:    16384,
}

var finishReasonToPb = map[genai.FinishReason]aiservicepb.TextToTextStopReason{
	genai.FinishReason(""):                   aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
	genai.FinishReasonStop:                   aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
	genai.FinishReasonMaxTokens:              aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_MAX_TOKENS,
	genai.FinishReasonSafety:                 aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
	genai.FinishReasonRecitation:             aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
	genai.FinishReasonLanguage:               aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
	genai.FinishReasonOther:                  aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
	genai.FinishReasonBlocklist:              aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
	genai.FinishReasonProhibitedContent:      aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
	genai.FinishReasonSPII:                   aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
	genai.FinishReasonMalformedFunctionCall:  aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
	genai.FinishReasonImageSafety:            aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
	genai.FinishReasonUnexpectedToolCall:     aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
	genai.FinishReasonImageProhibitedContent: aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
	genai.FinishReasonNoImage:                aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
	genai.FinishReasonImageRecitation:        aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_REFUSAL,
	genai.FinishReasonImageOther:             aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_END_TURN,
}

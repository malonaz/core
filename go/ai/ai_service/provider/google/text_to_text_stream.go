package google

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"google.golang.org/genai"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	blockTypeText     = "text"
	blockTypeThought  = "thought"
	blockTypeImage    = "image"
	blockTypeToolCall = "tool_call"
)

// TextToTextStream handles a streaming text-to-text generation request using the Google GenAI API.
// It converts proto messages to genai contents, streams the response, and sends blocks back to the caller.
func (c *Client) TextToTextStream(
	request *aiservicepb.TextToTextStreamRequest,
	srv aiservicepb.AiService_TextToTextStreamServer,
) error {
	ctx := srv.Context()

	model, err := c.modelService.GetModel(ctx, &aiservicepb.GetModelRequest{Name: request.Model})
	if err != nil {
		return err
	}

	contents, systemInstruction, err := c.buildContents(ctx, request.Messages)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "building contents: %v", err).Err()
	}

	config := &genai.GenerateContentConfig{}

	if systemInstruction != "" {
		config.SystemInstruction = &genai.Content{
			Parts: []*genai.Part{{Text: systemInstruction}},
		}
	}

	// Enable image output modality if image config is provided.
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

	// Configure extended thinking/reasoning if the model supports it.
	if model.GetTtt().GetReasoning() {
		thinkingConfig, err := buildThinkingConfig(model, request.GetConfiguration().GetReasoningEffort())
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "building thinking config: %v", err).Err()
		}
		config.ThinkingConfig = thinkingConfig
	}

	if len(request.Tools) > 0 {
		tools, err := buildTools(request.Tools)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "building tools: %v", err).Err()
		}
		config.Tools = tools

		toolConfig, err := buildToolConfig(request.GetConfiguration())
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "building tool config: %v", err).Err()
		}
		config.ToolConfig = toolConfig
	}

	startTime := time.Now()
	generateContentStream := c.client.Models.GenerateContentStream(ctx, model.ProviderModelId, contents, config)

	cs := provider.NewAsyncTextToTextContentSender(srv, 100)
	defer cs.Close()

	tca := provider.NewToolCallAccumulator()

	var sentTtfb bool
	var stopReason aiservicepb.TextToTextStopReason

	// Track block indexing to coalesce consecutive blocks of the same type.
	var currentBlockIndex int64 = -1
	var currentBlockType string

	for resp, err := range generateContentStream {
		if err != nil {
			// Map Google API errors to gRPC status codes for consistent error handling.
			if apiError, ok := errors.AsType[genai.APIError](err); ok {
				return status.Errorf(grpcCodeFromHTTPStatus(apiError.Code), "%s", apiError.Message).Err()
			}
			return status.FromError(err, "reading stream").Err()
		}

		if cs.Err() != nil {
			break
		}

		// Send time-to-first-byte metric on the first chunk.
		if !sentTtfb {
			cs.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{
				Ttfb: durationpb.New(time.Since(startTime)),
			})
			sentTtfb = true
		}

		for _, candidate := range resp.Candidates {
			if candidate.FinishReason != genai.FinishReasonUnspecified {
				var ok bool
				stopReason, ok = finishReasonToPb[candidate.FinishReason]
				if !ok {
					return status.Errorf(codes.Internal, "unknown finish reason: %v", candidate.FinishReason).Err()
				}
			}
			if candidate.Content == nil {
				continue
			}

			for _, part := range candidate.Content.Parts {
				var signature string
				if len(part.ThoughtSignature) > 0 {
					signature = base64.StdEncoding.EncodeToString(part.ThoughtSignature)
				}

				// Handle text and thought blocks, coalescing consecutive blocks of the same type
				// into the same block index.
				if part.Text != "" {
					if part.Thought {
						if currentBlockType != blockTypeThought {
							currentBlockIndex++
							currentBlockType = blockTypeThought
						}
						cs.SendBlocks(ctx, &aipb.Block{
							Index:     currentBlockIndex,
							Content:   &aipb.Block_Thought{Thought: part.Text},
							Signature: signature,
						})
					} else {
						if currentBlockType != blockTypeText {
							currentBlockIndex++
							currentBlockType = blockTypeText
						}
						cs.SendBlocks(ctx, &aipb.Block{
							Index:     currentBlockIndex,
							Content:   &aipb.Block_Text{Text: part.Text},
							Signature: signature,
						})
					}
				}

				// Images always get their own block index since they can't be coalesced.
				if part.InlineData != nil {
					currentBlockIndex++
					currentBlockType = blockTypeImage
					cs.SendBlocks(ctx, &aipb.Block{
						Index: currentBlockIndex,
						Content: &aipb.Block_Image{Image: &aipb.Image{
							Source:    &aipb.Image_Data{Data: part.InlineData.Data},
							MediaType: part.InlineData.MIMEType,
						}},
						Signature: signature,
					})
				}

				// Handle function calls, which may arrive as partial args across multiple chunks
				// or as complete calls in a single chunk.
				if part.FunctionCall != nil {
					fc := part.FunctionCall

					var signature string
					if len(part.ThoughtSignature) > 0 {
						signature = base64.StdEncoding.EncodeToString(part.ThoughtSignature)
					}

					if len(fc.PartialArgs) > 0 {
						// Streaming partial args mode: accumulate individual arg paths.
						if !tca.Has(currentBlockIndex) || currentBlockType != blockTypeToolCall {
							currentBlockIndex++
							currentBlockType = blockTypeToolCall
							tca.Start(currentBlockIndex, fc.ID, fc.Name)
						}

						for _, partialArg := range fc.PartialArgs {
							value := resolvePartialArgValue(partialArg)
							tca.AppendArg(currentBlockIndex, partialArg.JsonPath, value)
						}

						if fc.WillContinue != nil && !*fc.WillContinue {
							block, err := tca.Build(currentBlockIndex)
							if err != nil {
								return err
							}
							block.Signature = signature
							cs.SendBlocks(ctx, block)
						} else {
							block, err := tca.BuildPartial(currentBlockIndex)
							if err != nil {
								return err
							}
							block.Signature = signature
							cs.SendBlocks(ctx, block)
						}
					} else if fc.WillContinue != nil && *fc.WillContinue {
						// Streaming JSON args mode: the call will continue in future chunks.
						if !tca.Has(currentBlockIndex) || currentBlockType != blockTypeToolCall {
							currentBlockIndex++
							currentBlockType = blockTypeToolCall
							tca.Start(currentBlockIndex, fc.ID, fc.Name)
						} else {
							tca.StartOrUpdate(currentBlockIndex, fc.ID, fc.Name)
						}
						if fc.Args != nil {
							argsJSON, err := json.Marshal(fc.Args)
							if err != nil {
								return status.Errorf(codes.Internal, "marshaling function call args: %v", err).Err()
							}
							tca.AppendArgs(currentBlockIndex, string(argsJSON))
						}
						block, err := tca.BuildPartial(currentBlockIndex)
						if err != nil {
							return err
						}
						block.Signature = signature
						cs.SendBlocks(ctx, block)
					} else if tca.Has(currentBlockIndex) && currentBlockType == blockTypeToolCall {
						// Final chunk of a streaming tool call: finalize the accumulated call.
						tca.StartOrUpdate(currentBlockIndex, fc.ID, fc.Name)
						if fc.Args != nil {
							argsJSON, err := json.Marshal(fc.Args)
							if err != nil {
								return status.Errorf(codes.Internal, "marshaling function call args: %v", err).Err()
							}
							tca.AppendArgs(currentBlockIndex, string(argsJSON))
						}
						block, err := tca.Build(currentBlockIndex)
						if err != nil {
							return err
						}
						block.Signature = signature
						cs.SendBlocks(ctx, block)
					} else {
						// Non-streaming tool call: complete call in a single chunk.
						if currentBlockType != blockTypeToolCall {
							currentBlockIndex++
							currentBlockType = blockTypeToolCall
						}

						argsJSON := []byte("{}")
						if fc.Args != nil {
							var marshalErr error
							argsJSON, marshalErr = json.Marshal(fc.Args)
							if marshalErr != nil {
								return status.Errorf(codes.Internal, "marshaling function call args: %v", marshalErr).Err()
							}
						}

						tca.Start(currentBlockIndex, fc.ID, fc.Name)
						tca.AppendArgs(currentBlockIndex, string(argsJSON))

						block, err := tca.Build(currentBlockIndex)
						if err != nil {
							return err
						}
						block.Signature = signature
						cs.SendBlocks(ctx, block)
					}
				}
			}
		}

		if resp.UsageMetadata != nil {
			modelUsage, err := buildModelUsage(request.Model, resp.UsageMetadata)
			if err != nil {
				return status.Errorf(codes.Internal, "building model usage: %v", err).Err()
			}
			cs.SendModelUsage(ctx, modelUsage)
		}
	}

	// Emit any tool calls that were still accumulating when the stream ended.
	toolCalls, err := tca.BuildRemaining()
	if err != nil {
		return err
	}
	cs.SendBlocks(ctx, toolCalls...)

	if stopReason != aiservicepb.TextToTextStopReason_TEXT_TO_TEXT_STOP_REASON_UNSPECIFIED {
		cs.SendStopReason(ctx, stopReason)
	}

	// Send time-to-last-byte metric.
	cs.SendGenerationMetrics(ctx, &aipb.GenerationMetrics{
		Ttlb: durationpb.New(time.Since(startTime)),
	})

	cs.Close()
	if err := cs.Wait(ctx); err != nil {
		return status.FromError(err, "waiting on content sender").Err()
	}
	return nil
}

// buildContents converts proto messages to genai contents, extracting system instructions separately
// since the GenAI API treats them as a top-level config field rather than a message in the conversation.
func (c *Client) buildContents(ctx context.Context, messages []*aipb.Message) ([]*genai.Content, string, error) {
	var contents []*genai.Content
	var systemInstruction string

	for i, message := range messages {
		switch message.Role {
		case aipb.Role_ROLE_SYSTEM:
			for j, block := range message.Blocks {
				switch content := block.Content.(type) {
				case *aipb.Block_Text:
					if systemInstruction != "" {
						systemInstruction += "\n\n"
					}
					systemInstruction += content.Text
				default:
					return nil, "", fmt.Errorf("message [%d] block [%d]: unexpected block type %T for SYSTEM role", i, j, content)
				}
			}

		case aipb.Role_ROLE_USER:
			parts, err := c.buildUserParts(ctx, message.Blocks)
			if err != nil {
				return nil, "", fmt.Errorf("message [%d]: %w", i, err)
			}
			contents = append(contents, &genai.Content{
				Role:  genai.RoleUser,
				Parts: parts,
			})

		case aipb.Role_ROLE_ASSISTANT:
			parts, err := c.buildAssistantParts(ctx, message.Blocks)
			if err != nil {
				return nil, "", fmt.Errorf("message [%d]: %w", i, err)
			}
			contents = append(contents, &genai.Content{
				Role:  genai.RoleModel,
				Parts: parts,
			})

		case aipb.Role_ROLE_TOOL:
			for j, block := range message.Blocks {
				switch content := block.Content.(type) {
				case *aipb.Block_ToolResult:
					toolContent, err := buildToolResultContent(content.ToolResult)
					if err != nil {
						return nil, "", fmt.Errorf("message [%d] block [%d]: %w", i, j, err)
					}
					contents = append(contents, toolContent)
				default:
					return nil, "", fmt.Errorf("message [%d] block [%d]: unexpected block type %T for TOOL role", i, j, content)
				}
			}

		default:
			return nil, "", fmt.Errorf("message [%d]: unexpected role %v", i, message.Role)
		}
	}

	return contents, systemInstruction, nil
}

// buildUserParts converts user message blocks into genai parts.
// Images are uploaded via the Files API using content-addressable naming.
func (c *Client) buildUserParts(ctx context.Context, blocks []*aipb.Block) ([]*genai.Part, error) {
	parts := make([]*genai.Part, 0, len(blocks))

	for j, block := range blocks {
		switch content := block.Content.(type) {
		case *aipb.Block_Text:
			parts = append(parts, &genai.Part{Text: content.Text})

		case *aipb.Block_Image:
			part, err := c.buildImagePart(ctx, content.Image)
			if err != nil {
				return nil, fmt.Errorf("block [%d]: %w", j, err)
			}
			parts = append(parts, part)

		default:
			return nil, fmt.Errorf("block [%d]: unexpected block type %T for USER role", j, content)
		}
	}

	return parts, nil
}

// buildImagePart converts an image proto to a genai Part with inline data.
// URL-sourced images are downloaded and submitted as inline data.
func (c *Client) buildImagePart(ctx context.Context, img *aipb.Image) (*genai.Part, error) {
	if img.MediaType == "" {
		return nil, fmt.Errorf("media_type required for image data")
	}

	switch source := img.Source.(type) {
	case *aipb.Image_Data:
		return &genai.Part{
			InlineData: &genai.Blob{
				Data:     source.Data,
				MIMEType: img.MediaType,
			},
		}, nil

	case *aipb.Image_Url:
		httpResponse, err := http.Get(source.Url)
		if err != nil {
			return nil, fmt.Errorf("downloading image from URL: %w", err)
		}
		defer httpResponse.Body.Close()
		data, err := io.ReadAll(httpResponse.Body)
		if err != nil {
			return nil, fmt.Errorf("reading image response body: %w", err)
		}
		return &genai.Part{
			InlineData: &genai.Blob{
				Data:     data,
				MIMEType: img.MediaType,
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown image source type: %T", source)
	}
}

// buildAssistantParts converts assistant message blocks into genai parts, preserving
// thought signatures for multi-turn reasoning continuity.
func (c *Client) buildAssistantParts(ctx context.Context, blocks []*aipb.Block) ([]*genai.Part, error) {
	var parts []*genai.Part

	for j, block := range blocks {
		var thoughtSignature []byte
		if block.Signature != "" {
			decoded, err := base64.StdEncoding.DecodeString(block.Signature)
			if err != nil {
				return nil, fmt.Errorf("block [%d]: decoding thought signature: %w", j, err)
			}
			thoughtSignature = decoded
		}

		switch content := block.Content.(type) {
		case *aipb.Block_Thought:
			parts = append(parts, &genai.Part{
				Text:             content.Thought,
				Thought:          true,
				ThoughtSignature: thoughtSignature,
			})

		case *aipb.Block_Text:
			parts = append(parts, &genai.Part{
				Text:             content.Text,
				ThoughtSignature: thoughtSignature,
			})

		case *aipb.Block_ToolCall:
			tc := content.ToolCall
			if tc.Name == "" {
				return nil, fmt.Errorf("block [%d]: tool call missing name", j)
			}
			parts = append(parts, &genai.Part{
				FunctionCall: &genai.FunctionCall{
					Name: tc.Name,
					Args: tc.Arguments.AsMap(),
				},
				ThoughtSignature: thoughtSignature,
			})

		case *aipb.Block_Image:
			imagePart, err := c.buildImagePart(ctx, content.Image)
			if err != nil {
				return nil, fmt.Errorf("block [%d]: building image part: %w", j, err)
			}
			imagePart.ThoughtSignature = thoughtSignature
			parts = append(parts, imagePart)

		default:
			return nil, fmt.Errorf("block [%d]: unexpected block type %T for ASSISTANT role", j, content)
		}
	}

	return parts, nil
}

// buildToolResultContent wraps a tool result into a genai Content with a FunctionResponse.
// Uses "output" as the key for successful results and "error" for failures.
func buildToolResultContent(tr *aipb.ToolResult) (*genai.Content, error) {
	content, err := ai.ParseToolResult(tr)
	if err != nil {
		return nil, fmt.Errorf("parsing tool result: %w", err)
	}
	key := "output"
	if tr.GetError() != nil {
		key = "error"
	}
	functionResponse := &genai.FunctionResponse{
		ID:       tr.ToolCallId,
		Name:     tr.ToolName,
		Response: map[string]any{key: content},
	}

	return &genai.Content{
		Role: genai.RoleUser,
		Parts: []*genai.Part{
			{FunctionResponse: functionResponse},
		},
	}, nil
}

// buildTools converts proto tool definitions to genai function declarations.
// All declarations are grouped into a single genai.Tool per the API convention.
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

// buildToolConfig maps our tool choice configuration to the GenAI FunctionCallingConfig,
// supporting none/auto/required modes and specific tool name targeting.
func buildToolConfig(configuration *aiservicepb.TextToTextConfiguration) (*genai.ToolConfig, error) {
	functionCallingConfig := &genai.FunctionCallingConfig{}
	streamPartialToolCalls := configuration.GetStreamPartialToolCalls()
	if streamPartialToolCalls {
		functionCallingConfig.StreamFunctionCallArguments = &streamPartialToolCalls
	}
	if configuration.GetToolChoice() != nil {
		switch choice := configuration.GetToolChoice().Choice.(type) {
		case *aipb.ToolChoice_Mode:
			switch choice.Mode {
			case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_NONE:
				functionCallingConfig.Mode = genai.FunctionCallingConfigModeNone

			case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_AUTO:
				functionCallingConfig.Mode = genai.FunctionCallingConfigModeAuto

			// GenAI uses "Any" to mean "must call at least one function".
			case aipb.ToolChoiceMode_TOOL_CHOICE_MODE_REQUIRED:
				functionCallingConfig.Mode = genai.FunctionCallingConfigModeAny

			default:
				return nil, fmt.Errorf("unknown tool choice mode: %v", choice.Mode)
			}

		case *aipb.ToolChoice_ToolName:
			functionCallingConfig.Mode = genai.FunctionCallingConfigModeAny
			functionCallingConfig.AllowedFunctionNames = []string{choice.ToolName}

		default:
			return nil, fmt.Errorf("unknown tool choice type: %T", choice)
		}
	}

	return &genai.ToolConfig{
		FunctionCallingConfig: functionCallingConfig,
	}, nil
}

// buildThinkingConfig creates a thinking config from model provider settings.
// Models can use either a token budget or a named thinking level, configured via provider_settings.
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

// buildModelUsage converts GenAI usage metadata into our ModelUsage proto, splitting tokens
// by modality (text vs image) and accounting for cached tokens to avoid double-counting.
func buildModelUsage(modelName string, usage *genai.GenerateContentResponseUsageMetadata) (*aipb.ModelUsage, error) {
	modelUsage := &aipb.ModelUsage{
		Model: modelName,
	}

	var inputImageTokens, outputImageTokens, cacheReadImageTokens int32
	var inputTextTokens, outputTextTokens, cacheReadTextTokens int32

	// Fall back to aggregate counts when per-modality details aren't available.
	if len(usage.PromptTokensDetails) > 0 {
		for _, detail := range usage.PromptTokensDetails {
			switch detail.Modality {
			case genai.MediaModalityImage:
				inputImageTokens += detail.TokenCount
			case genai.MediaModalityText:
				inputTextTokens += detail.TokenCount
			}
		}
	} else {
		inputTextTokens = usage.PromptTokenCount
	}

	if len(usage.CandidatesTokensDetails) > 0 {
		for _, detail := range usage.CandidatesTokensDetails {
			switch detail.Modality {
			case genai.MediaModalityImage:
				outputImageTokens += detail.TokenCount
			case genai.MediaModalityText:
				outputTextTokens += detail.TokenCount
			}
		}
	} else {
		outputTextTokens = usage.CandidatesTokenCount
	}

	if len(usage.CacheTokensDetails) > 0 {
		for _, detail := range usage.CacheTokensDetails {
			switch detail.Modality {
			case genai.MediaModalityImage:
				cacheReadImageTokens += detail.TokenCount
			case genai.MediaModalityText:
				cacheReadTextTokens += detail.TokenCount
			}
		}
	} else {
		cacheReadTextTokens = usage.CachedContentTokenCount
	}

	// Report uncached input tokens only (cached tokens are reported separately for pricing).
	if inputTextTokens > 0 {
		uncachedTextTokens := inputTextTokens - cacheReadTextTokens
		if uncachedTextTokens < 0 {
			return nil, fmt.Errorf("negative uncached text tokens: input=%d, cacheRead=%d", inputTextTokens, cacheReadTextTokens)
		}
		modelUsage.InputToken = ai.NewResourceConsumption(uncachedTextTokens)
	}
	if inputImageTokens > 0 {
		uncachedImageTokens := inputImageTokens - cacheReadImageTokens
		if uncachedImageTokens < 0 {
			return nil, fmt.Errorf("negative uncached image tokens: input=%d, cacheRead=%d", inputImageTokens, cacheReadImageTokens)
		}
		modelUsage.InputImageToken = ai.NewResourceConsumption(uncachedImageTokens)
	}

	modelUsage.InputTokenCacheRead = ai.NewResourceConsumption(cacheReadTextTokens)
	modelUsage.OutputToken = ai.NewResourceConsumption(outputTextTokens)
	modelUsage.OutputReasoningToken = ai.NewResourceConsumption(usage.ThoughtsTokenCount)
	modelUsage.InputImageTokenCacheRead = ai.NewResourceConsumption(cacheReadImageTokens)
	modelUsage.OutputImageToken = ai.NewResourceConsumption(outputImageTokens)

	return modelUsage, nil
}

// finishReasonToPb maps GenAI finish reasons to our proto stop reasons.
// Safety/content-policy reasons map to REFUSAL; everything else maps to END_TURN or MAX_TOKENS.
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

// resolvePartialArgValue extracts the typed value from a PartialArg, which uses a union-like
// encoding where only one of the value fields is set.
func resolvePartialArgValue(partialArg *genai.PartialArg) any {
	if partialArg.NULLValue != "" {
		return nil
	}
	if partialArg.NumberValue != nil {
		return *partialArg.NumberValue
	}
	if partialArg.BoolValue != nil {
		return *partialArg.BoolValue
	}
	return partialArg.StringValue
}

// grpcCodeFromHTTPStatus maps HTTP status codes to gRPC codes for Google API error translation.
func grpcCodeFromHTTPStatus(status int) codes.Code {
	switch status {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusConflict:
		return codes.AlreadyExists
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusServiceUnavailable:
		return codes.Unavailable
	case http.StatusGatewayTimeout:
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}

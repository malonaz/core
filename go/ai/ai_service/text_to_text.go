package ai_service

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/grpcinproc"
)

// TextToTextStream implements the gRPC streaming method - direct pass-through
func (s *Service) TextToTextStream(request *pb.TextToTextStreamRequest, srv pb.AiService_TextToTextStreamServer) error {
	ctx := srv.Context()
	provider, model, err := s.GetTextToTextProvider(ctx, request.Model)
	if err != nil {
		return err
	}
	if err := checkModelDeprecation(model); err != nil {
		return grpc.Errorf(codes.FailedPrecondition, err.Error()).Err()
	}
	if request.Configuration == nil {
		request.Configuration = &pb.TextToTextConfiguration{}
	}
	if request.Configuration.MaxTokens == 0 {
		request.Configuration.MaxTokens = model.Ttt.OutputTokenLimit
	}

	if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED && !model.GetTtt().GetReasoning() {
		return grpc.Errorf(codes.InvalidArgument, "%s does not support reasoning", request.Model).Err()
	}
	if len(request.Tools) > 0 && !model.GetTtt().GetToolCall() {
		return grpc.Errorf(codes.InvalidArgument, "%s does not support tool calling", request.Model).Err()
	}

	toolNameToTool := make(map[string]*aipb.Tool, len(request.Tools))
	for _, tool := range request.Tools {
		toolNameToTool[tool.Name] = tool
	}
	wrapper := &tttStreamWrapper{
		AiService_TextToTextStreamServer: srv,
		model:                            model,
		modelUsage:                       &aipb.ModelUsage{},
		toolNameToTool:                   toolNameToTool,
		toolCallIDToToolCall:             map[string]*aipb.ToolCall{},
	}
	return provider.TextToTextStream(request, wrapper)
}

type tttStreamWrapper struct {
	pb.AiService_TextToTextStreamServer
	model                *aipb.Model
	modelUsage           *aipb.ModelUsage
	toolNameToTool       map[string]*aipb.Tool
	toolCallIDToToolCall map[string]*aipb.ToolCall
}

func (w *tttStreamWrapper) copyToolAnnotations(toolCall *aipb.ToolCall) bool {
	// We always instantiate tool calls annotations.
	if toolCall.Annotations == nil {
		toolCall.Annotations = map[string]string{}
	}

	// Find the target tool.
	tool, ok := w.toolNameToTool[toolCall.Name]
	if !ok {
		return false
	}

	// Copy annotations.
	for k, v := range tool.GetAnnotations() {
		toolCall.Annotations[k] = v
	}
	return true
}

func (w *tttStreamWrapper) Send(resp *pb.TextToTextStreamResponse) error {
	switch c := resp.GetContent().(type) {
	case *pb.TextToTextStreamResponse_Block:
		var toolCall *aipb.ToolCall
		if c.Block.GetToolCall() != nil {
			toolCall = c.Block.GetToolCall()
		}
		if c.Block.GetPartialToolCall() != nil {
			toolCall = c.Block.GetPartialToolCall()
		}
		if toolCall != nil {
			// We always instantiate tool calls annotations.
			if toolCall.Annotations == nil {
				toolCall.Annotations = map[string]string{}
			}
			// Find the target tool.
			tool, ok := w.toolNameToTool[toolCall.Name]
			if !ok {
				return grpc.Errorf(codes.Internal, "tool call targets unknown tool %q", toolCall.Name).
					WithDetails(&aipb.ToolCallRecoverableError{
						ToolCallBlock:   c.Block,
						ToolResultBlock: ai.NewToolResultBlock(ai.NewErrorToolResult(toolCall.Name, toolCall.Id, fmt.Errorf("unknown tool"))),
					}).Err()
			}

			// Copy annotations.
			for k, v := range tool.GetAnnotations() {
				toolCall.Annotations[k] = v
			}
		}

		// Dedupe partial tool calls.
		if partialToolCall := c.Block.GetPartialToolCall(); partialToolCall != nil {
			if last, ok := w.toolCallIDToToolCall[partialToolCall.Id]; ok && proto.Equal(last, partialToolCall) {
				return nil
			}
			w.toolCallIDToToolCall[partialToolCall.Id] = partialToolCall
			toolCall = partialToolCall
		}

	case *pb.TextToTextStreamResponse_ModelUsage:
		modelUsage := c.ModelUsage

		// INPUT TOKENS.
		if inputToken := modelUsage.GetInputToken(); inputToken != nil {
			if existingInputToken := w.modelUsage.GetInputToken(); existingInputToken != nil {
				// Check if this is a cache breakdown: input + cache_read == previous_input
				if inputTokenCacheRead := modelUsage.GetInputTokenCacheRead(); inputTokenCacheRead != nil &&
					inputToken.Quantity+inputTokenCacheRead.Quantity == existingInputToken.Quantity {
					// Valid cache breakdown - allow the new input value to be sent
					w.modelUsage.InputToken = inputToken
				} else if inputToken.Quantity < existingInputToken.Quantity {
					w.modelUsage.InputToken = inputToken
					// Gemini is misbehaving -_-.
					//return grpc.Errorf(codes.Internal,
					//	"received input tokens with smaller quantity: previous %d, current %d",
					//	existingInputToken.Quantity, inputToken.Quantity,
					//).Err()
				} else if inputToken.Quantity == existingInputToken.Quantity {
					modelUsage.InputToken = nil
				} else {
					w.modelUsage.InputToken = inputToken
				}
			} else {
				w.modelUsage.InputToken = inputToken
			}
		}
		// INPUT CACHE READ TOKENS.
		if inputTokenCacheRead := modelUsage.GetInputTokenCacheRead(); inputTokenCacheRead != nil {
			if existingInputTokenCacheRead := w.modelUsage.GetInputTokenCacheRead(); existingInputTokenCacheRead != nil {
				if existingInputTokenCacheRead.Quantity != inputTokenCacheRead.Quantity {
					return grpc.Errorf(codes.Internal,
						"received input cache read tokens twice with different quantities: previous %d, current %d",
						existingInputTokenCacheRead.Quantity, inputTokenCacheRead.Quantity,
					).Err()
				}
				modelUsage.InputTokenCacheRead = nil
			} else {
				w.modelUsage.InputTokenCacheRead = inputTokenCacheRead
			}
		}

		// INPUT CACHE WRITE TOKENS.
		if inputTokenCacheWrite := modelUsage.GetInputTokenCacheWrite(); inputTokenCacheWrite != nil {
			if existingInputTokenCacheWrite := w.modelUsage.GetInputTokenCacheWrite(); existingInputTokenCacheWrite != nil {
				if existingInputTokenCacheWrite.Quantity != inputTokenCacheWrite.Quantity {
					return grpc.Errorf(codes.Internal,
						"received input cache write tokens twice with different quantities: previous %d, current %d",
						existingInputTokenCacheWrite.Quantity, inputTokenCacheWrite.Quantity,
					).Err()
				}
				modelUsage.InputTokenCacheWrite = nil
			} else {
				w.modelUsage.InputTokenCacheWrite = inputTokenCacheWrite
			}
		}

		// OUTPUT TOKENS.
		if outputToken := modelUsage.GetOutputToken(); outputToken != nil {
			if existingOutputToken := w.modelUsage.GetOutputToken(); existingOutputToken != nil {
				if outputToken.Quantity < existingOutputToken.Quantity {
					return grpc.Errorf(codes.Internal,
						"received output tokens with smaller quantity: previous %d, current %d",
						existingOutputToken.Quantity, outputToken.Quantity,
					).Err()
				}
				if outputToken.Quantity == existingOutputToken.Quantity {
					modelUsage.OutputToken = nil
				} else {
					w.modelUsage.OutputToken = outputToken
				}
			} else {
				w.modelUsage.OutputToken = outputToken
			}
		}

		// INPUT IMAGE TOKENS.
		if inputImageToken := modelUsage.GetInputImageToken(); inputImageToken != nil {
			if existingInputImageToken := w.modelUsage.GetInputImageToken(); existingInputImageToken != nil {
				if inputImageToken.Quantity < existingInputImageToken.Quantity {
					w.modelUsage.InputImageToken = inputImageToken
				} else if inputImageToken.Quantity == existingInputImageToken.Quantity {
					modelUsage.InputImageToken = nil
				} else {
					w.modelUsage.InputImageToken = inputImageToken
				}
			} else {
				w.modelUsage.InputImageToken = inputImageToken
			}
		}

		// INPUT IMAGE CACHE READ TOKENS.
		if inputImageTokenCacheRead := modelUsage.GetInputImageTokenCacheRead(); inputImageTokenCacheRead != nil {
			if existingInputImageTokenCacheRead := w.modelUsage.GetInputImageTokenCacheRead(); existingInputImageTokenCacheRead != nil {
				if existingInputImageTokenCacheRead.Quantity != inputImageTokenCacheRead.Quantity {
					return grpc.Errorf(codes.Internal,
						"received input image cache read tokens twice with different quantities: previous %d, current %d",
						existingInputImageTokenCacheRead.Quantity, inputImageTokenCacheRead.Quantity,
					).Err()
				}
				modelUsage.InputImageTokenCacheRead = nil
			} else {
				w.modelUsage.InputImageTokenCacheRead = inputImageTokenCacheRead
			}
		}

		// INPUT IMAGE CACHE WRITE TOKENS.
		if inputImageTokenCacheWrite := modelUsage.GetInputImageTokenCacheWrite(); inputImageTokenCacheWrite != nil {
			if existingInputImageTokenCacheWrite := w.modelUsage.GetInputImageTokenCacheWrite(); existingInputImageTokenCacheWrite != nil {
				if existingInputImageTokenCacheWrite.Quantity != inputImageTokenCacheWrite.Quantity {
					return grpc.Errorf(codes.Internal,
						"received input image cache write tokens twice with different quantities: previous %d, current %d",
						existingInputImageTokenCacheWrite.Quantity, inputImageTokenCacheWrite.Quantity,
					).Err()
				}
				modelUsage.InputImageTokenCacheWrite = nil
			} else {
				w.modelUsage.InputImageTokenCacheWrite = inputImageTokenCacheWrite
			}
		}

		// OUTPUT IMAGE TOKENS.
		if outputImageToken := modelUsage.GetOutputImageToken(); outputImageToken != nil {
			if existingOutputImageToken := w.modelUsage.GetOutputImageToken(); existingOutputImageToken != nil {
				if outputImageToken.Quantity < existingOutputImageToken.Quantity {
					return grpc.Errorf(codes.Internal,
						"received output image tokens with smaller quantity: previous %d, current %d",
						existingOutputImageToken.Quantity, outputImageToken.Quantity,
					).Err()
				}
				if outputImageToken.Quantity == existingOutputImageToken.Quantity {
					modelUsage.OutputImageToken = nil
				} else {
					w.modelUsage.OutputImageToken = outputImageToken
				}
			} else {
				w.modelUsage.OutputImageToken = outputImageToken
			}
		}

		// OUTPUT REASONING TOKENS.
		if outputReasoningToken := modelUsage.GetOutputReasoningToken(); outputReasoningToken != nil {
			if existingOutputReasoningToken := w.modelUsage.GetOutputReasoningToken(); existingOutputReasoningToken != nil {
				if outputReasoningToken.Quantity == existingOutputReasoningToken.Quantity {
					modelUsage.OutputReasoningToken = nil
				} else {
					w.modelUsage.OutputReasoningToken = outputReasoningToken
				}
			} else {
				w.modelUsage.OutputReasoningToken = outputReasoningToken
			}
		}

		// Skip sending empty model usage responses
		if modelUsage.InputToken == nil && modelUsage.InputTokenCacheRead == nil &&
			modelUsage.InputTokenCacheWrite == nil && modelUsage.OutputToken == nil &&
			modelUsage.OutputReasoningToken == nil && modelUsage.InputImageToken == nil &&
			modelUsage.InputImageTokenCacheRead == nil && modelUsage.InputImageTokenCacheWrite == nil &&
			modelUsage.OutputImageToken == nil {
			return nil
		}
		computeModelUsagePrices(modelUsage, w.model.GetTtt().GetPricing())
	}

	return w.AiService_TextToTextStreamServer.Send(resp)
}

func computeModelUsagePrices(usage *aipb.ModelUsage, pricing *aipb.TttModelPricing) {
	if pricing == nil {
		return
	}
	if usage.InputToken != nil {
		usage.InputToken.Price = float64(usage.InputToken.Quantity) * pricing.InputTokenPricePerMillion / 1_000_000
	}
	if usage.OutputToken != nil {
		usage.OutputToken.Price = float64(usage.OutputToken.Quantity) * pricing.OutputTokenPricePerMillion / 1_000_000
	}
	if usage.OutputReasoningToken != nil {
		pricePerMillion := pricing.OutputReasoningTokenPricePerMillion
		if pricePerMillion == 0 {
			pricePerMillion = pricing.OutputTokenPricePerMillion
		}
		usage.OutputReasoningToken.Price = float64(usage.OutputReasoningToken.Quantity) * pricePerMillion / 1_000_000
	}
	if usage.InputTokenCacheRead != nil {
		usage.InputTokenCacheRead.Price = float64(usage.InputTokenCacheRead.Quantity) * pricing.InputTokenCacheReadPricePerMillion / 1_000_000
	}
	if usage.InputTokenCacheWrite != nil {
		usage.InputTokenCacheWrite.Price = float64(usage.InputTokenCacheWrite.Quantity) * pricing.InputTokenCacheWritePricePerMillion / 1_000_000
	}

	// IMAGE TOKENS.
	if usage.InputImageToken != nil {
		usage.InputImageToken.Price = float64(usage.InputImageToken.Quantity) * pricing.InputImageTokenPricePerMillion / 1_000_000
	}
	if usage.OutputImageToken != nil {
		usage.OutputImageToken.Price = float64(usage.OutputImageToken.Quantity) * pricing.OutputImageTokenPricePerMillion / 1_000_000
	}
	if usage.InputImageTokenCacheRead != nil {
		usage.InputImageTokenCacheRead.Price = float64(usage.InputImageTokenCacheRead.Quantity) * pricing.InputImageTokenCacheReadPricePerMillion / 1_000_000
	}
	if usage.InputImageTokenCacheWrite != nil {
		usage.InputImageTokenCacheWrite.Price = float64(usage.InputImageTokenCacheWrite.Quantity) * pricing.InputImageTokenCacheWritePricePerMillion / 1_000_000
	}
}

// TextToText collects all streamed text chunks into a single response
func (s *Service) TextToText(ctx context.Context, request *pb.TextToTextRequest) (*pb.TextToTextResponse, error) {
	// Convert to streaming request
	streamRequest := &pb.TextToTextStreamRequest{
		Model:         request.Model,
		Messages:      request.Messages,
		Tools:         request.Tools,
		Configuration: request.Configuration,
	}

	// Create a local streaming client using grpcinproc
	serverStreamClient := grpcinproc.NewServerStreamAsClient[
		pb.TextToTextStreamRequest,
		pb.TextToTextStreamResponse,
		pb.AiService_TextToTextStreamServer,
	](s.TextToTextStream)

	stream, err := serverStreamClient(ctx, streamRequest)
	if err != nil {
		return nil, err
	}

	accumulator := ai.NewTextToTextAccumulator()
	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if err := accumulator.Add(event); err != nil {
			return nil, grpc.Errorf(codes.Internal, "accumulating stream events: %v", err).Err()
		}
	}

	return accumulator.Response(), nil
}

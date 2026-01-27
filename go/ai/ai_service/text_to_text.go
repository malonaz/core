package ai_service

import (
	"context"
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
	}
	return provider.TextToTextStream(request, wrapper)
}

type tttStreamWrapper struct {
	pb.AiService_TextToTextStreamServer
	model          *aipb.Model
	modelUsage     *aipb.ModelUsage
	toolNameToTool map[string]*aipb.Tool
}

func (w *tttStreamWrapper) copyToolAnnotations(toolCall *aipb.ToolCall) {
	tool, ok := w.toolNameToTool[toolCall.Name]
	if !ok {
		return
	}
	if len(tool.GetAnnotations()) == 0 {
		return
	}
	if toolCall.Annotations == nil {
		toolCall.Annotations = map[string]string{}
	}
	for k, v := range tool.GetAnnotations() {
		toolCall.Annotations[k] = v
	}
}

func (w *tttStreamWrapper) Send(resp *pb.TextToTextStreamResponse) error {
	switch c := resp.GetContent().(type) {
	case *pb.TextToTextStreamResponse_Block:
		if c.Block.GetToolCall() != nil {
			w.copyToolAnnotations(c.Block.GetToolCall())
		}
		if c.Block.GetPartialToolCall() != nil {
			w.copyToolAnnotations(c.Block.GetPartialToolCall())
		}

	case *pb.TextToTextStreamResponse_ModelUsage:
		modelUsage := c.ModelUsage

		// INPUT TOKENS.
		if inputToken := modelUsage.GetInputToken(); inputToken != nil {
			if existingInputToken := w.modelUsage.GetInputToken(); existingInputToken != nil {
				// Check if this is a cache breakdown: input + cache_read == previous_input
				if inputCacheReadToken := modelUsage.GetInputCacheReadToken(); inputCacheReadToken != nil &&
					inputToken.Quantity+inputCacheReadToken.Quantity == existingInputToken.Quantity {
					// Valid cache breakdown - allow the new input value to be sent
					w.modelUsage.InputToken = inputToken
				} else if inputToken.Quantity < existingInputToken.Quantity {
					return grpc.Errorf(codes.Internal,
						"received input tokens with smaller quantity: previous %d, current %d",
						existingInputToken.Quantity, inputToken.Quantity,
					).Err()
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
		if inputCacheReadToken := modelUsage.GetInputCacheReadToken(); inputCacheReadToken != nil {
			if existingInputCacheReadToken := w.modelUsage.GetInputCacheReadToken(); existingInputCacheReadToken != nil {
				if existingInputCacheReadToken.Quantity != inputCacheReadToken.Quantity {
					return grpc.Errorf(codes.Internal,
						"received input cache read tokens twice with different quantities: previous %d, current %d",
						existingInputCacheReadToken.Quantity, inputCacheReadToken.Quantity,
					).Err()
				}
				modelUsage.InputCacheReadToken = nil
			} else {
				w.modelUsage.InputCacheReadToken = inputCacheReadToken
			}
		}

		// INPUT CACHE WRITE TOKENS.
		if inputCacheWriteToken := modelUsage.GetInputCacheWriteToken(); inputCacheWriteToken != nil {
			if existingInputCacheWriteToken := w.modelUsage.GetInputCacheWriteToken(); existingInputCacheWriteToken != nil {
				if existingInputCacheWriteToken.Quantity != inputCacheWriteToken.Quantity {
					return grpc.Errorf(codes.Internal,
						"received input cache write tokens twice with different quantities: previous %d, current %d",
						existingInputCacheWriteToken.Quantity, inputCacheWriteToken.Quantity,
					).Err()
				}
				modelUsage.InputCacheWriteToken = nil
			} else {
				w.modelUsage.InputCacheWriteToken = inputCacheWriteToken
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
		if modelUsage.InputToken == nil && modelUsage.InputCacheReadToken == nil &&
			modelUsage.InputCacheWriteToken == nil && modelUsage.OutputToken == nil &&
			modelUsage.OutputReasoningToken == nil {
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
	if usage.InputCacheReadToken != nil {
		usage.InputCacheReadToken.Price = float64(usage.InputCacheReadToken.Quantity) * pricing.InputCacheReadTokenPricePerMillion / 1_000_000
	}
	if usage.InputCacheWriteToken != nil {
		usage.InputCacheWriteToken.Price = float64(usage.InputCacheWriteToken.Quantity) * pricing.InputCacheWriteTokenPricePerMillion / 1_000_000
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

	// Aggregate chunks to form a single response.
	blockIndexToBlock := map[int64]*aipb.Block{}
	message := ai.NewAssistantMessage()
	response := &pb.TextToTextResponse{Message: message}

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		switch c := event.GetContent().(type) {
		case *pb.TextToTextStreamResponse_Block:
			block, ok := blockIndexToBlock[c.Block.Index]
			if !ok {
				block = &aipb.Block{Index: c.Block.Index}
				blockIndexToBlock[c.Block.Index] = block
				message.Blocks = append(message.Blocks, block)
			}
			if c.Block.Signature != "" {
				block.Signature = c.Block.Signature
			}
			switch content := c.Block.Content.(type) {
			case *aipb.Block_Text:
				if block.Content == nil {
					block.Content = &aipb.Block_Text{}
				}
				existing, ok := block.Content.(*aipb.Block_Text)
				if !ok {
					return nil, grpc.Errorf(codes.Internal, "block %d: received text content but block has type %T", c.Block.Index, block.Content).Err()
				}
				existing.Text += content.Text
			case *aipb.Block_Thought:
				if block.Content == nil {
					block.Content = &aipb.Block_Thought{}
				}
				existing, ok := block.Content.(*aipb.Block_Thought)
				if !ok {
					return nil, grpc.Errorf(codes.Internal, "block %d: received thought content but block has type %T", c.Block.Index, block.Content).Err()
				}
				existing.Thought += content.Thought
			case *aipb.Block_ToolCall:
				if block.Content != nil {
					if _, ok := block.Content.(*aipb.Block_ToolCall); !ok {
						return nil, grpc.Errorf(codes.Internal, "block %d: received tool_call content but block has type %T", c.Block.Index, block.Content).Err()
					}
				}
				block.Content = content
			case *aipb.Block_Image:
				if block.Content != nil {
					if _, ok := block.Content.(*aipb.Block_Image); !ok {
						return nil, grpc.Errorf(codes.Internal, "block %d: received image content but block has type %T", c.Block.Index, block.Content).Err()
					}
				}
				block.Content = content
			case *aipb.Block_PartialToolCall:
				// Skip partial tool calls in aggregation
			}

		case *pb.TextToTextStreamResponse_StopReason:
			response.StopReason = c.StopReason

		case *pb.TextToTextStreamResponse_ModelUsage:
			if response.ModelUsage == nil {
				response.ModelUsage = &aipb.ModelUsage{}
			}
			proto.Merge(response.ModelUsage, c.ModelUsage)

		case *pb.TextToTextStreamResponse_GenerationMetrics:
			if response.GenerationMetrics == nil {
				response.GenerationMetrics = &aipb.GenerationMetrics{}
			}
			proto.Merge(response.GenerationMetrics, c.GenerationMetrics)
			response.GenerationMetrics.Ttfb = nil
		}
	}

	return response, nil
}

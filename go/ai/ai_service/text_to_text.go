package ai_service

import (
	"context"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/grpcinproc"
	"github.com/malonaz/core/go/pbutil"
)

// TextToTextStream implements the gRPC streaming method - direct pass-through
func (s *Service) TextToTextStream(request *pb.TextToTextStreamRequest, srv pb.Ai_TextToTextStreamServer) error {
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

	wrapper := &tttStreamWrapper{
		Ai_TextToTextStreamServer: srv,
		model:                     model,
		modelUsage:                &aipb.ModelUsage{},
	}
	return provider.TextToTextStream(request, wrapper)
}

type tttStreamWrapper struct {
	pb.Ai_TextToTextStreamServer
	model      *aipb.Model
	modelUsage *aipb.ModelUsage
}

func (w *tttStreamWrapper) Send(resp *pb.TextToTextStreamResponse) error {
	if content, ok := resp.GetContent().(*pb.TextToTextStreamResponse_ModelUsage); ok {
		modelUsage := content.ModelUsage

		// INPUT TOKENS.
		if inputToken := modelUsage.GetInputToken(); inputToken != nil {
			if existingInputToken := w.modelUsage.GetInputToken(); existingInputToken != nil {
				// Check if this is a cache breakdown: input + cache_read == previous_input
				if inputCacheReadToken := modelUsage.GetInputCacheReadToken(); inputCacheReadToken != nil &&
					inputToken.Quantity+inputCacheReadToken.Quantity == existingInputToken.Quantity {
					// Valid cache breakdown - allow the new input value to be sent
					w.modelUsage.InputToken = inputToken
				} else if existingInputToken.Quantity != inputToken.Quantity {
					return grpc.Errorf(codes.Internal,
						"received input tokens twice with different quantities: previous %d, current %d",
						existingInputToken.Quantity, inputToken.Quantity,
					).Err()
				} else {
					modelUsage.InputToken = nil
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
				if outputReasoningToken.Quantity < existingOutputReasoningToken.Quantity {
					return grpc.Errorf(codes.Internal,
						"received output reasoning tokens with smaller quantity: previous %d, current %d",
						existingOutputReasoningToken.Quantity, outputReasoningToken.Quantity,
					).Err()
				}
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
	return w.Ai_TextToTextStreamServer.Send(resp)
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
		pb.Ai_TextToTextStreamServer,
	](s.TextToTextStream)

	stream, err := serverStreamClient(ctx, streamRequest)
	if err != nil {
		return nil, err
	}

	// Aggregate chunks to form a single response.
	assistantMessage := &aipb.AssistantMessage{}
	response := &pb.TextToTextResponse{Message: ai.NewAssistantMessage(assistantMessage)}

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		switch c := event.GetContent().(type) {
		case *pb.TextToTextStreamResponse_ContentChunk:
			assistantMessage.Content += c.ContentChunk

		case *pb.TextToTextStreamResponse_ReasoningChunk:
			assistantMessage.Reasoning += c.ReasoningChunk

		case *pb.TextToTextStreamResponse_StopReason:
			response.StopReason = c.StopReason

		case *pb.TextToTextStreamResponse_ToolCall:
			assistantMessage.ToolCalls = append(assistantMessage.ToolCalls, c.ToolCall)

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

	if request.GetConfiguration().GetExtractJsonObject() {
		structuredContent, err := extractJSONToStruct(assistantMessage.Content)
		if err != nil {
			return nil, grpc.Errorf(codes.Internal, "extracting json: %v", err).WithErrorInfo(
				"JSON_EXTRACTION_FAILED", "ai_service",
				map[string]string{"original_content": assistantMessage.Content},
			).Err()
		}
		assistantMessage.StructuredContent = structuredContent
		assistantMessage.Content = ""
	}

	return response, nil
}

func extractJSONToStruct(content string) (*structpb.Struct, error) {
	jsonString, err := ai.ExtractJSONString(content)
	if err != nil {
		return nil, err
	}
	return pbutil.NewStructFromJSON([]byte(jsonString))
}

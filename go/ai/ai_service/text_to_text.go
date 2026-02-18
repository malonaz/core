package ai_service

import (
	"context"
	"fmt"
	"io"
	"maps"

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
	maps.Copy(toolCall.Annotations, tool.GetAnnotations())
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
			maps.Copy(toolCall.Annotations, tool.GetAnnotations())
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
		// Check if the model usage is empty.
		if ai.IsModelUsageEmpty(c.ModelUsage) {
			return nil
		}
		// Merge the incoming model usage onto the base model usage.
		proto.Merge(w.modelUsage, c.ModelUsage)

		// Set the model usage pricing.
		ai.SetModelUsagePrices(w.modelUsage, w.model.GetTtt().GetPricing())
	}

	return w.AiService_TextToTextStreamServer.Send(resp)
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

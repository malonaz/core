package ai_service

import (
	"context"
	"fmt"
	"io"
	"maps"

	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	textToTextAccumulator := ai.NewTextToTextAccumulator()
	wrapper := &tttStreamWrapper{
		AiService_TextToTextStreamServer: srv,
		textToTextAccumulator:            textToTextAccumulator,
		model:                            model,
		modelUsage:                       &aipb.ModelUsage{},
		toolNameToTool:                   toolNameToTool,
		toolCallIDToToolCall:             map[string]*aipb.ToolCall{},
	}

	// Get or create chat chat.
	eg, ctxEg := errgroup.WithContext(ctx)
	var chat *aipb.Chat
	if request.GetParent() != "" {
		chatRn := &aipb.ChatResourceName{}
		if err := chatRn.UnmarshalString(request.GetParent()); err != nil {
			return grpc.Errorf(codes.InvalidArgument, "unmarshaling parent: %v", err).Err()
		}
		eg.Go(func() error {
			getChatRequest := &pb.GetChatRequest{Name: request.GetParent()}
			var err error
			chat, err = s.GetChat(ctxEg, getChatRequest)
			if err != nil {
				if status.Code(err) != codes.NotFound {
					return err
				}
				createChatRequest := &pb.CreateChatRequest{
					Parent: chatRn.UserResourceName().String(),
					ChatId: chatRn.Chat,
					Chat: &aipb.Chat{
						Metadata: &aipb.ChatMetadata{},
					},
				}
				chat, err = s.CreateChat(ctxEg, createChatRequest)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	// Process the request.
	if err := provider.TextToTextStream(request, wrapper); err != nil {
		return err
	}

	// Wait on the errgroup.
	if err := eg.Wait(); err != nil {
		return err
	}

	// Update the chat.
	if chat != nil {
		// Capture existing messages.
		createTimeToMessage := make(map[int64]*aipb.Message, len(chat.GetMetadata().GetMessages()))
		for _, message := range chat.GetMetadata().GetMessages() {
			createTimeToMessage[message.GetCreateTime().AsTime().UnixNano()] = message
		}

		// Iterate through the new messages and make updates.
		for _, message := range append(request.GetMessages(), textToTextAccumulator.Response().GetMessage()) {
			t := message.GetCreateTime().AsTime().UnixNano()
			if _, ok := createTimeToMessage[t]; ok {
				// The message exist.
				delete(createTimeToMessage, t)
				continue
			}
			// The message does not exist => add it.
			chat.Metadata.Messages = append(chat.Metadata.Messages, message)
		}

		// Iterate through the remaining entries in `createTimeMessage` and set their delete time if it's not already set.
		now := timestamppb.Now()
		for _, message := range createTimeToMessage {
			if message.DeleteTime == nil { // Do no re-delete a message.
				message.DeleteTime = now
			}
		}

		// Append the model usage.
		chat.Metadata.ModelUsages = append(chat.Metadata.ModelUsages, wrapper.modelUsage)

		// Update the chat.
		updateChatRequest := &pb.UpdateChatRequest{
			Chat:       chat,
			UpdateMask: pbfieldmask.FromPaths("metadata").Proto(),
		}
		if _, err := s.UpdateChat(ctx, updateChatRequest); err != nil {
			return err
		}
	}
	return nil
}

type tttStreamWrapper struct {
	pb.AiService_TextToTextStreamServer
	textToTextAccumulator *ai.TextToTextAccumulator
	model                 *aipb.Model
	modelUsage            *aipb.ModelUsage
	toolNameToTool        map[string]*aipb.Tool
	toolCallIDToToolCall  map[string]*aipb.ToolCall
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
	if err := w.textToTextAccumulator.Add(resp); err != nil {
		return grpc.Errorf(codes.Internal, "accumulating stream events: %v", err).Err()
	}

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
		Parent:        request.Parent,
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

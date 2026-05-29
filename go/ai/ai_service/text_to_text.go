package ai_service

import (
	"context"
	"fmt"
	"io"
	"maps"

	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/grpc/grpcinproc"
	"github.com/malonaz/core/go/grpc/status"
)

var (
	textToTextDefaultUserRn = &aipb.UserResourceName{
		Organization: "unknown",
		User:         "unknown",
	}
)

// context key
type tttAccumulatorKey struct{}

// TextToTextStream implements the gRPC streaming method - direct pass-through
func (s *Service) TextToTextStream(request *pb.TextToTextStreamRequest, srv pb.AiService_TextToTextStreamServer) error {
	// Parse or create chat resource name.
	var chatRn *aipb.ChatResourceName
	if request.GetParent() == "" {
		chatRn = new(textToTextDefaultUserRn.ChatResourceName(aip.NewSystemGeneratedBase32ResourceID()))
	} else {
		chatRn = &aipb.ChatResourceName{}
		if err := chatRn.UnmarshalString(request.GetParent()); err != nil {
			return status.Errorf(codes.InvalidArgument, "unmarshaling parent: %v", err).Err()
		}
	}

	ctx := srv.Context()

	provider, model, err := s.GetTextToTextProvider(ctx, request.Model)
	if err != nil {
		return err
	}
	if err := checkModelDeprecation(model); err != nil {
		return status.Errorf(codes.FailedPrecondition, err.Error()).Err()
	}
	if request.Configuration == nil {
		request.Configuration = &pb.TextToTextConfiguration{}
	}
	if request.Configuration.MaxTokens == 0 {
		request.Configuration.MaxTokens = model.Ttt.OutputTokenLimit
	}

	if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED && !model.GetTtt().GetReasoning() {
		return status.Errorf(codes.InvalidArgument, "%s does not support reasoning", request.Model).Err()
	}
	if len(request.Tools) > 0 && !model.GetTtt().GetToolCall() {
		return status.Errorf(codes.InvalidArgument, "%s does not support tool calling", request.Model).Err()
	}

	// Create a mapping from tool set name to tool name to tool.
	toolSetNameToToolNameToTool := make(map[string]map[string]*aipb.Tool, len(request.GetToolSets()))
	for _, toolSet := range request.GetToolSets() {
		toolNameToTool := make(map[string]*aipb.Tool, len(toolSet.GetTools()))
		for _, tool := range toolSet.GetTools() {
			toolNameToTool[tool.GetName()] = tool
			// Tool is prediscovered => add it to request tools.
			if val, _ := aip.GetAnnotation(tool, aitool.AnnotationKeyPreDiscoveredTool); val == aip.LabelValueTrue {
				request.Tools = append(request.Tools, tool)
			}
		}
		toolSetNameToToolNameToTool[toolSet.GetName()] = toolNameToTool
	}

	// Create a mapping of tool name to tool.
	toolNameToTool := make(map[string]*aipb.Tool, len(request.GetTools()))
	for _, tool := range request.GetTools() {
		toolNameToTool[tool.GetName()] = tool
	}

	// Process tool set discoveries.
	for i, message := range request.GetMessages() {
		for j, block := range ai.FilterBlocks(message.GetBlocks(), ai.BlockTypeToolCall) {
			toolCall := block.GetToolCall()
			toolType, ok := aip.GetAnnotation(toolCall, aitool.AnnotationKeyToolType)
			if !ok || toolType != aitool.AnnotationValueToolTypeDiscovery {
				continue
			}
			toolCallDiscovery, err := aitool.ParseDiscoveryToolCall(toolCall)
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "parsing message %d block %d tool call discovery", i, j).Err()
			}
			if _, ok := toolSetNameToToolNameToTool[toolCallDiscovery.GetToolSetName()]; !ok {
				return status.Errorf(codes.InvalidArgument, "message %d block %d has unknown tool set %q", i, j, toolCallDiscovery.GetToolSetName()).Err()
			}
			for _, toolName := range toolCallDiscovery.GetToolNames() {
				tool, ok := toolSetNameToToolNameToTool[toolCallDiscovery.GetToolSetName()][toolName]
				if !ok {
					return status.Errorf(codes.InvalidArgument, "message %d block %d has unknown tool %q in tool set %q", i, j, toolName, toolCallDiscovery.GetToolSetName()).Err()
				}
				if _, ok := toolNameToTool[tool.GetName()]; ok {
					continue
				}
				toolNameToTool[tool.GetName()] = tool
				request.Tools = append(request.Tools, tool)
			}
		}
	}

	// Instantiate the text to text accumulator if it doesn't exist in context.
	var textToTextAccumulator *ai.TextToTextAccumulator
	if v, ok := ctx.Value(tttAccumulatorKey{}).(*ai.TextToTextAccumulator); ok {
		textToTextAccumulator = v
	} else {
		textToTextAccumulator = ai.NewTextToTextAccumulator()
	}

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
	eg.Go(func() error {
		getChatRequest := &pb.GetChatRequest{Name: chatRn.String()}
		var err error
		chat, err = s.GetChat(ctxEg, getChatRequest)
		if err != nil {
			if !status.HasCode(err, codes.NotFound) {
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

	// Filter out deleted messages.
	messages := request.GetMessages()
	request.Messages = nil
	for _, message := range messages {
		if message.GetDeleteTime() == nil {
			request.Messages = append(request.Messages, message)
		}
	}

	// Process the request.
	if err := provider.TextToTextStream(request, wrapper); err != nil {
		return err
	}

	response := textToTextAccumulator.Response()
	ai.SetModelUsagePrices(response.GetModelUsage(), model.GetTtt().GetPricing())
	recordModelUsage(response.GetModelUsage())
	recordGenerationMetrics(request.GetModel(), response.GetGenerationMetrics())

	// Wait on the errgroup.
	if err := eg.Wait(); err != nil {
		return err
	}

	// Update the chat.
	chat.Metadata.Messages = append(messages, response.GetMessage())
	redactInlineImageData(chat.Metadata.Messages)
	chat.Metadata.ModelUsages = append(chat.Metadata.ModelUsages, wrapper.modelUsage)
	for k, v := range request.Labels {
		aip.SetLabel(chat, k, v)
	}
	updateChatRequest := &pb.UpdateChatRequest{
		Chat:       chat,
		UpdateMask: pbfieldmask.FromPaths("metadata", "labels").Proto(),
	}
	if _, err := s.UpdateChat(ctx, updateChatRequest); err != nil {
		return err
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
				return status.Errorf(codes.Internal, "tool call targets unknown tool %q", toolCall.Name).
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
		resp = &pb.TextToTextStreamResponse{
			Content: &pb.TextToTextStreamResponse_ModelUsage{
				ModelUsage: w.modelUsage,
			},
		}
	}

	if err := w.textToTextAccumulator.Add(resp); err != nil {
		return status.Errorf(codes.Internal, "accumulating stream events: %v", err).Err()
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
		ToolSets:      request.ToolSets,
		Configuration: request.Configuration,
		Labels:        request.Labels,
	}
	accumulator := ai.NewTextToTextAccumulator()
	ctx = context.WithValue(ctx, tttAccumulatorKey{}, accumulator)

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

	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	return accumulator.Response(), nil
}

// Add this function at the bottom of the file:

func redactInlineImageData(messages []*aipb.Message) {
	for _, message := range messages {
		for _, block := range message.GetBlocks() {
			if img := block.GetImage(); img != nil {
				if _, ok := img.Source.(*aipb.Image_Data); ok {
					img.Source = &aipb.Image_Data{Data: nil}
				}
			}
		}
	}
}

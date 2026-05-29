package ai_service

import (
	"context"
	"fmt"
	"io"
	"maps"
	"strings"

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

func (s *Service) TextToTextStream(originalRequest *pb.TextToTextStreamRequest, srv pb.AiService_TextToTextStreamServer) error {
	accumulator, _ := srv.Context().Value(tttAccumulatorKey{}).(*ai.TextToTextAccumulator)
	if accumulator == nil {
		accumulator = ai.NewTextToTextAccumulator()
	}

	var blockIndexOffset int64
	for {
		request := proto.CloneOf(originalRequest)
		if len(accumulator.Message.GetBlocks()) > 0 {
			request.Messages = append(request.Messages, proto.CloneOf(accumulator.Message))
		}
		if err := s.textToTextStream(request, srv, accumulator, blockIndexOffset); err != nil {
			return err
		}

		// Check if we need to loop again.
		var loop bool
		for _, block := range accumulator.Message.GetBlocks() {
			if block.Index < blockIndexOffset {
				continue
			}
			if block.GetToolCall().GetResult() != nil {
				loop = true
				break
			}
		}
		if !loop {
			return nil
		}
		blockIndexOffset = int64(len(accumulator.Message.Blocks))
	}
}

// textToTextStream executes a single turn of text-to-text streaming. It uses the provided
// accumulator to collect stream events, and applies blockIndexOffset to ensure block indices
// are globally unique across multiple turns.
func (s *Service) textToTextStream(
	request *pb.TextToTextStreamRequest,
	srv pb.AiService_TextToTextStreamServer,
	accumulator *ai.TextToTextAccumulator,
	blockIndexOffset int64,
) error {
	ctx := srv.Context()

	// Reconstruct messages to insert tool result messages for prior discovery tool calls.
	var reconstructedMessages []*aipb.Message
	for _, message := range request.GetMessages() {
		reconstructedMessages = append(reconstructedMessages, message)
		var toolResultBlocks []*aipb.Block
		for _, block := range ai.FilterBlocks(message.GetBlocks(), ai.BlockTypeToolCall) {
			toolResult := block.GetToolCall().GetResult()
			if toolResult != nil {
				toolResultBlocks = append(toolResultBlocks, ai.NewToolResultBlock(toolResult))
			}
		}
		if len(toolResultBlocks) > 0 {
			reconstructedMessages = append(reconstructedMessages, ai.NewToolMessage(toolResultBlocks...))
		}
	}
	request.Messages = reconstructedMessages

	// Resolve provider and model.
	provider, model, err := s.GetTextToTextProvider(ctx, request.Model)
	if err != nil {
		return err
	}
	if err := checkModelDeprecation(model); err != nil {
		return status.Errorf(codes.FailedPrecondition, err.Error()).Err()
	}

	// Apply default configuration.
	if request.Configuration == nil {
		request.Configuration = &pb.TextToTextConfiguration{}
	}
	if request.Configuration.MaxTokens == 0 {
		request.Configuration.MaxTokens = model.Ttt.OutputTokenLimit
	}

	// Validate model capabilities against request.
	if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED && !model.GetTtt().GetReasoning() {
		return status.Errorf(codes.InvalidArgument, "%s does not support reasoning", request.Model).Err()
	}
	if len(request.Tools) > 0 && !model.GetTtt().GetToolCall() {
		return status.Errorf(codes.InvalidArgument, "%s does not support tool calling", request.Model).Err()
	}

	// Build tool set index: tool set name -> tool name -> tool.
	toolSetNameToToolNameToTool := make(map[string]map[string]*aipb.Tool, len(request.GetToolSets()))
	for _, toolSet := range request.GetToolSets() {
		toolNameToTool := make(map[string]*aipb.Tool, len(toolSet.GetTools()))
		var discoveredTools []*aipb.Tool
		for _, tool := range toolSet.GetTools() {
			toolNameToTool[tool.GetName()] = tool
			if val, _ := aip.GetAnnotation(tool, aitool.AnnotationKeyPreDiscoveredTool); val == aip.LabelValueTrue {
				discoveredTools = append(discoveredTools, tool)
			}
		}
		toolSetNameToToolNameToTool[toolSet.GetName()] = toolNameToTool
		// Only add the discovery tool if some tools in the set are not pre-discovered.
		if len(discoveredTools) != len(toolSet.GetTools()) {
			request.Tools = append(request.Tools, toolSet.DiscoveryTool)
		}
		request.Tools = append(request.Tools, discoveredTools...)
	}

	// Build tool name index.
	toolNameToTool := make(map[string]*aipb.Tool, len(request.GetTools()))
	for _, tool := range request.GetTools() {
		toolNameToTool[tool.GetName()] = tool
	}

	// Replay prior discovery tool calls to rebuild the set of available tools.
	for i, message := range request.GetMessages() {
		for j, block := range ai.FilterBlocks(message.GetBlocks(), ai.BlockTypeToolCall) {
			toolCall := block.GetToolCall()
			toolType, ok := aip.GetAnnotation(toolCall, aitool.AnnotationKeyToolType)
			if !ok || toolType != aitool.AnnotationValueToolTypeDiscovery {
				continue
			}

			toolSetName, ok := aip.GetAnnotation(toolCall, aitool.AnnotationKeyToolSetName)
			if !ok {
				continue
			}
			if _, ok := toolSetNameToToolNameToTool[toolSetName]; !ok {
				return status.Errorf(codes.InvalidArgument, "message %d block %d has unknown tool set %q", i, j, toolSetName).Err()
			}
			toolResult := toolCall.GetResult()
			if toolResult == nil {
				return status.Errorf(codes.Internal, "message %d block %d has discovery tool call to %q with no result", i, j, toolSetName).Err()
			}
			discoveredToolsString, ok := aip.GetAnnotation(toolResult, aitool.AnnotationKeyDiscoveredTools)
			if !ok {
				continue
			}
			for _, discoveredTool := range strings.Split(discoveredToolsString, ",") {
				tool, ok := toolSetNameToToolNameToTool[toolSetName][discoveredTool]
				if !ok {
					return status.Errorf(codes.InvalidArgument, "message %d block %d has unknown tool %q in tool set %q", i, j, discoveredTool, toolSetName).Err()
				}
				if _, ok := toolNameToTool[tool.GetName()]; ok {
					return status.Errorf(codes.InvalidArgument, "message %d block %d has already discovered tool %q in tool set %q", i, j, discoveredTool, toolSetName).Err()
				}
				toolNameToTool[tool.GetName()] = tool
				request.Tools = append(request.Tools, tool)
			}
		}
	}

	// Filter out deleted messages.
	messages := request.GetMessages()
	request.Messages = nil
	for _, message := range messages {
		if message.GetDeleteTime() == nil {
			request.Messages = append(request.Messages, message)
		}
	}

	// Get or create chat.
	var chatRn *aipb.ChatResourceName
	if request.GetParent() == "" {
		chatRn = new(textToTextDefaultUserRn.ChatResourceName(aip.NewSystemGeneratedBase32ResourceID()))
	} else {
		chatRn = &aipb.ChatResourceName{}
		if err := chatRn.UnmarshalString(request.GetParent()); err != nil {
			return status.Errorf(codes.InvalidArgument, "unmarshaling parent: %v", err).Err()
		}
	}

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
				Chat:   &aipb.Chat{Metadata: &aipb.ChatMetadata{}},
			}
			chat, err = s.CreateChat(ctxEg, createChatRequest)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Stream the response from the provider.
	wrapper := &tttStreamWrapper{
		AiService_TextToTextStreamServer: srv,
		textToTextAccumulator:            accumulator,
		model:                            model,
		modelUsage:                       &aipb.ModelUsage{},
		toolNameToTool:                   toolNameToTool,
		toolSetNameToToolNameToTool:      toolSetNameToToolNameToTool,
		toolCallIDToToolCall:             map[string]*aipb.ToolCall{},
		blockIndexOffset:                 blockIndexOffset,
	}

	if err := provider.TextToTextStream(request, wrapper); err != nil {
		return err
	}

	response := accumulator.Response()
	ai.SetModelUsagePrices(response.GetModelUsage(), model.GetTtt().GetPricing())
	recordModelUsage(response.GetModelUsage())
	recordGenerationMetrics(request.GetModel(), response.GetGenerationMetrics())

	// Wait for chat creation.
	if err := eg.Wait(); err != nil {
		return err
	}

	// Persist messages and model usage to the chat.
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
	textToTextAccumulator       *ai.TextToTextAccumulator
	model                       *aipb.Model
	modelUsage                  *aipb.ModelUsage
	toolNameToTool              map[string]*aipb.Tool
	toolSetNameToToolNameToTool map[string]map[string]*aipb.Tool
	toolCallIDToToolCall        map[string]*aipb.ToolCall
	blockIndexOffset            int64
}

func (w *tttStreamWrapper) Send(resp *pb.TextToTextStreamResponse) error {
	if c, ok := resp.GetContent().(*pb.TextToTextStreamResponse_Block); ok && w.blockIndexOffset > 0 {
		block := proto.CloneOf(c.Block)
		block.Index += w.blockIndexOffset
		resp = &pb.TextToTextStreamResponse{
			Content: &pb.TextToTextStreamResponse_Block{Block: block},
		}
	}

	if c, ok := resp.GetContent().(*pb.TextToTextStreamResponse_Block); ok {
		c.Block.Index += w.blockIndexOffset
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
				return status.Errorf(codes.Internal, "tool call targets unknown tool %q", toolCall.Name).
					WithDetails(&aipb.ToolCallRecoverableError{
						ToolCallBlock:   c.Block,
						ToolResultBlock: ai.NewToolResultBlock(ai.NewErrorToolResult(toolCall.Name, toolCall.Id, fmt.Errorf("unknown tool"))),
					}).Err()
			}

			// Copy annotations.
			maps.Copy(toolCall.Annotations, tool.GetAnnotations())

			// Annotate discovery tool calls.
			if !toolCall.GetPartial() {
				if toolType, _ := aip.GetAnnotation(toolCall, aitool.AnnotationKeyToolType); toolType == aitool.AnnotationValueToolTypeDiscovery {
					toolCall.Result = processDiscoveryToolCall(toolCall, w.toolSetNameToToolNameToTool, w.toolNameToTool)
				}
			}
		}

		// Dedupe partial tool calls.
		if partialToolCall := c.Block.GetPartialToolCall(); partialToolCall != nil {
			if last, ok := w.toolCallIDToToolCall[partialToolCall.Id]; ok && proto.Equal(last, partialToolCall) {
				return nil
			}
			w.toolCallIDToToolCall[partialToolCall.Id] = partialToolCall
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

func processDiscoveryToolCall(
	toolCall *aipb.ToolCall,
	toolSetNameToToolNameToTool map[string]map[string]*aipb.Tool,
	toolNameToTool map[string]*aipb.Tool,
) *aipb.ToolResult {
	toolSetName, _ := aip.GetAnnotation(toolCall, aitool.AnnotationKeyToolSetName)
	toolNameToToolInSet, ok := toolSetNameToToolNameToTool[toolSetName]
	if !ok {
		return ai.NewErrorToolResult(toolCall.Name, toolCall.Id, fmt.Errorf("unknown tool set %q", toolSetName))
	}

	discoveryResult, err := aitool.ParseDiscoveryToolCall(toolCall)
	if err != nil {
		return ai.NewErrorToolResult(toolCall.Name, toolCall.Id, err)
	}

	var validToolNames []string
	var errors []string
	for _, discoveredToolName := range discoveryResult.GetToolNames() {
		discoveredTool, exists := toolNameToToolInSet[discoveredToolName]
		if !exists {
			errors = append(errors, fmt.Sprintf("discovery references unknown tool %q in tool set %q", discoveredToolName, toolSetName))
			continue
		}
		if _, alreadyDiscovered := toolNameToTool[discoveredTool.GetName()]; alreadyDiscovered {
			errors = append(errors, fmt.Sprintf("tool %q already discovered", discoveredToolName))
			continue
		}
		toolNameToTool[discoveredTool.GetName()] = discoveredTool
		validToolNames = append(validToolNames, discoveredToolName)
	}

	annotations := map[string]string{}
	if len(validToolNames) > 0 {
		annotations[aitool.AnnotationKeyDiscoveredTools] = strings.Join(validToolNames, ",")
	}

	if len(errors) > 0 {
		errMsg := strings.Join(errors, "; ")
		if len(validToolNames) > 0 {
			errMsg = fmt.Sprintf("%s (successfully discovered: %s)", errMsg, strings.Join(validToolNames, ", "))
		}
		toolResult := ai.NewErrorToolResult(toolCall.Name, toolCall.Id, fmt.Errorf("%s", errMsg))
		toolResult.Annotations = annotations
		return toolResult
	}

	toolResult := ai.NewToolResult(toolCall.Name, toolCall.Id, "ok")
	toolResult.Annotations = annotations
	return toolResult
}

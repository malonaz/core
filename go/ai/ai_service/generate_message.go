package ai_service

import (
	"context"
	"fmt"
	"io"
	"maps"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	aitool "github.com/malonaz/core/go/ai/tool"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/grpcinproc"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
	grpcstatus "google.golang.org/grpc/status"
)

type generateMessageAccumulatorKey struct{}

// GenerateMessage is the unary flavor of StreamGenerateMessage: it drains the
// stream through an accumulator and returns the aggregate response.
func (s *Service) GenerateMessage(ctx context.Context, request *pb.GenerateMessageRequest) (*pb.GenerateMessageResponse, error) {
	accumulator := ai.NewMessageAccumulator()
	ctx = context.WithValue(ctx, generateMessageAccumulatorKey{}, accumulator)

	serverStreamClient := grpcinproc.NewServerStreamAsClient[
		pb.GenerateMessageRequest,
		pb.StreamGenerateMessageResponse,
		pb.AiService_StreamGenerateMessageServer,
	](s.StreamGenerateMessage)

	stream, err := serverStreamClient(ctx, request)
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

	return &pb.GenerateMessageResponse{
		GeneratedMessage:  accumulator.Message,
		StopReason:        accumulator.StopReason,
		ModelUsage:        accumulator.ModelUsage,
		GenerationMetrics: accumulator.GenerationMetrics,
	}, nil
}

func (s *Service) StreamGenerateMessage(request *pb.GenerateMessageRequest, srv pb.AiService_StreamGenerateMessageServer) error {
	ctx := srv.Context()

	// GenerateMessage plants an accumulator in the context to capture the stream.
	accumulator, _ := ctx.Value(generateMessageAccumulatorKey{}).(*ai.MessageAccumulator)
	if accumulator == nil {
		accumulator = ai.NewMessageAccumulator()
	}

	chatRn := &aipb.ChatResourceName{}
	if err := chatRn.UnmarshalString(request.GetParent()); err != nil {
		return status.Errorf(codes.InvalidArgument, "unmarshaling parent: %v", err).Err()
	}

	providerClient, model, err := s.GetGenerateMessageProvider(ctx, request.Model)
	if err != nil {
		return err
	}
	if err := checkModelDeprecation(model); err != nil {
		return status.Errorf(codes.FailedPrecondition, err.Error()).Err()
	}

	if request.Configuration == nil {
		request.Configuration = &pb.MessageGenerationConfiguration{}
	}
	if request.Configuration.MaxTokens == 0 {
		request.Configuration.MaxTokens = model.Ttt.OutputTokenLimit
	}
	if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED && !model.GetTtt().GetReasoning() {
		return status.Errorf(codes.InvalidArgument, "%s does not support reasoning", request.Model).Err()
	}
	if (len(request.GetTools()) > 0 || len(request.GetToolSets()) > 0) && !model.GetTtt().GetToolCall() {
		return status.Errorf(codes.InvalidArgument, "%s does not support tool calling", request.Model).Err()
	}

	// Fetch (or lazily create) the chat, and load its valid message history:
	// errored and superseded messages are excluded.
	var chat *aipb.Chat
	var history []*aipb.Message
	eg, ctxEg := errgroup.WithContext(ctx)
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
				Chat:   &aipb.Chat{},
			}
			chat, err = s.CreateChat(ctxEg, createChatRequest)
			if err != nil {
				return err
			}
		}
		return nil
	})
	eg.Go(func() error {
		listMessagesRequest := &pb.ListMessagesRequest{
			Parent: chatRn.String(),
			Filter: fmt.Sprintf("-status:* AND -labels.%q:*", aipb.Labels.Superseded.GetKey()),
			// create_time asc keeps the conversation in order and stable while paginating.
			OrderBy: "create_time asc",
		}
		var err error
		history, err = aip.Paginate[*aipb.Message](ctxEg, listMessagesRequest, s.ListMessages)
		return err
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	// Fork: truncate the history after previous_message, superseding the tail.
	if request.GetPreviousMessage() != "" {
		previousMessageIndex := -1
		for i, message := range history {
			if message.GetName() == request.GetPreviousMessage() {
				previousMessageIndex = i
				break
			}
		}
		if previousMessageIndex == -1 {
			return status.Errorf(codes.NotFound, "previous message %q not found in chat %q", request.GetPreviousMessage(), chatRn.String()).Err()
		}
		if history[previousMessageIndex].GetRole() != aipb.Role_ROLE_ASSISTANT {
			return status.Errorf(codes.InvalidArgument, "previous message %q is not an assistant message", request.GetPreviousMessage()).Err()
		}
		for _, supersededMessage := range history[previousMessageIndex+1:] {
			aip.SetLabel(supersededMessage, aipb.Labels.Superseded.GetKey(), aip.LabelValueTrue)
			updateMessageRequest := &pb.UpdateMessageRequest{
				Message:    supersededMessage,
				UpdateMask: pbfieldmask.FromPaths("labels").Proto(),
			}
			if _, err := s.UpdateMessage(ctx, updateMessageRequest); err != nil {
				return err
			}
		}
		history = history[:previousMessageIndex+1]
	}

	// Persist the input messages, in order. They are not echoed back to the client.
	inputMessages := make([]*aipb.Message, 0, len(request.GetMessages()))
	for _, message := range request.GetMessages() {
		createMessageRequest := &pb.CreateMessageRequest{
			Parent:  chatRn.String(),
			Message: message,
		}
		inputMessage, err := s.CreateMessage(ctx, createMessageRequest)
		if err != nil {
			return err
		}
		inputMessages = append(inputMessages, inputMessage)
	}
	history = append(history, inputMessages...)
	if len(history) == 0 {
		return status.Errorf(codes.FailedPrecondition, "chat %q has no messages to generate from", chatRn.String()).Err()
	}

	// Index tools & tool sets, injecting discovery tools and pre-discovered tools.
	// Discoverable tools are never added to the provider-visible tool list:
	// they are invoked through the single Execute tool, keeping the tool list
	// static so the prompt cache survives discoveries.
	toolSetNameToToolNameToTool := make(map[string]map[string]*aipb.Tool, len(request.GetToolSets()))
	var hasDiscoverableTools bool
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
		if len(discoveredTools) != len(toolSet.GetTools()) {
			hasDiscoverableTools = true
			request.Tools = append(request.Tools, toolSet.DiscoveryTool)
		}
		request.Tools = append(request.Tools, discoveredTools...)
	}
	if hasDiscoverableTools {
		// The Execute tool name is reserved: a user tool with the same name
		// would be shadowed by the generic execute tool.
		for _, toolSet := range request.GetToolSets() {
			for _, tool := range toolSet.GetTools() {
				if tool.GetName() == aitool.ExecuteToolName {
					return status.Errorf(codes.InvalidArgument, "tool set %q contains a tool named %q, which is reserved for the execute tool", toolSet.GetName(), aitool.ExecuteToolName).Err()
				}
			}
		}
		for _, tool := range request.GetTools() {
			if tool.GetName() == aitool.ExecuteToolName {
				return status.Errorf(codes.InvalidArgument, "tool %q collides with the reserved execute tool name", aitool.ExecuteToolName).Err()
			}
		}
		request.Tools = append(request.Tools, aitool.CreateExecuteTool())
	}

	toolNameToTool := make(map[string]*aipb.Tool, len(request.GetTools()))
	for _, tool := range request.GetTools() {
		toolNameToTool[tool.GetName()] = tool
	}

	// Replay discovery tool call results from the conversation history so that
	// previously discovered tools remain available to the model.
	for i, message := range history {
		for j, block := range ai.FilterBlocks(message.GetBlocks(), ai.BlockTypeToolResult) {
			toolResult := block.GetToolResult()
			toolSetName, ok := aip.GetAnnotation(toolResult, aitool.AnnotationKeyToolSetName)
			if !ok {
				continue
			}
			if _, ok := toolSetNameToToolNameToTool[toolSetName]; !ok {
				return status.Errorf(codes.InvalidArgument, "message %d block %d has unknown tool set %q", i, j, toolSetName).Err()
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
				// A tool discovered twice (or pre-discovered then discovered) is
				// benign; failing here would poison the whole chat for the rest
				// of its life.
				// Register for Execute routing only; the provider-visible tool
				// list stays untouched to preserve the prompt cache.
				toolNameToTool[tool.GetName()] = tool
			}
		}
	}

	wrapper := &generateMessageWrapper{
		AiService_StreamGenerateMessageServer: srv,
		messageAccumulator:                    accumulator,
		model:                                 model,
		modelUsage:                            &aipb.ModelUsage{Model: request.Model},
		toolNameToTool:                        toolNameToTool,
		toolSetNameToToolNameToTool:           toolSetNameToToolNameToTool,
		toolCallIDToToolCall:                  map[string]*aipb.ToolCall{},
	}

	// The service owns the sender lifecycle; providers only emit events.
	sender := provider.NewAsyncMessageContentSender(wrapper, 100)
	generationError := providerClient.StreamGenerateMessage(ctx, request, history, sender)
	sender.Close()
	if generationError == nil {
		generationError = sender.Wait(ctx)
	}
	if generationError != nil {
		s.markGenerationFailure(ctx, chatRn, inputMessages, accumulator, generationError)
		return generationError
	}

	ai.SetModelUsagePrices(wrapper.modelUsage, model.GetTtt().GetPricing())
	recordModelUsage(wrapper.modelUsage)
	recordGenerationMetrics(request.GetModel(), accumulator.GenerationMetrics)

	// Persist the generated assistant message.
	generatedMessage := accumulator.Message
	redactInlineImageData(generatedMessage)
	generatedMessage.Labels = request.GetLabels()
	generatedMessage.Model = request.GetModel()
	generatedMessage.ModelUsage = wrapper.modelUsage
	generatedMessage.Price = ai.ModelUsageCost(wrapper.modelUsage)
	createMessageRequest := &pb.CreateMessageRequest{
		Parent:  chatRn.String(),
		Message: generatedMessage,
	}
	persistedMessage, err := s.CreateMessage(ctx, createMessageRequest)
	if err != nil {
		return err
	}

	// Roll the message's price up into the chat.
	chat.Price += persistedMessage.GetPrice()
	updateChatRequest := &pb.UpdateChatRequest{
		Chat:       chat,
		UpdateMask: pbfieldmask.FromPaths("price").Proto(),
	}
	if _, err := s.UpdateChat(ctx, updateChatRequest); err != nil {
		return err
	}

	// The persisted message is the final event of the stream.
	finalResponse := &pb.StreamGenerateMessageResponse{
		Content: &pb.StreamGenerateMessageResponse_GeneratedMessage{GeneratedMessage: persistedMessage},
	}
	return srv.Send(finalResponse)
}

// markGenerationFailure flags the input messages of a failed generation (and
// any partially generated assistant message) with the generation error, so
// they are excluded from future generations. Best effort: the original
// generation error is what surfaces to the caller.
func (s *Service) markGenerationFailure(
	ctx context.Context,
	chatRn *aipb.ChatResourceName,
	inputMessages []*aipb.Message,
	accumulator *ai.MessageAccumulator,
	generationError error,
) {
	// The request context is typically already cancelled when generation fails,
	// so detach from it to ensure the failure is still persisted.
	ctx = context.WithoutCancel(ctx)
	errorStatus := grpcstatus.Convert(generationError).Proto()

	for _, inputMessage := range inputMessages {
		inputMessage.Status = errorStatus
		updateMessageRequest := &pb.UpdateMessageRequest{
			Message:    inputMessage,
			UpdateMask: pbfieldmask.FromPaths("status").Proto(),
		}
		if _, err := s.UpdateMessage(ctx, updateMessageRequest); err != nil {
			s.log.Error("marking input message as failed", "message", inputMessage.GetName(), "error", err)
		}
	}

	// Persist the partial assistant message for debugging. Partial tool call
	// blocks are dropped: they are invalid on a persisted message.
	partialMessage := accumulator.Message
	partialMessage.Blocks = ai.FilterBlocks(partialMessage.GetBlocks(), ai.BlockTypeText, ai.BlockTypeThought, ai.BlockTypeToolCall, ai.BlockTypeImage)
	if len(partialMessage.GetBlocks()) == 0 {
		return
	}
	redactInlineImageData(partialMessage)
	partialMessage.Model = "" // Keep pricing off failed partials; usage was not finalized.
	partialMessage.Status = errorStatus
	createMessageRequest := &pb.CreateMessageRequest{
		Parent:  chatRn.String(),
		Message: partialMessage,
	}
	if _, err := s.CreateMessage(ctx, createMessageRequest); err != nil {
		s.log.Error("persisting partial assistant message", "chat", chatRn.String(), "error", err)
	}
}

type generateMessageWrapper struct {
	pb.AiService_StreamGenerateMessageServer
	messageAccumulator          *ai.MessageAccumulator
	model                       *aipb.Model
	modelUsage                  *aipb.ModelUsage
	toolNameToTool              map[string]*aipb.Tool
	toolSetNameToToolNameToTool map[string]map[string]*aipb.Tool
	toolCallIDToToolCall        map[string]*aipb.ToolCall
}

func (w *generateMessageWrapper) Send(response *pb.StreamGenerateMessageResponse) error {
	switch c := response.GetContent().(type) {
	case *pb.StreamGenerateMessageResponse_Block:
		var toolCall *aipb.ToolCall
		if c.Block.GetToolCall() != nil {
			toolCall = c.Block.GetToolCall()
		}
		if c.Block.GetPartialToolCall() != nil {
			toolCall = c.Block.GetPartialToolCall()
		}
		if toolCall != nil {
			if toolCall.Annotations == nil {
				toolCall.Annotations = map[string]string{}
			}
			tool, ok := w.toolNameToTool[toolCall.Name]
			if !ok {
				return status.Errorf(codes.Internal, "tool call targets unknown tool %q", toolCall.Name).
					WithDetails(&aipb.ToolCallRecoverableError{
						ToolCallBlock:   c.Block,
						ToolResultBlock: ai.NewToolResultBlock(ai.NewErrorToolResult(toolCall.Name, toolCall.Id, fmt.Errorf("unknown tool"))),
					}).Err()
			}

			maps.Copy(toolCall.Annotations, tool.GetAnnotations())

			if !toolCall.GetPartial() {
				switch toolType, _ := aip.GetAnnotation(toolCall, aitool.AnnotationKeyToolType); toolType {
				case aitool.AnnotationValueToolTypeDiscovery:
					toolCall.Result = processDiscoveryToolCall(toolCall, w.toolSetNameToToolNameToTool, w.toolNameToTool)

				case aitool.AnnotationValueToolTypeExecute:
					// Unwrap into the discovered tool's call so downstream
					// consumers see the real tool call.
					unwrappedToolCall, err := aitool.UnwrapExecuteToolCall(toolCall, w.toolNameToTool)
					if err != nil {
						return status.FromError(err, "unwrapping execute tool call").
							WithDetails(&aipb.ToolCallRecoverableError{
								ToolCallBlock:   c.Block,
								ToolResultBlock: ai.NewToolResultBlock(ai.NewErrorToolResult(toolCall.Name, toolCall.Id, err)),
							}).Err()
					}
					c.Block.Content = &aipb.Block_ToolCall{ToolCall: unwrappedToolCall}
				}
			}
		}

		if partialToolCall := c.Block.GetPartialToolCall(); partialToolCall != nil {
			if last, ok := w.toolCallIDToToolCall[partialToolCall.Id]; ok && proto.Equal(last, partialToolCall) {
				return nil
			}
			w.toolCallIDToToolCall[partialToolCall.Id] = partialToolCall
		}

	case *pb.StreamGenerateMessageResponse_ModelUsage:
		if ai.IsModelUsageEmpty(c.ModelUsage) {
			return nil
		}
		proto.Merge(w.modelUsage, c.ModelUsage)
		ai.SetModelUsagePrices(w.modelUsage, w.model.GetTtt().GetPricing())
		response = &pb.StreamGenerateMessageResponse{
			Content: &pb.StreamGenerateMessageResponse_ModelUsage{
				ModelUsage: w.modelUsage,
			},
		}
	}

	if err := w.messageAccumulator.Add(response); err != nil {
		return status.Errorf(codes.Internal, "accumulating stream events: %v", err).Err()
	}
	return w.AiService_StreamGenerateMessageServer.Send(response)
}

func redactInlineImageData(messages ...*aipb.Message) {
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

	var (
		discoveredToolNames []string
		discoveredTools     []*aipb.Tool
		unknownToolNames    []string
	)
	for _, discoveredToolName := range discoveryResult.GetToolNames() {
		tool, exists := toolNameToToolInSet[discoveredToolName]
		if !exists {
			unknownToolNames = append(unknownToolNames, discoveredToolName)
			continue
		}
		// Already discovered (earlier in this call or in a previous turn):
		// simply omit it from the result.
		if _, alreadyDiscovered := toolNameToTool[tool.GetName()]; alreadyDiscovered {
			continue
		}
		toolNameToTool[tool.GetName()] = tool
		discoveredToolNames = append(discoveredToolNames, discoveredToolName)
		discoveredTools = append(discoveredTools, tool)
	}

	// Only unknown tools were named: the model must correct itself, so surface an error.
	if len(discoveredTools) == 0 && len(unknownToolNames) > 0 {
		return ai.NewErrorToolResult(toolCall.Name, toolCall.Id, fmt.Errorf(
			"discovery references unknown tool(s) %s in tool set %q",
			strings.Join(unknownToolNames, ", "), toolSetName,
		))
	}

	annotations := map[string]string{
		aitool.AnnotationKeyToolSetName:     toolSetName,
		aitool.AnnotationKeyDiscoveredTools: strings.Join(discoveredToolNames, ","),
	}

	// Return the discovered tools' schemas as the tool result so the model can
	// invoke them through the Execute tool without any change to the
	// provider-visible tool list.
	// Only the fields the model needs are included: annotations are internal
	// routing metadata and would waste context tokens.
	modelVisibleTools := make([]*aipb.Tool, 0, len(discoveredTools))
	for _, discoveredTool := range discoveredTools {
		modelVisibleTools = append(modelVisibleTools, &aipb.Tool{
			Name:        discoveredTool.GetName(),
			Description: discoveredTool.GetDescription(),
			JsonSchema:  discoveredTool.GetJsonSchema(),
		})
	}
	discovery := &aipb.ToolCallDiscovery{
		ToolSetName:      toolSetName,
		ToolNames:        discoveredToolNames,
		Tools:            modelVisibleTools,
		UnknownToolNames: unknownToolNames,
	}
	discoveryStruct, err := pbutil.MarshalToStruct(discovery)
	if err != nil {
		toolResult := ai.NewErrorToolResult(toolCall.Name, toolCall.Id, fmt.Errorf("marshaling discovered tools: %w", err))
		toolResult.Annotations = annotations
		return toolResult
	}
	toolResult := ai.NewToolResult(toolCall.Name, toolCall.Id, "")
	toolResult.Result = &aipb.ToolResult_StructuredContent{StructuredContent: structpb.NewStructValue(discoveryStruct)}
	toolResult.Annotations = annotations
	return toolResult
}

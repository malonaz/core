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

type tttAccumulatorKey struct{}

func (s *Service) TextToTextStream(originalRequest *pb.TextToTextStreamRequest, srv pb.AiService_TextToTextStreamServer) error {
	accumulator, _ := srv.Context().Value(tttAccumulatorKey{}).(*ai.TextToTextAccumulator)
	if accumulator == nil {
		accumulator = ai.NewTextToTextAccumulator()
	}

	return s.textToTextStream(originalRequest, srv, accumulator)
}

func (s *Service) textToTextStream(
	request *pb.TextToTextStreamRequest,
	srv pb.AiService_TextToTextStreamServer,
	accumulator *ai.TextToTextAccumulator,
) error {
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
	if (len(request.GetTools()) > 0 || len(request.GetToolSets()) > 0) && !model.GetTtt().GetToolCall() {
		return status.Errorf(codes.InvalidArgument, "%s does not support tool calling", request.Model).Err()
	}

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
		if len(discoveredTools) != len(toolSet.GetTools()) {
			request.Tools = append(request.Tools, toolSet.DiscoveryTool)
		}
		request.Tools = append(request.Tools, discoveredTools...)
	}

	toolNameToTool := make(map[string]*aipb.Tool, len(request.GetTools()))
	for _, tool := range request.GetTools() {
		toolNameToTool[tool.GetName()] = tool
	}

	// Process discovery tool call results.
	for i, message := range request.GetMessages() {
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
				if _, ok := toolNameToTool[tool.GetName()]; ok {
					return status.Errorf(codes.InvalidArgument, "message %d block %d has already discovered tool %q in tool set %q", i, j, discoveredTool, toolSetName).Err()
				}
				toolNameToTool[tool.GetName()] = tool
				request.Tools = append(request.Tools, tool)
			}
		}
	}

	messages := request.GetMessages()
	request.Messages = nil
	for _, message := range messages {
		if message.GetDeleteTime() == nil {
			request.Messages = append(request.Messages, message)
		}
	}

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

	wrapper := &tttStreamWrapper{
		AiService_TextToTextStreamServer: srv,
		textToTextAccumulator:            accumulator,
		model:                            model,
		modelUsage:                       &aipb.ModelUsage{},
		toolNameToTool:                   toolNameToTool,
		toolSetNameToToolNameToTool:      toolSetNameToToolNameToTool,
		toolCallIDToToolCall:             map[string]*aipb.ToolCall{},
	}

	if err := provider.TextToTextStream(request, wrapper); err != nil {
		return err
	}

	response := accumulator.Response()
	ai.SetModelUsagePrices(response.GetModelUsage(), model.GetTtt().GetPricing())
	recordModelUsage(response.GetModelUsage())
	recordGenerationMetrics(request.GetModel(), response.GetGenerationMetrics())

	if err := eg.Wait(); err != nil {
		return err
	}

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
				if toolType, _ := aip.GetAnnotation(toolCall, aitool.AnnotationKeyToolType); toolType == aitool.AnnotationValueToolTypeDiscovery {
					toolCall.Result = processDiscoveryToolCall(toolCall, w.toolSetNameToToolNameToTool, w.toolNameToTool)
				}
			}
		}

		if partialToolCall := c.Block.GetPartialToolCall(); partialToolCall != nil {
			if last, ok := w.toolCallIDToToolCall[partialToolCall.Id]; ok && proto.Equal(last, partialToolCall) {
				return nil
			}
			w.toolCallIDToToolCall[partialToolCall.Id] = partialToolCall
		}

	case *pb.TextToTextStreamResponse_ModelUsage:
		if ai.IsModelUsageEmpty(c.ModelUsage) {
			return nil
		}
		proto.Merge(w.modelUsage, c.ModelUsage)
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

func (s *Service) TextToText(ctx context.Context, request *pb.TextToTextRequest) (*pb.TextToTextResponse, error) {
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

	annotations := map[string]string{
		aitool.AnnotationKeyToolSetName: toolSetName,
	}
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

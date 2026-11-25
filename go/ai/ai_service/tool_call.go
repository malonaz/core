package ai_service

import (
	"context"
	"errors"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/gengo/ai/model"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// toolCallRequestIDNamespace derives a tool call's request id from its name: a replayed message writes it once.
var toolCallRequestIDNamespace = uuid.MustParse("8b1f5d3a-6e29-4c47-a0d2-9f3e71b4c858")

// insertMessage persists a message prepared by the generated create, with the tool calls it makes and the settlement
// of the calls its results answer, in one transaction.
func (s *Service) insertMessage(ctx context.Context, requestID string, message *aipb.Message) (*aipb.Message, error) {
	messageModel, err := model.MessageFromPb(message)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting message from pb to model: %v", err).Err()
	}
	toolCalls, err := toolCallsOf(message)
	if err != nil {
		return nil, err
	}
	toolCallRequestIDs := make([]string, len(toolCalls))
	toolCallModels := make([]*model.ToolCall, len(toolCalls))
	for i, toolCall := range toolCalls {
		toolCallRequestIDs[i] = uuid.NewV5(toolCallRequestIDNamespace, toolCall.GetName()).String()
		if toolCallModels[i], err = model.ToolCallFromPb(toolCall); err != nil {
			return nil, status.Errorf(codes.Internal, "converting tool call from pb to model: %v", err).Err()
		}
	}
	callIDToResult := map[string]*aipb.ToolResult{}
	var callIDs []string
	for _, block := range ai.FilterBlocks(message.GetBlocks(), ai.BlockTypeToolResult) {
		toolResult := block.GetToolResult()
		callIDToResult[toolResult.GetToolCallId()] = toolResult
		callIDs = append(callIDs, toolResult.GetToolCallId())
	}
	settle := func(pending []*model.ToolCall) ([]*model.ToolCall, error) {
		var settled []*model.ToolCall
		for _, pendingModel := range pending {
			toolCall, err := pendingModel.ToPb()
			if err != nil {
				return nil, err
			}
			toolCall.Result = proto.CloneOf(callIDToResult[toolCall.GetToolCallId()])
			toolCall.UpdateTime = message.GetCreateTime()
			if toolCall.Etag, err = aip.ComputeETag(toolCall); err != nil {
				return nil, err
			}
			settledModel, err := model.ToolCallFromPb(toolCall)
			if err != nil {
				return nil, err
			}
			settled = append(settled, settledModel)
		}
		return settled, nil
	}

	inserted, err := s.aiPostgresStore.InsertMessage(ctx, requestID, messageModel, toolCallRequestIDs, toolCallModels, callIDs, settle)
	if err != nil {
		if errors.Is(err, model.ErrMessageAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "message already exists").Err()
		}
		return nil, status.FromError(err, "inserting message").Err()
	}
	insertedMessage, err := inserted.ToPb()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting message from model to pb: %v", err).Err()
	}
	return insertedMessage, nil
}

// toolCallsOf returns the tool calls an assistant message makes, named by their position among its tool_call
// blocks, in order of appearance. A failed generation's partial message makes none: its calls never ran.
func toolCallsOf(message *aipb.Message) ([]*aipb.ToolCall, error) {
	if message.GetRole() != aipb.Role_ROLE_ASSISTANT || message.GetStatus() != nil {
		return nil, nil
	}
	var toolCalls []*aipb.ToolCall
	for i, block := range ai.FilterBlocks(message.GetBlocks(), ai.BlockTypeToolCall) {
		toolUse := block.GetToolCall()
		toolCall := &aipb.ToolCall{
			Name:        message.GetName() + "/toolCalls/" + strconv.Itoa(i+1),
			CreateTime:  message.GetCreateTime(),
			UpdateTime:  message.GetCreateTime(),
			Annotations: toolUse.GetAnnotations(),
			ToolName:    toolUse.GetName(),
			ToolCallId:  toolUse.GetId(),
			Arguments:   toolUse.GetArguments(),
			// A call the generation answered itself (a discovery, an unknown tool) carries its result already.
			Result: toolUse.GetResult(),
		}
		etag, err := aip.ComputeETag(toolCall)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "computing tool call etag: %v", err).Err()
		}
		toolCall.Etag = etag
		toolCalls = append(toolCalls, toolCall)
	}
	return toolCalls, nil
}

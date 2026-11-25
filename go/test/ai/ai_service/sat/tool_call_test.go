package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
)

// newToolUseBlock returns a tool_call block calling a tool, answered in the block when result is set.
func newToolUseBlock(t *testing.T, callID, toolName string, result *aipb.ToolResult) *aipb.Block {
	t.Helper()
	arguments, err := structpb.NewStruct(map[string]any{"tool_call_title": "Look " + callID + " up"})
	require.NoError(t, err)
	return &aipb.Block{Content: &aipb.Block_ToolCall{ToolCall: &aipb.ToolUse{Id: callID, Name: toolName, Arguments: arguments, Result: result}}}
}

// A message's tool calls are persisted with it, named by their position, and settled by the results that follow.
func TestToolCalls_PersistedWithTheirMessageAndSettledByResults(t *testing.T) {
	t.Parallel()
	userParent := newUserParent()
	chat, err := aiServiceClient.CreateChat(ctx, &aiservicepb.CreateChatRequest{Parent: userParent, Chat: &aipb.Chat{}})
	require.NoError(t, err)

	assistantMessage := &aipb.Message{
		Role: aipb.Role_ROLE_ASSISTANT,
		Blocks: []*aipb.Block{
			newToolUseBlock(t, "call_a", "Search", nil),
			newToolUseBlock(t, "call_b", "Tools_Discover", ai.NewToolResult("Tools_Discover", "call_b", "ok")),
		},
	}
	createMessageRequest := &aiservicepb.CreateMessageRequest{Parent: chat.GetName(), Message: assistantMessage, RequestId: uuid.MustNewV7().String()}
	created, err := aiServiceClient.CreateMessage(ctx, createMessageRequest)
	require.NoError(t, err)
	// A replayed create writes nothing twice.
	_, err = aiServiceClient.CreateMessage(ctx, createMessageRequest)
	require.NoError(t, err)

	listToolCallsResponse, err := aiServiceClient.ListToolCalls(ctx, &aiservicepb.ListToolCallsRequest{Parent: created.GetName(), OrderBy: "create_time"})
	require.NoError(t, err)
	require.Len(t, listToolCallsResponse.GetToolCalls(), 2)
	first, second := listToolCallsResponse.GetToolCalls()[0], listToolCallsResponse.GetToolCalls()[1]
	if first.GetToolCallId() != "call_a" {
		first, second = second, first
	}
	// What was called, with what: the block's own types.
	require.Equal(t, created.GetName()+"/toolCalls/1", first.GetName())
	require.Equal(t, "Search", first.GetToolName())
	require.Equal(t, "Look call_a up", first.GetArguments().GetFields()["tool_call_title"].GetStringValue())
	require.Nil(t, first.GetResult())
	// A call the generation answered itself has its result from the start.
	require.Equal(t, created.GetName()+"/toolCalls/2", second.GetName())
	require.Equal(t, "ok", second.GetResult().GetContent())

	// Pending calls show across the user's chats.
	pendingRequest := &aiservicepb.ListToolCallsRequest{Parent: userParent + "/chats/-/messages/-", Filter: "NOT result:*"}
	pendingResponse, err := aiServiceClient.ListToolCalls(ctx, pendingRequest)
	require.NoError(t, err)
	require.Len(t, pendingResponse.GetToolCalls(), 1)
	require.Equal(t, first.GetName(), pendingResponse.GetToolCalls()[0].GetName())

	// The message bringing the result settles the call with it.
	toolResult := ai.NewErrorToolResult("Search", "call_a", grpcstatus.Error(codes.PermissionDenied, "denied"))
	toolMessage := ai.NewToolMessage(ai.NewToolResultBlock(toolResult))
	_, err = aiServiceClient.CreateMessage(ctx, &aiservicepb.CreateMessageRequest{Parent: chat.GetName(), Message: toolMessage})
	require.NoError(t, err)
	settled, err := aiServiceClient.GetToolCall(ctx, &aiservicepb.GetToolCallRequest{Name: first.GetName()})
	require.NoError(t, err)
	require.Equal(t, int32(codes.PermissionDenied), settled.GetResult().GetError().GetCode())

	failedRequest := &aiservicepb.ListToolCallsRequest{Parent: userParent + "/chats/-/messages/-", Filter: "result.error:*"}
	failedResponse, err := aiServiceClient.ListToolCalls(ctx, failedRequest)
	require.NoError(t, err)
	require.Len(t, failedResponse.GetToolCalls(), 1)
	require.Equal(t, first.GetName(), failedResponse.GetToolCalls()[0].GetName())

	// A message with tool calls is deleted only with them.
	_, err = aiServiceClient.DeleteMessage(ctx, &aiservicepb.DeleteMessageRequest{Name: created.GetName()})
	grpcrequire.Error(t, codes.FailedPrecondition, err)
}

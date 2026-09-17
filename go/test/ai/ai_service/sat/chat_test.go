package sat

import (
	"testing"

	"github.com/stretchr/testify/require"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
)

func getChat(t *testing.T, name string) *aipb.Chat {
	t.Helper()
	chat, err := aiServiceClient.GetChat(ctx, &aiservicepb.GetChatRequest{Name: name})
	require.NoError(t, err)
	return chat
}

// Generations roll spend up into the chat and message writes keep the
// per-role counts live.
func TestChatRollup(t *testing.T) {
	t.Parallel()
	parent := newChatParent()
	userMessage := newScriptedUserMessage(t, newAssistantText("one"), newAssistantText("two"))

	// Turn 1: one user message in, one assistant message out.
	firstResponse, err := generate(t, parent, newToolSet(), userMessage)
	require.NoError(t, err)
	chat := getChat(t, parent)
	require.Equal(t, int32(1), chat.GetUserMessageCount())
	require.Equal(t, int32(1), chat.GetAssistantMessageCount())
	require.Zero(t, chat.GetSystemMessageCount())
	require.Zero(t, chat.GetToolMessageCount())
	require.InDelta(t, firstResponse.GetGeneratedMessage().GetPrice(), chat.GetPrice(), 1e-12)
	require.Len(t, chat.GetModelUsages(), 1)
	require.Equal(t, mockModel, chat.GetModelUsages()[0].GetModel())
	require.InDelta(t, chat.GetPrice(), ai.ModelUsageCost(chat.GetModelUsages()[0]), 1e-12)

	// Turn 2: same model folds into the single entry, price accumulates.
	secondResponse, err := generate(t, parent, newToolSet(), &aipb.Message{
		Role:   aipb.Role_ROLE_USER,
		Blocks: []*aipb.Block{{Content: &aipb.Block_Text{Text: "again"}}},
	})
	require.NoError(t, err)
	chat = getChat(t, parent)
	require.Equal(t, int32(2), chat.GetUserMessageCount())
	require.Equal(t, int32(2), chat.GetAssistantMessageCount())
	// The last user message is turn 2's input: the newest live user message.
	listResponse, err := aiServiceClient.ListMessages(ctx, &aiservicepb.ListMessagesRequest{Parent: parent, Filter: "role = ROLE_USER", OrderBy: "create_time desc"})
	require.NoError(t, err)
	require.Equal(t, listResponse.GetMessages()[0].GetName(), chat.GetLastUserMessage())
	expectedPrice := firstResponse.GetGeneratedMessage().GetPrice() + secondResponse.GetGeneratedMessage().GetPrice()
	require.InDelta(t, expectedPrice, chat.GetPrice(), 1e-12)
	require.Len(t, chat.GetModelUsages(), 1)
	require.InDelta(t, expectedPrice, ai.ModelUsageCost(chat.GetModelUsages()[0]), 1e-12)

	// Deleting a message moves its count; spend is append-only.
	deletedMessage, err := aiServiceClient.DeleteMessage(ctx, &aiservicepb.DeleteMessageRequest{
		Name: secondResponse.GetGeneratedMessage().GetName(),
	})
	require.NoError(t, err)
	chat = getChat(t, parent)
	require.Equal(t, int32(1), chat.GetAssistantMessageCount())
	require.InDelta(t, expectedPrice, chat.GetPrice(), 1e-12)

	// Re-deleting with allow_missing is a no-op on the count.
	_, err = aiServiceClient.DeleteMessage(ctx, &aiservicepb.DeleteMessageRequest{
		Name:         deletedMessage.GetName(),
		AllowMissing: true,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), getChat(t, parent).GetAssistantMessageCount())

	// Undeleting restores it.
	_, err = aiServiceClient.UndeleteMessage(ctx, &aiservicepb.UndeleteMessageRequest{Name: deletedMessage.GetName()})
	require.NoError(t, err)
	require.Equal(t, int32(2), getChat(t, parent).GetAssistantMessageCount())
}

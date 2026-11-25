package sat

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
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

// A chat reads its latest successful generation's message and usage, joined
// from its messages: failed generations and later user messages leave it be.
func TestChatLastGeneratedMessage(t *testing.T) {
	t.Parallel()
	parent := newChatParent()
	userParent := parent[:strings.LastIndex(parent, "/chats/")]

	// Two successful generations, then a third failing on the exhausted script.
	_, err := generate(t, parent, newToolSet(), newScriptedUserMessage(t, newAssistantText("one"), newAssistantText("two")))
	require.NoError(t, err)
	second, err := generate(t, parent, newToolSet(), &aipb.Message{Role: aipb.Role_ROLE_USER, Blocks: []*aipb.Block{{Content: &aipb.Block_Text{Text: "again"}}}})
	require.NoError(t, err)
	_, err = generate(t, parent, newToolSet(), &aipb.Message{Role: aipb.Role_ROLE_USER, Blocks: []*aipb.Block{{Content: &aipb.Block_Text{Text: "fail"}}}})
	require.Error(t, err)
	_, err = aiServiceClient.CreateMessage(ctx, &aiservicepb.CreateMessageRequest{
		Parent:  parent,
		Message: &aipb.Message{Role: aipb.Role_ROLE_USER, Blocks: []*aipb.Block{{Content: &aipb.Block_Text{Text: "later"}}}},
	})
	require.NoError(t, err)
	generated := second.GetGeneratedMessage()
	require.NotZero(t, generated.GetModelUsage().GetInputToken().GetQuantity())

	// A chat whose only generation failed has none.
	failedParent := userParent + "/chats/" + uuid.MustNewV7().String()
	_, err = generate(t, failedParent, newToolSet(), newScriptedUserMessage(t))
	require.Error(t, err)

	requireLastGeneration := func(t *testing.T, chats ...*aipb.Chat) {
		t.Helper()
		byName := map[string]*aipb.Chat{}
		for _, chat := range chats {
			byName[chat.GetName()] = chat
		}
		require.Equal(t, generated.GetName(), byName[parent].GetLastGeneratedMessage())
		grpcrequire.Equal(t, generated.GetModelUsage(), byName[parent].GetLastGeneratedMessageModelUsage())
		require.Empty(t, byName[failedParent].GetLastGeneratedMessage())
		require.Nil(t, byName[failedParent].GetLastGeneratedMessageModelUsage())
	}
	requireLastGeneration(t, getChat(t, parent), getChat(t, failedParent))
	batchGetChatsResponse, err := aiServiceClient.BatchGetChats(ctx, &aiservicepb.BatchGetChatsRequest{Parent: userParent, Names: []string{parent, failedParent}})
	require.NoError(t, err)
	requireLastGeneration(t, batchGetChatsResponse.GetChats()...)
	listChatsResponse, err := aiServiceClient.ListChats(ctx, &aiservicepb.ListChatsRequest{Parent: userParent})
	require.NoError(t, err)
	require.Len(t, listChatsResponse.GetChats(), 2)
	requireLastGeneration(t, listChatsResponse.GetChats()...)
}

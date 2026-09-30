package sat

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/uuid"
)

func createChat(t *testing.T, userParent string, chat *aipb.Chat) *aipb.Chat {
	t.Helper()
	createdChat, err := aiServiceClient.CreateChat(ctx, &aiservicepb.CreateChatRequest{
		Parent:    userParent,
		ChatId:    uuid.MustNewV7().String(),
		RequestId: uuid.MustNewV7().String(),
		Chat:      chat,
	})
	require.NoError(t, err)
	return createdChat
}

func createMessage(t *testing.T, chatName string, message *aipb.Message) *aipb.Message {
	t.Helper()
	createdMessage, err := aiServiceClient.CreateMessage(ctx, &aiservicepb.CreateMessageRequest{
		Parent:    chatName,
		RequestId: uuid.MustNewV7().String(),
		Message:   message,
	})
	require.NoError(t, err)
	return createdMessage
}

func searchChats(t *testing.T, request *aiservicepb.SearchChatsRequest) *aiservicepb.SearchChatsResponse {
	t.Helper()
	response, err := aiServiceClient.SearchChats(ctx, request)
	require.NoError(t, err)
	return response
}

func searchMessages(t *testing.T, request *aiservicepb.SearchMessagesRequest) *aiservicepb.SearchMessagesResponse {
	t.Helper()
	response, err := aiServiceClient.SearchMessages(ctx, request)
	require.NoError(t, err)
	return response
}

func chatNames(chats []*aipb.Chat) []string {
	names := make([]string, len(chats))
	for i, chat := range chats {
		names[i] = chat.GetName()
	}
	return names
}

func messageNames(messages []*aipb.Message) []string {
	names := make([]string, len(messages))
	for i, message := range messages {
		names[i] = message.GetName()
	}
	return names
}

func TestSearchChats(t *testing.T) {
	t.Parallel()
	userParent := newUserParent()
	migrationChat := createChat(t, userParent, &aipb.Chat{Title: "Postgres migration plan"})
	tsvectorChat := createChat(t, userParent, &aipb.Chat{Title: "Postgres tsvector limits"})
	createChat(t, userParent, &aipb.Chat{Title: "Kitchen remodel quote"})

	t.Run("TitlePrefix", func(t *testing.T) {
		response := searchChats(t, &aiservicepb.SearchChatsRequest{Parent: userParent, Query: "postg", IncludeSnippets: true})
		require.ElementsMatch(t, []string{migrationChat.GetName(), tsvectorChat.GetName()}, chatNames(response.GetChats()))
		for _, snippet := range response.GetSnippets() {
			require.Equal(t, "title", snippet.GetMatches()[0].GetPath())
			require.Contains(t, snippet.GetMatches()[0].GetMatch(), "**Postgres**")
		}
	})

	t.Run("FilterComposes", func(t *testing.T) {
		response := searchChats(t, &aiservicepb.SearchChatsRequest{
			Parent: userParent,
			Query:  "postgres",
			Filter: `title = "Postgres migration plan"`,
		})
		require.Equal(t, []string{migrationChat.GetName()}, chatNames(response.GetChats()))
	})

	t.Run("NoMatch", func(t *testing.T) {
		response := searchChats(t, &aiservicepb.SearchChatsRequest{Parent: userParent, Query: "zebra"})
		require.Empty(t, response.GetChats())
	})

	t.Run("TitleUpdatesAreSearchable", func(t *testing.T) {
		chat := createChat(t, userParent, &aipb.Chat{})
		chat.Title = "Pangolin habitat notes"
		_, err := aiServiceClient.UpdateChat(ctx, &aiservicepb.UpdateChatRequest{
			Chat:       chat,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
		})
		require.NoError(t, err)
		response := searchChats(t, &aiservicepb.SearchChatsRequest{Parent: userParent, Query: "pangolin"})
		require.Equal(t, []string{chat.GetName()}, chatNames(response.GetChats()))
	})
}

func TestSearchMessages(t *testing.T) {
	t.Parallel()
	userParent := newUserParent()
	allChats := userParent + "/chats/-"
	firstChat := createChat(t, userParent, &aipb.Chat{})
	secondChat := createChat(t, userParent, &aipb.Chat{})

	firstQuestion := createMessage(t, firstChat.GetName(), ai.NewUserMessage(ai.NewTextBlock("How do I rotate the twilio credentials?")))
	arguments, err := structpb.NewStruct(map[string]any{"query": "flamingo"})
	require.NoError(t, err)
	// Only text blocks are indexed: the thought and tool payloads mention
	// terms that must not match.
	createMessage(t, firstChat.GetName(), ai.NewAssistantMessage(
		ai.NewThoughtBlock("twilio secrets live in the vault"),
		ai.NewTextBlock("Use the vault rotation endpoint."),
		ai.NewToolCallBlock(&aipb.ToolCall{Id: "call-1", Name: "search", Arguments: arguments}),
	))
	createMessage(t, firstChat.GetName(), ai.NewToolMessage(ai.NewToolResultBlock(&aipb.ToolResult{
		ToolName:   "search",
		ToolCallId: "call-1",
		Result:     &aipb.ToolResult_Content{Content: "flamingo results"},
	})))
	secondQuestion := createMessage(t, secondChat.GetName(), ai.NewUserMessage(ai.NewTextBlock("Why do twilio webhooks retry?")))

	t.Run("AcrossChats", func(t *testing.T) {
		response := searchMessages(t, &aiservicepb.SearchMessagesRequest{Parent: allChats, Query: "twilio", IncludeSnippets: true})
		require.ElementsMatch(t, []string{firstQuestion.GetName(), secondQuestion.GetName()}, messageNames(response.GetMessages()))
		for _, snippet := range response.GetSnippets() {
			require.Equal(t, "blocks.text", snippet.GetMatches()[0].GetPath())
			require.Contains(t, snippet.GetMatches()[0].GetMatch(), "**twilio**")
		}
	})

	t.Run("WithinChat", func(t *testing.T) {
		response := searchMessages(t, &aiservicepb.SearchMessagesRequest{Parent: secondChat.GetName(), Query: "twilio"})
		require.Equal(t, []string{secondQuestion.GetName()}, messageNames(response.GetMessages()))
	})

	t.Run("ToolPayloadsAreNotIndexed", func(t *testing.T) {
		response := searchMessages(t, &aiservicepb.SearchMessagesRequest{Parent: allChats, Query: "flamingo"})
		require.Empty(t, response.GetMessages())
	})

	t.Run("FilterComposes", func(t *testing.T) {
		response := searchMessages(t, &aiservicepb.SearchMessagesRequest{Parent: allChats, Query: "vault", Filter: "role = ROLE_USER"})
		require.Empty(t, response.GetMessages())
		response = searchMessages(t, &aiservicepb.SearchMessagesRequest{Parent: allChats, Query: "vault", Filter: "role = ROLE_ASSISTANT"})
		require.Len(t, response.GetMessages(), 1)
	})

	t.Run("EveryTextBlockIsIndexed", func(t *testing.T) {
		message := createMessage(t, secondChat.GetName(), ai.NewUserMessage(ai.NewTextBlock("alpha"), ai.NewTextBlock("omega")))
		response := searchMessages(t, &aiservicepb.SearchMessagesRequest{Parent: allChats, Query: "omega"})
		require.Equal(t, []string{message.GetName()}, messageNames(response.GetMessages()))
	})

	t.Run("OversizedTextIsTruncated", func(t *testing.T) {
		// Beyond the indexed prefix: the write succeeds, the tail is not searchable.
		message := createMessage(t, secondChat.GetName(), ai.NewUserMessage(ai.NewTextBlock(strings.Repeat("filler ", 20000)+"pangolin")))
		response := searchMessages(t, &aiservicepb.SearchMessagesRequest{Parent: allChats, Query: "filler"})
		require.Equal(t, []string{message.GetName()}, messageNames(response.GetMessages()))
		response = searchMessages(t, &aiservicepb.SearchMessagesRequest{Parent: allChats, Query: "pangolin"})
		require.Empty(t, response.GetMessages())
	})
}

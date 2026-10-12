package sat

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	evalpb "github.com/malonaz/core/genproto/ai/eval/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/eval"
)

// Recorded evaluations land on the chat under their own keys only, and the verdict
// label finds the chat.
func TestEvalRecord(t *testing.T) {
	t.Parallel()
	chatRn, err := aipb.ParseChatRn(newChatParent())
	require.NoError(t, err)
	createChatRequest := &aiservicepb.CreateChatRequest{
		Parent: chatRn.UserRn().String(),
		ChatId: chatRn.Chat,
		Chat: &aipb.Chat{
			Labels:      map[string]string{"eval.malonaz.com/polite": "fail", "team": "support"},
			Annotations: map[string]string{"tooling.com/note": "kept"},
		},
	}
	_, err = aiServiceClient.CreateChat(ctx, createChatRequest)
	require.NoError(t, err)

	evaluations := []*evalpb.Evaluation{
		{Eval: "polite", Verdict: evalpb.Verdict_VERDICT_PASS, Rationale: "Greets warmly."},
		{Eval: "on-topic", Verdict: evalpb.Verdict_VERDICT_NOT_APPLICABLE},
	}
	evaluatedChat := &aipb.Chat{Name: chatRn.String()}
	require.NoError(t, eval.Record(evaluatedChat, evaluations...))
	updateChatRequest := &aiservicepb.UpdateChatRequest{
		Chat:       evaluatedChat,
		UpdateMask: eval.UpdateMask(evaluations...).Proto(),
	}
	chat, err := aiServiceClient.UpdateChat(ctx, updateChatRequest)
	require.NoError(t, err)

	require.Equal(t, map[string]string{
		"eval.malonaz.com/polite":   "pass",
		"eval.malonaz.com/on-topic": "not-applicable",
		"team":                      "support",
	}, chat.GetLabels())
	require.Equal(t, "kept", chat.GetAnnotations()["tooling.com/note"])
	evaluation, ok, err := evalpb.Annotations.Eval("polite").Get(chat)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "Greets warmly.", evaluation.GetRationale())

	listChatsRequest := &aiservicepb.ListChatsRequest{
		Parent: chatRn.UserRn().String(),
		Filter: fmt.Sprintf("labels.%q = %q", evalpb.Labels.Eval.GetKey("polite"), evalpb.Labels.Eval.Pass),
	}
	listChatsResponse, err := aiServiceClient.ListChats(ctx, listChatsRequest)
	require.NoError(t, err)
	require.Len(t, listChatsResponse.GetChats(), 1)
	require.Equal(t, chatRn.String(), listChatsResponse.GetChats()[0].GetName())
}

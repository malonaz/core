package eval

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	evalpb "github.com/malonaz/core/genproto/ai/eval/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	_ "github.com/malonaz/core/genproto/test/eval/v1"
	"github.com/malonaz/core/go/ai"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

const (
	chatName      = "organizations/o/users/u/chats/c"
	judgeChatName = "organizations/o/users/u/chats/judge"
	judgeModel    = "providers/mock/models/judge"
)

// fakeAiServiceClient serves a judged chat and scripts the judge's tool call.
type fakeAiServiceClient struct {
	aiservicepb.AiServiceClient
	messages          []*aipb.Message
	verdictArguments  map[string]any
	createChatRequest *aiservicepb.CreateChatRequest
	generateRequest   *aiservicepb.GenerateMessageRequest
	updateRequest     *aiservicepb.UpdateChatRequest
}

func (c *fakeAiServiceClient) ListMessages(ctx context.Context, request *aiservicepb.ListMessagesRequest, _ ...grpc.CallOption) (*aiservicepb.ListMessagesResponse, error) {
	return &aiservicepb.ListMessagesResponse{Messages: c.messages}, nil
}

func (c *fakeAiServiceClient) CreateChat(ctx context.Context, request *aiservicepb.CreateChatRequest, _ ...grpc.CallOption) (*aipb.Chat, error) {
	c.createChatRequest = request
	return &aipb.Chat{Name: judgeChatName, Title: request.GetChat().GetTitle()}, nil
}

func (c *fakeAiServiceClient) GenerateMessage(ctx context.Context, request *aiservicepb.GenerateMessageRequest, _ ...grpc.CallOption) (*aiservicepb.GenerateMessageResponse, error) {
	c.generateRequest = request
	arguments, err := structpb.NewStruct(c.verdictArguments)
	if err != nil {
		return nil, err
	}
	toolCall := &aipb.ToolCall{Id: "1", Name: verdictToolName, Arguments: arguments}
	return &aiservicepb.GenerateMessageResponse{GeneratedMessage: ai.NewAssistantMessage(ai.NewToolCallBlock(toolCall))}, nil
}

func (c *fakeAiServiceClient) UpdateChat(ctx context.Context, request *aiservicepb.UpdateChatRequest, _ ...grpc.CallOption) (*aipb.Chat, error) {
	c.updateRequest = request
	return request.GetChat(), nil
}

func newFakeAiServiceClient() *fakeAiServiceClient {
	return &fakeAiServiceClient{
		messages: []*aipb.Message{
			ai.NewSystemMessage(ai.NewTextBlock("Be polite.")),
			ai.NewUserMessage(ai.NewTextBlock("Hi!")),
			ai.NewAssistantMessage(ai.NewThoughtBlock("secret"), ai.NewTextBlock("Hello, how can I help?")),
		},
		verdictArguments: map[string]any{
			"polite":   map[string]any{"rationale": "Greets warmly.", "verdict": "pass"},
			"on-topic": map[string]any{"rationale": "No request yet.", "verdict": "not-applicable"},
		},
	}
}

func TestMust(t *testing.T) {
	t.Run("Declared", func(t *testing.T) {
		polite := Must("polite")
		require.Equal(t, "The assistant stays polite throughout.", polite.GetCriterion())
	})
	t.Run("Undeclared", func(t *testing.T) {
		require.PanicsWithError(t, `eval "rude" is not declared by any linked proto file`, func() { Must("rude") })
	})
}

func TestEvaluate(t *testing.T) {
	polite, onTopic := Must("polite"), Must("on-topic")

	t.Run("OneVerdictPerEvalInOrder", func(t *testing.T) {
		client := newFakeAiServiceClient()
		evaluations, err := NewJudge(client, judgeModel).Evaluate(context.Background(), chatName, polite, onTopic)
		require.NoError(t, err)
		require.Len(t, evaluations, 2)
		require.Equal(t, "polite", evaluations[0].GetEval())
		require.Equal(t, evalpb.Verdict_VERDICT_PASS, evaluations[0].GetVerdict())
		require.Equal(t, "Greets warmly.", evaluations[0].GetRationale())
		require.Equal(t, judgeModel, evaluations[0].GetModel())
		require.NotNil(t, evaluations[0].GetEvaluateTime())
		require.Equal(t, evalpb.Verdict_VERDICT_NOT_APPLICABLE, evaluations[1].GetVerdict())

		// The judge works in its own chat, beside the judged one and titled after it.
		require.Equal(t, "organizations/o/users/u", client.createChatRequest.GetParent())
		require.Equal(t, chatName, client.createChatRequest.GetChat().GetTitle())
		require.Equal(t, judgeChatName, client.generateRequest.GetParent())
		require.Equal(t, verdictToolName, client.generateRequest.GetConfiguration().GetToolChoice().GetToolName())
		schema := client.generateRequest.GetTools()[0].GetJsonSchema()
		require.ElementsMatch(t, []string{"polite", "on-topic"}, schema.GetRequired())
	})

	t.Run("TranscriptKeepsSystemPromptDropsThoughts", func(t *testing.T) {
		client := newFakeAiServiceClient()
		_, err := NewJudge(client, judgeModel).Evaluate(context.Background(), chatName, polite)
		require.NoError(t, err)
		prompt := client.generateRequest.GetMessages()[1].GetBlocks()[0].GetText()
		require.Contains(t, prompt, "- polite: The assistant stays polite throughout.")
		require.Contains(t, prompt, "SYSTEM:\nBe polite.\n\nUSER:\nHi!\n\nASSISTANT:\nHello, how can I help?")
		require.NotContains(t, prompt, "secret")
	})

	t.Run("NoEval", func(t *testing.T) {
		_, err := NewJudge(newFakeAiServiceClient(), judgeModel).Evaluate(context.Background(), chatName)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("DuplicateEval", func(t *testing.T) {
		_, err := NewJudge(newFakeAiServiceClient(), judgeModel).Evaluate(context.Background(), chatName, polite, polite)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("EmptyChat", func(t *testing.T) {
		client := newFakeAiServiceClient()
		client.messages = nil
		_, err := NewJudge(client, judgeModel).Evaluate(context.Background(), chatName, polite)
		grpcrequire.Error(t, codes.FailedPrecondition, err)
	})

	t.Run("InvalidVerdict", func(t *testing.T) {
		client := newFakeAiServiceClient()
		client.verdictArguments["polite"] = map[string]any{"rationale": "?", "verdict": "maybe"}
		_, err := NewJudge(client, judgeModel).Evaluate(context.Background(), chatName, polite)
		grpcrequire.Error(t, codes.Internal, err)
	})
}

func TestEvaluateChat(t *testing.T) {
	client := newFakeAiServiceClient()
	chat, err := NewJudge(client, judgeModel).EvaluateChat(context.Background(), chatName, Must("polite"), Must("on-topic"))
	require.NoError(t, err)

	// Each verdict is a filterable label; the evaluation, an annotation of the same key.
	require.Equal(t, "pass", chat.GetLabels()["eval.malonaz.com/polite"])
	require.Equal(t, "not-applicable", chat.GetLabels()["eval.malonaz.com/on-topic"])
	evaluation, ok, err := evalpb.Annotations.Eval("polite").Get(chat)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "Greets warmly.", evaluation.GetRationale())

	// Only the recorded keys are written, so other labels and annotations are left alone.
	require.ElementsMatch(t, []string{
		"labels.`eval.malonaz.com/polite`", "annotations.`eval.malonaz.com/polite`",
		"labels.`eval.malonaz.com/on-topic`", "annotations.`eval.malonaz.com/on-topic`",
	}, client.updateRequest.GetUpdateMask().GetPaths())
}

func TestRecord(t *testing.T) {
	t.Run("Overwrites", func(t *testing.T) {
		chat := &aipb.Chat{Labels: map[string]string{"eval.malonaz.com/polite": "fail", "other": "kept"}}
		evaluation := &evalpb.Evaluation{Eval: "polite", Verdict: evalpb.Verdict_VERDICT_PASS}
		require.NoError(t, Record(chat, evaluation))
		require.Equal(t, map[string]string{"eval.malonaz.com/polite": "pass", "other": "kept"}, chat.GetLabels())
	})
	t.Run("UnspecifiedVerdict", func(t *testing.T) {
		require.Error(t, Record(&aipb.Chat{}, &evalpb.Evaluation{Eval: "polite"}))
	})
}

func TestVerdictLabelValues(t *testing.T) {
	// The label declares exactly the verdicts' external values.
	labelValues := []string{evalpb.Labels.Eval.Pass, evalpb.Labels.Eval.Fail, evalpb.Labels.Eval.NotApplicable}
	verdictValues, err := verdictMapping.SliceToExternal(verdicts)
	require.NoError(t, err)
	require.Equal(t, labelValues, verdictValues)
}

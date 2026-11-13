package analysis

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	analysispb "github.com/malonaz/core/genproto/analysis/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider/mock"
	"github.com/malonaz/core/go/ai/analysis/fixtures"
	"github.com/malonaz/core/go/analysis"
)

const (
	parent    = "organizations/o/users/u"
	mockModel = "providers/mock/models/mock-1"
)

var analyzeTime = timestamppb.New(timestamppb.Now().AsTime().Truncate(1e9))

func fixture(t *testing.T, id string) *fixtures.Fixture {
	t.Helper()
	for _, f := range fixtures.All {
		if f.ID == id {
			return f
		}
	}
	t.Fatalf("no fixture %s", id)
	return nil
}

func TestTranscript(t *testing.T) {
	blocks, err := Transcript(fixture(t, "clean-booking").Messages)
	require.NoError(t, err)
	expected := []*aipb.Block{analysis.TextBlock("# Transcript\n\n" +
		"USER:\nCan I book a table for four this Friday at 8pm?\n\n" +
		"ASSISTANT:\nOf course. I've booked a table for four on Friday at 8pm under your name. Anything else I can help with?\n\n" +
		"USER:\nNo, that's all, thank you!\n\n" +
		"ASSISTANT:\nYou're welcome, enjoy your dinner!")}
	require.Empty(t, cmp.Diff(expected, blocks, protocmp.Transform()))
}

// mockAI answers decisions with the mock provider, as the AI service does.
type mockAI struct {
	aiservicepb.AiServiceClient
	count int
}

func (m *mockAI) CreateDecision(ctx context.Context, request *aiservicepb.CreateDecisionRequest, _ ...grpc.CallOption) (*aipb.Decision, error) {
	decision := proto.Clone(request.GetDecision()).(*aipb.Decision)
	answers, _, err := mock.NewClient().CreateDecision(ctx, &aipb.Model{Name: decision.GetModel()}, decision)
	if err != nil {
		return nil, err
	}
	for i, answer := range answers {
		answer.QuestionId = decision.GetQuestions()[i].GetId()
	}
	m.count++
	decision.Name = fmt.Sprintf("%s/decisions/%d", request.GetParent(), m.count)
	decision.Answers = answers
	decision.CreateTime = analyzeTime
	return decision, nil
}

// scriptedJudge answers every question with the fixture's expected value.
func scriptedJudge(client aiservicepb.AiServiceClient, f *fixtures.Fixture) *analysis.Judge {
	return analysis.NewJudge(client, &analysis.Config{Parent: parent, DecisionModel: mockModel}, analysis.WithDecisionHook(func(decision *aipb.Decision) error {
		answers, err := f.Script(decision)
		if err != nil {
			return err
		}
		script, err := mock.Script(answers...)
		if err != nil {
			return err
		}
		decision.Annotations = map[string]string{aipb.Annotations.Script.Key: script}
		return nil
	}))
}

func TestEvaluate(t *testing.T) {
	ctx := context.Background()
	f := fixture(t, "warm-late-delivery")
	judge := scriptedJudge(&mockAI{}, f)
	transcript, err := Transcript(f.Messages)
	require.NoError(t, err)

	result, err := analysis.Evaluate(ctx, judge, Resolution, transcript)
	require.NoError(t, err)
	resolution, err := Resolution.Proto()
	require.NoError(t, err)
	etag, err := analysis.PromptEtag(resolution, mockModel, transcript)
	require.NoError(t, err)
	expected := &analysispb.Result{
		PromptEtag: etag,
		Value: &analysispb.Result_Answer{Answer: &aipb.Answer{
			QuestionId: "resolution",
			Kind:       &aipb.Answer_Score{Score: &aipb.ScoreAnswer{Score: 2, Probabilities: []float64{0.05, 0.05, 0.9}}},
		}},
		Model:       mockModel,
		Source:      parent + "/decisions/1",
		AnalyzeTime: analyzeTime,
	}
	require.Empty(t, cmp.Diff(expected, result, protocmp.Transform()))
}

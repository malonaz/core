package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai/ai_service/provider/mock"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
)

// scriptedAnswers are the mock model's answers to the questions of newDecision.
func scriptedAnswers() []*aipb.Answer {
	return []*aipb.Answer{
		{Kind: &aipb.Answer_Boolean{Boolean: &aipb.BooleanAnswer{Probability: 0.92}}},
		{Kind: &aipb.Answer_Choice{Choice: &aipb.ChoiceAnswer{
			Option:        "billing",
			Probabilities: map[string]float64{"billing": 0.95, "shipping": 0.03, "other": 0.02},
		}}},
		{Kind: &aipb.Answer_Score{Score: &aipb.ScoreAnswer{Score: 1.1, Probabilities: []float64{0.1, 0.7, 0.2}}}},
	}
}

// newDecision returns a decision asking one question of each kind, carrying
// the mock provider script: the answers the mock model gives.
func newDecision(t *testing.T, answers ...*aipb.Answer) *aipb.Decision {
	t.Helper()
	script, err := mock.Script(answers...)
	require.NoError(t, err)
	return &aipb.Decision{
		Model:       mockModel,
		Annotations: map[string]string{aipb.Annotations.Script.Key: script},
		Input:       []*aipb.Block{{Content: &aipb.Block_Text{Text: "I was charged twice for my order."}}},
		Questions: []*aipb.Question{
			{
				Id:           "is_complaint",
				Instructions: "Is the customer complaining?",
				Kind:         &aipb.Question_Boolean{Boolean: &aipb.BooleanQuestion{}},
			},
			{
				Id:           "department",
				Instructions: "Which department should handle this message?",
				Kind: &aipb.Question_Choice{Choice: &aipb.ChoiceQuestion{Options: map[string]string{
					"billing":  "Payments, invoices and refunds.",
					"shipping": "Delivery and tracking.",
					"other":    "",
				}}},
			},
			{
				Id:           "severity",
				Instructions: "How severe is the issue?",
				Kind:         &aipb.Question_Score{Score: &aipb.ScoreQuestion{Levels: []string{"Cosmetic", "Workaround available", "Fully blocked"}}},
			},
		},
	}
}

func TestCreateDecision(t *testing.T) {
	t.Parallel()
	parent := newUserParent()
	decision := newDecision(t, scriptedAnswers()...)
	// Answers are the service's to set: what the client sends is dropped.
	decision.Answers = []*aipb.Answer{{QuestionId: "is_complaint", Kind: &aipb.Answer_Refusal{Refusal: &aipb.Refusal{}}}}
	decision.Price = 42

	createDecisionRequest := &aiservicepb.CreateDecisionRequest{Parent: parent, Decision: decision}
	createdDecision, err := aiServiceClient.CreateDecision(ctx, createDecisionRequest)
	require.NoError(t, err)

	// One answer per question, in question order, each naming its question.
	expectedAnswers := scriptedAnswers()
	for i, question := range decision.GetQuestions() {
		expectedAnswers[i].QuestionId = question.GetId()
	}
	require.Len(t, createdDecision.GetAnswers(), len(expectedAnswers))
	for i, expectedAnswer := range expectedAnswers {
		grpcrequire.Equal(t, expectedAnswer, createdDecision.GetAnswers()[i])
	}

	// The mock model reports 100 input tokens, priced at $1 per million.
	require.Equal(t, mockModel, createdDecision.GetModelUsage().GetModel())
	require.Equal(t, int64(100), createdDecision.GetModelUsage().GetInputToken().GetQuantity())
	require.InDelta(t, 0.0001, createdDecision.GetModelUsage().GetInputToken().GetPrice(), 1e-12)
	require.InDelta(t, 0.0001, createdDecision.GetPrice(), 1e-12)

	// The decision is persisted as returned.
	getDecisionRequest := &aiservicepb.GetDecisionRequest{Name: createdDecision.GetName()}
	gotDecision, err := aiServiceClient.GetDecision(ctx, getDecisionRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, createdDecision, gotDecision)
}

// A request id makes the create idempotent: a retry returns the first
// decision without asking the model again.
func TestCreateDecisionIdempotent(t *testing.T) {
	t.Parallel()
	parent := newUserParent()
	requestID := uuid.MustNewV7().String()

	createDecisionRequest := &aiservicepb.CreateDecisionRequest{
		Parent:    parent,
		Decision:  newDecision(t, scriptedAnswers()...),
		RequestId: requestID,
	}
	decision, err := aiServiceClient.CreateDecision(ctx, createDecisionRequest)
	require.NoError(t, err)

	// The retry carries no scripted answer: the mock model would fail it.
	retryDecisionRequest := &aiservicepb.CreateDecisionRequest{
		Parent:    parent,
		Decision:  newDecision(t),
		RequestId: requestID,
	}
	retriedDecision, err := aiServiceClient.CreateDecision(ctx, retryDecisionRequest)
	require.NoError(t, err)
	grpcrequire.Equal(t, decision, retriedDecision)

	// Without the request id the same request does reach the model.
	retryDecisionRequest.RequestId = ""
	_, err = aiServiceClient.CreateDecision(ctx, retryDecisionRequest)
	grpcrequire.Error(t, codes.Internal, err)

	// A request id is only replayed under its own parent.
	otherParentRequest := &aiservicepb.CreateDecisionRequest{
		Parent:    newUserParent(),
		Decision:  newDecision(t),
		RequestId: requestID,
	}
	_, err = aiServiceClient.CreateDecision(ctx, otherParentRequest)
	grpcrequire.Error(t, codes.Internal, err)

	listDecisionsRequest := &aiservicepb.ListDecisionsRequest{Parent: parent}
	listDecisionsResponse, err := aiServiceClient.ListDecisions(ctx, listDecisionsRequest)
	require.NoError(t, err)
	require.Len(t, listDecisionsResponse.GetDecisions(), 1)
}

func TestCreateDecisionValidation(t *testing.T) {
	t.Parallel()
	parent := newUserParent()

	tests := []struct {
		name   string
		mutate func(*aipb.Decision)
		code   codes.Code
	}{
		{
			name:   "unknown model",
			mutate: func(decision *aipb.Decision) { decision.Model = "providers/mock/models/unknown" },
			code:   codes.NotFound,
		},
		{
			name:   "no input",
			mutate: func(decision *aipb.Decision) { decision.Input = nil },
			code:   codes.InvalidArgument,
		},
		{
			name: "document the model does not accept",
			mutate: func(decision *aipb.Decision) {
				document := &aipb.Document{Source: &aipb.Document_Url{Url: "https://example.com/contract.pdf"}}
				decision.Input = append(decision.Input, &aipb.Block{Content: &aipb.Block_Document{Document: document}})
			},
			code: codes.InvalidArgument,
		},
		{
			name:   "no question",
			mutate: func(decision *aipb.Decision) { decision.Questions = nil },
			code:   codes.InvalidArgument,
		},
		{
			name:   "duplicate question ids",
			mutate: func(decision *aipb.Decision) { decision.Questions[1].Id = decision.Questions[0].Id },
			code:   codes.InvalidArgument,
		},
		{
			name:   "question without a kind",
			mutate: func(decision *aipb.Decision) { decision.Questions[0].Kind = nil },
			code:   codes.InvalidArgument,
		},
		{
			name: "choice with a single option",
			mutate: func(decision *aipb.Decision) {
				decision.Questions[1].GetChoice().Options = map[string]string{"billing": ""}
			},
			code: codes.InvalidArgument,
		},
		{
			name:   "score with a single level",
			mutate: func(decision *aipb.Decision) { decision.Questions[2].GetScore().Levels = []string{"Cosmetic"} },
			code:   codes.InvalidArgument,
		},
		{
			name: "model answering too few questions",
			mutate: func(decision *aipb.Decision) {
				script, err := mock.Script(scriptedAnswers()[:2]...)
				require.NoError(t, err)
				decision.Annotations[aipb.Annotations.Script.Key] = script
			},
			code: codes.Internal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := newDecision(t, scriptedAnswers()...)
			test.mutate(decision)
			createDecisionRequest := &aiservicepb.CreateDecisionRequest{Parent: parent, Decision: decision}
			_, err := aiServiceClient.CreateDecision(ctx, createDecisionRequest)
			grpcrequire.Error(t, test.code, err)
		})
	}

	// Nothing was persisted along the way.
	listDecisionsRequest := &aiservicepb.ListDecisionsRequest{Parent: parent}
	listDecisionsResponse, err := aiServiceClient.ListDecisions(ctx, listDecisionsRequest)
	require.NoError(t, err)
	require.Empty(t, listDecisionsResponse.GetDecisions())
}

// validate_only checks the request without asking the model nor persisting.
func TestCreateDecisionValidateOnly(t *testing.T) {
	t.Parallel()
	createDecisionRequest := &aiservicepb.CreateDecisionRequest{
		Parent:       newUserParent(),
		Decision:     newDecision(t),
		ValidateOnly: true,
	}
	decision, err := aiServiceClient.CreateDecision(ctx, createDecisionRequest)
	require.NoError(t, err)
	require.Empty(t, decision.GetAnswers())

	getDecisionRequest := &aiservicepb.GetDecisionRequest{Name: decision.GetName()}
	_, err = aiServiceClient.GetDecision(ctx, getDecisionRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

// A decision is a record: only its labels and annotations can change.
func TestUpdateDecision(t *testing.T) {
	t.Parallel()
	createDecisionRequest := &aiservicepb.CreateDecisionRequest{Parent: newUserParent(), Decision: newDecision(t, scriptedAnswers()...)}
	decision, err := aiServiceClient.CreateDecision(ctx, createDecisionRequest)
	require.NoError(t, err)

	labeledDecision := proto.CloneOf(decision)
	labeledDecision.Labels = map[string]string{"outcome": "correct"}
	updateDecisionRequest := &aiservicepb.UpdateDecisionRequest{
		Decision:   labeledDecision,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	}
	updatedDecision, err := aiServiceClient.UpdateDecision(ctx, updateDecisionRequest)
	require.NoError(t, err)
	require.Equal(t, "correct", updatedDecision.GetLabels()["outcome"])
	require.Len(t, updatedDecision.GetAnswers(), len(decision.GetAnswers()))

	updateDecisionRequest.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"questions"}}
	_, err = aiServiceClient.UpdateDecision(ctx, updateDecisionRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)
}

func TestComputeStatsDecisions(t *testing.T) {
	t.Parallel()
	userParent := newUserParent()
	for range 2 {
		createDecisionRequest := &aiservicepb.CreateDecisionRequest{Parent: userParent, Decision: newDecision(t, scriptedAnswers()...)}
		_, err := aiServiceClient.CreateDecision(ctx, createDecisionRequest)
		require.NoError(t, err)
	}

	computeStatsRequest := &aiservicepb.ComputeStatsRequest{Name: userParent}
	stats, err := aiServiceClient.ComputeStats(ctx, computeStatsRequest)
	require.NoError(t, err)

	decisionStats := stats.GetTotals().GetDecisions()
	require.Equal(t, int32(2), decisionStats.GetCount())
	require.InDelta(t, 0.0002, decisionStats.GetPrice(), 1e-12)
	require.Len(t, decisionStats.GetModelBreakdowns(), 1)
	breakdown := decisionStats.GetModelBreakdowns()[0]
	require.Equal(t, mockModel, breakdown.GetModel())
	require.Equal(t, int32(2), breakdown.GetCount())
	require.Equal(t, int64(200), breakdown.GetInputToken().GetQuantity())
	require.Nil(t, breakdown.GetOutputToken())
	// Decisions are not messages.
	require.Zero(t, stats.GetTotals().GetMessages().GetCount())
}

package typesafe

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
)

func TestStateToDTO(t *testing.T) {
	state, err := stateToDTO([]*aipb.Block{ai.NewTextBlock("hello")})
	require.NoError(t, err)
	require.Equal(t, "hello", state)

	state, err = stateToDTO([]*aipb.Block{ai.NewTextBlock("hello"), ai.NewTextBlock("world")})
	require.NoError(t, err)
	require.Equal(t, []any{"hello", "world"}, state)

	_, err = stateToDTO([]*aipb.Block{ai.NewImageBlock(ai.NewImageFromURL("https://example.com/a.png"))})
	require.Error(t, err)
}

func TestQuestionToDTOBoolean(t *testing.T) {
	question := &aiservicepb.Question{Type: &aiservicepb.Question_Boolean{Boolean: &aiservicepb.BooleanQuestion{
		CriteriaTrue: structpb.NewStringValue("spam"),
	}}}

	dto, err := questionToDTO(question)
	require.NoError(t, err)
	require.Equal(t, questionDTO{Type: "noul", Criteria: map[string]any{"true": "spam"}}, dto)
}

func TestAnswerDTOToAnswer(t *testing.T) {
	confidence := 0.9
	dto := answerDTO{Type: "choice", Choice: "refund", Probabilities: map[string]float64{"refund": 0.95, "shipping": 0.05}, Confidence: &confidence}

	answer, err := dto.toAnswer()
	require.NoError(t, err)

	expected := &aiservicepb.Answer{
		Type: &aiservicepb.Answer_Choice{Choice: &aiservicepb.ChoiceAnswer{
			Choice:        "refund",
			Probabilities: map[string]float64{"refund": 0.95, "shipping": 0.05},
		}},
		ExtraFields: &structpb.Struct{Fields: map[string]*structpb.Value{"confidence": structpb.NewNumberValue(0.9)}},
	}
	require.Empty(t, cmp.Diff(expected, answer, protocmp.Transform()))
}

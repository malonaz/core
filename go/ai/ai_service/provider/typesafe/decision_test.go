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

func TestQuestionToDTO(t *testing.T) {
	tests := []struct {
		name     string
		question *aiservicepb.Question
		expected questionDTO
	}{
		{
			name: "choice with an empty description",
			question: &aiservicepb.Question{Instructions: "Which topic?", Type: &aiservicepb.Question_Choice{Choice: &aiservicepb.ChoiceQuestion{
				Criteria: map[string]string{"refund": "Money back", "other": ""},
			}}},
			expected: questionDTO{Type: "choice", Instructions: "Which topic?", Criteria: map[string]any{"refund": "Money back", "other": nil}},
		},
		{
			name: "boolean without instructions",
			question: &aiservicepb.Question{Type: &aiservicepb.Question_Boolean{Boolean: &aiservicepb.BooleanQuestion{
				CriteriaTrue: "spam",
			}}},
			expected: questionDTO{Type: "noul", Criteria: map[string]string{"true": "spam"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dto, err := questionToDTO(tt.question)
			require.NoError(t, err)
			require.Equal(t, tt.expected, dto)
		})
	}
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

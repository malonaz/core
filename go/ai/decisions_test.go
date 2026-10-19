package ai

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
)

type choiceOption struct {
	What     string   `json:"what"`
	Examples []string `json:"examples"`
}

func TestNewChoiceQuestion(t *testing.T) {
	question, err := NewChoiceQuestion(
		map[string]any{"question": "Which topic?", "focus": "Ignore greetings."},
		map[string]any{
			"refund": choiceOption{What: "Money back", Examples: []string{"where is my refund?"}},
			"tone":   nil,
		},
	)
	require.NoError(t, err)

	expected := &aiservicepb.Question{Type: &aiservicepb.Question_Choice{Choice: &aiservicepb.ChoiceQuestion{
		Instructions: structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"question": structpb.NewStringValue("Which topic?"),
			"focus":    structpb.NewStringValue("Ignore greetings."),
		}}),
		Criteria: map[string]*structpb.Value{
			"refund": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
				"what":     structpb.NewStringValue("Money back"),
				"examples": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewStringValue("where is my refund?")}}),
			}}),
			"tone": structpb.NewNullValue(),
		},
	}}}
	require.Empty(t, cmp.Diff(expected, question, protocmp.Transform()))
}

func TestNewScoreQuestion(t *testing.T) {
	question, err := NewScoreQuestion("How urgent?", "not urgent", "today")
	require.NoError(t, err)

	expected := &aiservicepb.Question{Type: &aiservicepb.Question_Score{Score: &aiservicepb.ScoreQuestion{
		Instructions: structpb.NewStringValue("How urgent?"),
		Criteria:     []*structpb.Value{structpb.NewStringValue("not urgent"), structpb.NewStringValue("today")},
	}}}
	require.Empty(t, cmp.Diff(expected, question, protocmp.Transform()))
}

func TestNewNoulQuestion(t *testing.T) {
	question, err := NewNoulQuestion("Is this spam?", "Unsolicited promotion", nil)
	require.NoError(t, err)

	expected := &aiservicepb.Question{Type: &aiservicepb.Question_Noul{Noul: &aiservicepb.NoulQuestion{
		Instructions: structpb.NewStringValue("Is this spam?"),
		CriteriaTrue: structpb.NewStringValue("Unsolicited promotion"),
	}}}
	require.Empty(t, cmp.Diff(expected, question, protocmp.Transform()))
}

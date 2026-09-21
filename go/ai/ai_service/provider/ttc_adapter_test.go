package provider

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
)

func TestTtcBuildAnswers(t *testing.T) {
	questions := map[string]*aiservicepb.Question{
		"department": {Type: &aiservicepb.Question_Choice{Choice: &aiservicepb.ChoiceQuestion{
			Instructions: structpb.NewStringValue("which team?"),
			Criteria: map[string]*structpb.Value{
				"billing":   structpb.NewStringValue("payments"),
				"technical": structpb.NewStringValue("bugs"),
			},
		}}},
		"frustration": {Type: &aiservicepb.Question_Score{Score: &aiservicepb.ScoreQuestion{
			Instructions: structpb.NewStringValue("how frustrated?"),
			Criteria: []*structpb.Value{
				structpb.NewStringValue("calm"),
				structpb.NewStringValue("frustrated"),
				structpb.NewStringValue("very angry"),
			},
		}}},
		"is_urgent": {Type: &aiservicepb.Question_Noul{Noul: &aiservicepb.NoulQuestion{
			Instructions: structpb.NewStringValue("is this urgent?"),
		}}},
	}
	arguments := map[string]any{
		"department":  "billing",
		"frustration": float64(1),
		"is_urgent":   true,
	}

	answers, err := ttcBuildAnswers(questions, arguments)
	require.NoError(t, err)

	require.Equal(t, "billing", answers["department"].GetChoice().GetChoice())
	require.Nil(t, answers["department"].GetChoice().Confidence)
	require.Empty(t, answers["department"].GetChoice().GetProbabilities())

	require.Equal(t, float64(1), answers["frustration"].GetScore().GetScore())
	require.Equal(t, "frustrated", answers["frustration"].GetScore().GetLegend()["1"])

	require.Equal(t, float64(1), answers["is_urgent"].GetNoul().GetNoul())
}

func TestTtcBuildAnswersMissingQuestion(t *testing.T) {
	questions := map[string]*aiservicepb.Question{
		"is_urgent": {Type: &aiservicepb.Question_Noul{Noul: &aiservicepb.NoulQuestion{
			Instructions: structpb.NewStringValue("is this urgent?"),
		}}},
	}
	_, err := ttcBuildAnswers(questions, map[string]any{})
	require.Error(t, err)
}

func TestTtcBuildTool(t *testing.T) {
	questions := map[string]*aiservicepb.Question{
		"is_urgent": {Type: &aiservicepb.Question_Noul{Noul: &aiservicepb.NoulQuestion{
			Instructions: structpb.NewStringValue("is this urgent?"),
		}}},
	}
	tool, err := ttcBuildTool(questions)
	require.NoError(t, err)
	require.Equal(t, ttcAnswerToolName, tool.GetName())
	require.Equal(t, "boolean", tool.GetJsonSchema().GetProperties()["is_urgent"].GetType())
	require.Contains(t, tool.GetJsonSchema().GetRequired(), "is_urgent")
}

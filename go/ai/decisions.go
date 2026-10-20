package ai

import (
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
)

// NewChoiceQuestion builds a choice question. criteria maps option name to description.
func NewChoiceQuestion(instructions any, criteria map[string]any) (*aiservicepb.Question, error) {
	instructionsValue, err := structpb.NewValue(instructions)
	if err != nil {
		return nil, err
	}
	criteriaValues := make(map[string]*structpb.Value, len(criteria))
	for name, description := range criteria {
		if criteriaValues[name], err = structpb.NewValue(description); err != nil {
			return nil, err
		}
	}
	return &aiservicepb.Question{Type: &aiservicepb.Question_Choice{Choice: &aiservicepb.ChoiceQuestion{
		Instructions: instructionsValue,
		Criteria:     criteriaValues,
	}}}, nil
}

// NewScoreQuestion builds a score question. levels are ordered lowest to highest.
func NewScoreQuestion(instructions any, levels ...any) (*aiservicepb.Question, error) {
	instructionsValue, err := structpb.NewValue(instructions)
	if err != nil {
		return nil, err
	}
	criteria := make([]*structpb.Value, len(levels))
	for i, level := range levels {
		if criteria[i], err = structpb.NewValue(level); err != nil {
			return nil, err
		}
	}
	return &aiservicepb.Question{Type: &aiservicepb.Question_Score{Score: &aiservicepb.ScoreQuestion{
		Instructions: instructionsValue,
		Criteria:     criteria,
	}}}, nil
}

// NewNoulQuestion builds a yes/no question. Nil criteria are left unset.
func NewNoulQuestion(instructions, criteriaTrue, criteriaFalse any) (*aiservicepb.Question, error) {
	instructionsValue, err := structpb.NewValue(instructions)
	if err != nil {
		return nil, err
	}
	noul := &aiservicepb.NoulQuestion{Instructions: instructionsValue}
	if criteriaTrue != nil {
		if noul.CriteriaTrue, err = structpb.NewValue(criteriaTrue); err != nil {
			return nil, err
		}
	}
	if criteriaFalse != nil {
		if noul.CriteriaFalse, err = structpb.NewValue(criteriaFalse); err != nil {
			return nil, err
		}
	}
	return &aiservicepb.Question{Type: &aiservicepb.Question_Noul{Noul: noul}}, nil
}

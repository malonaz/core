package ai

import (
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
)

// NewChoiceQuestion builds a choice question. criteria maps option key to description. Nil instructions are left unset.
func NewChoiceQuestion(instructions any, criteria map[string]any) (*aiservicepb.Question, error) {
	question, err := newQuestion(instructions)
	if err != nil {
		return nil, err
	}
	choice := &aiservicepb.ChoiceQuestion{Criteria: make(map[string]*structpb.Value, len(criteria))}
	for key, description := range criteria {
		if choice.Criteria[key], err = structpb.NewValue(description); err != nil {
			return nil, err
		}
	}
	question.Type = &aiservicepb.Question_Choice{Choice: choice}
	return question, nil
}

// NewScoreQuestion builds a score question. levels are ordered lowest to highest. Nil instructions are left unset.
func NewScoreQuestion(instructions any, levels ...any) (*aiservicepb.Question, error) {
	question, err := newQuestion(instructions)
	if err != nil {
		return nil, err
	}
	score := &aiservicepb.ScoreQuestion{Criteria: make([]*structpb.Value, len(levels))}
	for i, level := range levels {
		if score.Criteria[i], err = structpb.NewValue(level); err != nil {
			return nil, err
		}
	}
	question.Type = &aiservicepb.Question_Score{Score: score}
	return question, nil
}

// NewBooleanQuestion builds a boolean question. Nil arguments are left unset.
func NewBooleanQuestion(instructions, criteriaTrue, criteriaFalse any) (*aiservicepb.Question, error) {
	question, err := newQuestion(instructions)
	if err != nil {
		return nil, err
	}
	boolean := &aiservicepb.BooleanQuestion{}
	if criteriaTrue != nil {
		if boolean.CriteriaTrue, err = structpb.NewValue(criteriaTrue); err != nil {
			return nil, err
		}
	}
	if criteriaFalse != nil {
		if boolean.CriteriaFalse, err = structpb.NewValue(criteriaFalse); err != nil {
			return nil, err
		}
	}
	question.Type = &aiservicepb.Question_Boolean{Boolean: boolean}
	return question, nil
}

func newQuestion(instructions any) (*aiservicepb.Question, error) {
	if instructions == nil {
		return &aiservicepb.Question{}, nil
	}
	value, err := structpb.NewValue(instructions)
	if err != nil {
		return nil, err
	}
	return &aiservicepb.Question{Instructions: value}, nil
}

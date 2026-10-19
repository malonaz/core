package ai

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
)

// NewJSONValue converts v to a structpb.Value via its JSON encoding, so structs and typed slices work.
func NewJSONValue(v any) (*structpb.Value, error) {
	bytes, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshaling json value: %w", err)
	}
	value := &structpb.Value{}
	if err := protojson.Unmarshal(bytes, value); err != nil {
		return nil, fmt.Errorf("unmarshaling json value: %w", err)
	}
	return value, nil
}

// NewChoiceQuestion builds a choice question. criteria maps option name to description.
func NewChoiceQuestion(instructions any, criteria map[string]any) (*aiservicepb.Question, error) {
	instructionsValue, err := NewJSONValue(instructions)
	if err != nil {
		return nil, err
	}
	criteriaValues := make(map[string]*structpb.Value, len(criteria))
	for name, description := range criteria {
		if criteriaValues[name], err = NewJSONValue(description); err != nil {
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
	instructionsValue, err := NewJSONValue(instructions)
	if err != nil {
		return nil, err
	}
	criteria := make([]*structpb.Value, len(levels))
	for i, level := range levels {
		if criteria[i], err = NewJSONValue(level); err != nil {
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
	instructionsValue, err := NewJSONValue(instructions)
	if err != nil {
		return nil, err
	}
	noul := &aiservicepb.NoulQuestion{Instructions: instructionsValue}
	if criteriaTrue != nil {
		if noul.CriteriaTrue, err = NewJSONValue(criteriaTrue); err != nil {
			return nil, err
		}
	}
	if criteriaFalse != nil {
		if noul.CriteriaFalse, err = NewJSONValue(criteriaFalse); err != nil {
			return nil, err
		}
	}
	return &aiservicepb.Question{Type: &aiservicepb.Question_Noul{Noul: noul}}, nil
}

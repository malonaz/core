package ai_service

import (
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
)

func TestGenerateDecisionRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	require.NoError(t, err)
	boolean := func(instructions, criteriaTrue string) map[string]*pb.Question {
		return map[string]*pb.Question{"q": {
			Instructions: instructions,
			Type:         &pb.Question_Boolean{Boolean: &pb.BooleanQuestion{CriteriaTrue: criteriaTrue}},
		}}
	}
	request := func(questions map[string]*pb.Question, blocks ...*aipb.Block) *pb.GenerateDecisionRequest {
		return &pb.GenerateDecisionRequest{Model: "providers/typesafe/models/jev-1.13", State: blocks, Questions: questions}
	}
	text := ai.NewTextBlock("text")

	require.NoError(t, validator.Validate(request(boolean("", "spam"), text, ai.NewImageBlock(ai.NewImageFromURL("https://example.com/a.png")))))
	require.NoError(t, validator.Validate(request(boolean("Is this spam?", ""), text)))
	require.ErrorContains(t, validator.Validate(request(boolean("", "spam"), ai.NewThoughtBlock("thinking"))), "state blocks must be text, image or document")
	require.ErrorContains(t, validator.Validate(request(boolean("", ""), text)), "a boolean question without instructions must set criteria_true or criteria_false")
}

package ai_service

import (
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
)

func TestValidateGenerateDecisionRequest(t *testing.T) {
	question := func(instructions, criterion *structpb.Value) map[string]*pb.Question {
		return map[string]*pb.Question{"q": {
			Instructions: instructions,
			Type:         &pb.Question_Choice{Choice: &pb.ChoiceQuestion{Criteria: map[string]*structpb.Value{"a": criterion}}},
		}}
	}
	text := structpb.NewStringValue("text")
	state := []*aipb.Block{ai.NewTextBlock("text")}

	tests := []struct {
		name        string
		request     *pb.GenerateDecisionRequest
		expectedErr string
	}{
		{
			name:    "string instructions and null criteria",
			request: &pb.GenerateDecisionRequest{State: state, Questions: question(text, structpb.NewNullValue())},
		},
		{
			name:        "null instructions",
			request:     &pb.GenerateDecisionRequest{State: state, Questions: question(structpb.NewNullValue(), text)},
			expectedErr: "questions[q].instructions must be a string, object or array",
		},
		{
			name:        "bool criteria",
			request:     &pb.GenerateDecisionRequest{State: state, Questions: question(text, structpb.NewBoolValue(true))},
			expectedErr: "questions[q].choice.criteria[a] must be a string, object or array",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateGenerateDecisionRequest(tt.request)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
			require.Equal(t, tt.expectedErr, grpcstatus.Convert(err).Message())
		})
	}
}

func TestGenerateDecisionRequestStateBlocks(t *testing.T) {
	validator, err := protovalidate.New()
	require.NoError(t, err)
	questions := map[string]*pb.Question{"q": {Type: &pb.Question_Boolean{Boolean: &pb.BooleanQuestion{CriteriaTrue: structpb.NewStringValue("spam")}}}}
	request := func(blocks ...*aipb.Block) *pb.GenerateDecisionRequest {
		return &pb.GenerateDecisionRequest{Model: "providers/typesafe/models/jev-1.13", State: blocks, Questions: questions}
	}

	require.NoError(t, validator.Validate(request(ai.NewTextBlock("text"), ai.NewImageBlock(ai.NewImageFromURL("https://example.com/a.png")))))
	require.ErrorContains(t, validator.Validate(request(ai.NewThoughtBlock("thinking"))), "state blocks must be text, image or document")
}

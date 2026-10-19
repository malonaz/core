package ai_service

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
)

func TestValidateGenerateDecisionRequest(t *testing.T) {
	question := func(instructions, criterion *structpb.Value) map[string]*pb.Question {
		return map[string]*pb.Question{"q": {Type: &pb.Question_Choice{Choice: &pb.ChoiceQuestion{
			Instructions: instructions,
			Criteria:     map[string]*structpb.Value{"a": criterion},
		}}}}
	}
	text := structpb.NewStringValue("text")

	tests := []struct {
		name        string
		request     *pb.GenerateDecisionRequest
		expectedErr string
	}{
		{
			name:    "string, object and null criteria",
			request: &pb.GenerateDecisionRequest{State: structpb.NewStructValue(&structpb.Struct{}), Questions: question(text, structpb.NewNullValue())},
		},
		{
			name:        "number state",
			request:     &pb.GenerateDecisionRequest{State: structpb.NewNumberValue(1), Questions: question(text, text)},
			expectedErr: "state must be a string, object or array",
		},
		{
			name:        "null instructions",
			request:     &pb.GenerateDecisionRequest{State: text, Questions: question(structpb.NewNullValue(), text)},
			expectedErr: "questions[q].choice.instructions must be a string, object or array",
		},
		{
			name:        "bool criteria",
			request:     &pb.GenerateDecisionRequest{State: text, Questions: question(text, structpb.NewBoolValue(true))},
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

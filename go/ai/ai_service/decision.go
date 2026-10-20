package ai_service

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// GenerateDecision answers typed questions about a state using a decision
// model. Unlike GenerateMessage, this is stateless: no chat is involved.
func (s *Service) GenerateDecision(ctx context.Context, request *pb.GenerateDecisionRequest) (*pb.GenerateDecisionResponse, error) {
	if err := validateGenerateDecisionRequest(request); err != nil {
		return nil, err
	}
	providerClient, model, err := s.GetGenerateDecisionProvider(ctx, request.GetModel())
	if err != nil {
		return nil, err
	}
	if err := checkModelDeprecation(model); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", err.Error()).Err()
	}

	response, err := providerClient.GenerateDecision(ctx, request)
	if err != nil {
		return nil, err
	}

	setTtdModelUsagePrice(response.GetModelUsage(), model.GetTtd().GetPricing())
	recordModelUsage(response.GetModelUsage())
	return response, nil
}

// setTtdModelUsagePrice prices input tokens against ttd pricing. Output
// tokens are free for decision models and are left unpriced. Nil for
// a TTT model used through the adapter, which has no ttd pricing.
func setTtdModelUsagePrice(usage *aipb.ModelUsage, pricing *aipb.TtdModelPricing) {
	if usage.GetInputToken() == nil || pricing == nil {
		return
	}
	usage.InputToken.Price = (float64(usage.InputToken.Quantity) * pricing.GetInputTokenPricePerMillion()) / 1_000_000
}

// validateGenerateDecisionRequest checks every json field is a string, object or array. Criteria may also be null.
func validateGenerateDecisionRequest(request *pb.GenerateDecisionRequest) error {
	if err := checkDecisionValue("state", request.GetState(), false); err != nil {
		return err
	}
	for id, question := range request.GetQuestions() {
		switch questionType := question.GetType().(type) {
		case *pb.Question_Choice:
			if err := checkDecisionValue(fmt.Sprintf("questions[%s].choice.instructions", id), questionType.Choice.GetInstructions(), false); err != nil {
				return err
			}
			for name, criterion := range questionType.Choice.GetCriteria() {
				if err := checkDecisionValue(fmt.Sprintf("questions[%s].choice.criteria[%s]", id, name), criterion, true); err != nil {
					return err
				}
			}
		case *pb.Question_Score:
			if err := checkDecisionValue(fmt.Sprintf("questions[%s].score.instructions", id), questionType.Score.GetInstructions(), false); err != nil {
				return err
			}
			for i, level := range questionType.Score.GetCriteria() {
				if err := checkDecisionValue(fmt.Sprintf("questions[%s].score.criteria[%d]", id, i), level, false); err != nil {
					return err
				}
			}
		case *pb.Question_Noul:
			if err := checkDecisionValue(fmt.Sprintf("questions[%s].noul.instructions", id), questionType.Noul.GetInstructions(), false); err != nil {
				return err
			}
			if criteria := questionType.Noul.GetCriteriaTrue(); criteria != nil {
				if err := checkDecisionValue(fmt.Sprintf("questions[%s].noul.criteria_true", id), criteria, true); err != nil {
					return err
				}
			}
			if criteria := questionType.Noul.GetCriteriaFalse(); criteria != nil {
				if err := checkDecisionValue(fmt.Sprintf("questions[%s].noul.criteria_false", id), criteria, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func checkDecisionValue(field string, value *structpb.Value, allowNull bool) error {
	switch value.GetKind().(type) {
	case *structpb.Value_StringValue, *structpb.Value_StructValue, *structpb.Value_ListValue:
		return nil
	case *structpb.Value_NullValue:
		if allowNull {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "%s must be a string, object or array", field).Err()
}

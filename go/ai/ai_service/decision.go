package ai_service

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/malonaz/core/gengo/ai/model"
	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc/status"
)

// CreateDecision asks the decision's model its questions, then persists the
// answered decision. Nothing is persisted when the model fails.
func (s *Service) CreateDecision(ctx context.Context, request *pb.CreateDecisionRequest) (*aipb.Decision, error) {
	decision := request.GetDecision()
	// The answers are the service's to set, whatever the client sent.
	decision.Answers, decision.ModelUsage, decision.Price = nil, nil, 0

	// A replayed request gets the decision it already paid for.
	if request.GetRequestId() != "" && !request.GetValidateOnly() {
		replayedDecision, err := s.getReplayedDecision(ctx, request)
		if replayedDecision != nil || err != nil {
			return replayedDecision, err
		}
	}

	providerClient, model, err := s.GetCreateDecisionProvider(ctx, decision.GetModel())
	if err != nil {
		return nil, err
	}
	if err := checkModelDeprecation(model); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", err.Error()).Err()
	}
	for i, block := range decision.GetInput() {
		switch {
		case block.GetImage() != nil && !model.GetDecision().GetImage():
			return nil, status.Errorf(codes.InvalidArgument, "input [%d]: %s does not accept images", i, model.GetName()).Err()
		case block.GetDocument() != nil && !model.GetDecision().GetDocument():
			return nil, status.Errorf(codes.InvalidArgument, "input [%d]: %s does not accept documents", i, model.GetName()).Err()
		}
	}
	if request.GetValidateOnly() {
		return s.AiServiceServer.CreateDecision(ctx, request)
	}

	startTime := time.Now()
	answers, modelUsage, err := providerClient.CreateDecision(ctx, model, decision)
	if err != nil {
		return nil, err
	}
	if len(answers) != len(decision.GetQuestions()) {
		return nil, status.Errorf(codes.Internal, "%s answered %d of %d questions", model.GetName(), len(answers), len(decision.GetQuestions())).Err()
	}
	for i, answer := range answers {
		answer.QuestionId = decision.GetQuestions()[i].GetId()
	}
	modelUsage.Model = model.GetName()
	ai.SetModelUsagePrices(modelUsage, model.GetDecision().GetPricing())
	decision.Answers, decision.ModelUsage, decision.Price = answers, modelUsage, ai.ModelUsageCost(modelUsage)
	recordModelUsage(modelUsage)
	recordGenerationMetrics(model.GetName(), &aipb.GenerationMetrics{Ttlb: durationpb.New(time.Since(startTime))})

	// A retry that raced this request has the same request id: the insert returns the decision that won.
	return s.AiServiceServer.CreateDecision(ctx, request)
}

// getReplayedDecision returns the decision the request's id already created
// under its parent, or nil when the request is new.
func (s *Service) getReplayedDecision(ctx context.Context, request *pb.CreateDecisionRequest) (*aipb.Decision, error) {
	userRn, err := aipb.ParseUserRn(request.GetParent())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	decisionModel, err := s.aiPostgresStore.GetDecisionByRequestID(ctx, userRn.Organization, userRn.User, request.GetRequestId())
	if errors.Is(err, model.ErrDecisionNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, status.FromError(err, "getting decision by request id").Err()
	}
	decision, err := decisionModel.ToPb()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting decision from model to pb: %v", err).Err()
	}
	return decision, nil
}

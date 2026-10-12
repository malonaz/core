package ai_service

import (
	"context"

	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// GetDecision answers typed questions about a state using a decision
// model. Unlike GenerateMessage, this is stateless: no chat is involved.
func (s *Service) GetDecision(ctx context.Context, request *pb.GetDecisionRequest) (*pb.GetDecisionResponse, error) {
	providerClient, model, err := s.GetDecisionProvider(ctx, request.GetModel())
	if err != nil {
		return nil, err
	}
	if err := checkModelDeprecation(model); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", err.Error()).Err()
	}

	response, err := providerClient.GetDecision(ctx, request)
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

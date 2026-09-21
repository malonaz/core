package ai_service

import (
	"context"

	"google.golang.org/grpc/codes"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// Classify answers typed questions about a state using a classification
// model. Unlike GenerateMessage, this is stateless: no chat is involved.
func (s *Service) Classify(ctx context.Context, request *pb.ClassifyRequest) (*pb.ClassifyResponse, error) {
	providerClient, model, err := s.GetClassificationProvider(ctx, request.GetModel())
	if err != nil {
		return nil, err
	}
	if err := checkModelDeprecation(model); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", err.Error()).Err()
	}

	response, err := providerClient.Classify(ctx, request)
	if err != nil {
		return nil, err
	}

	setTtcModelUsagePrice(response.GetModelUsage(), model.GetTtc().GetPricing())
	recordModelUsage(response.GetModelUsage())
	return response, nil
}

// setTtcModelUsagePrice prices input tokens against ttc pricing. Output
// tokens are free for classification models and are left unpriced. Nil for
// a TTT model used through the adapter, which has no ttc pricing.
func setTtcModelUsagePrice(usage *aipb.ModelUsage, pricing *aipb.TtcModelPricing) {
	if usage.GetInputToken() == nil || pricing == nil {
		return
	}
	usage.InputToken.Price = (float64(usage.InputToken.Quantity) * pricing.GetInputTokenPricePerMillion()) / 1_000_000
}

package agent_service

import (
	"context"

	"google.golang.org/grpc/codes"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// CreateInput queues an input on an agent, forwarded to its executor if leased.
func (s *Service) CreateInput(ctx context.Context, request *agentservicepb.CreateInputRequest) (*agentpb.Input, error) {
	requestID := request.RequestId
	if requestID == "" {
		requestID = uuid.MustNewV7().String()
	}
	var input *agentpb.Input
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		agent, err := t.getAgent(ctx, request.Parent)
		if err != nil {
			return err
		}
		if agent.DeleteTime != nil {
			return status.Errorf(codes.NotFound, "agent %s does not exist", agent.Name).Err()
		}
		input, err = t.queueInput(ctx, agent, requestID, request.Input.Message)
		return err
	}); err != nil {
		return nil, err
	}
	return input, nil
}

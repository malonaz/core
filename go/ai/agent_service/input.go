package agent_service

import (
	"context"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/uuid"
)

// CreateInput queues an input on an agent, starting its turn on this replica if it is idle.
func (s *Service) CreateInput(ctx context.Context, request *agentservicepb.CreateInputRequest) (*agentpb.Input, error) {
	requestID := request.RequestId
	if requestID == "" {
		requestID = uuid.MustNewV7().String()
	}
	var input *agentpb.Input
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		agent, err := t.getLiveAgent(ctx, request.Parent)
		if err != nil {
			return err
		}
		input, err = t.queueInput(ctx, agent, requestID, request.Input.Message)
		return err
	}); err != nil {
		return nil, err
	}
	return input, nil
}

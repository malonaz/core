package agent_service

import (
	"context"

	"google.golang.org/grpc/codes"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// CancelAgent cancels the agent's running turn wherever it runs: here, on the replica holding its lease, or, if no
// replica holds it any more, by ending the turn CANCELLED directly. The turn has ended when it returns.
func (s *Service) CancelAgent(ctx context.Context, request *agentservicepb.CancelAgentRequest) (*agentpb.Agent, error) {
	agentRn, err := agentpb.ParseAgentRn(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
	getAgentRequest := &agentservicepb.GetAgentRequest{Name: request.Name}
	if done, ok := s.turns.cancel(request.Name, errTurnCancelled); ok {
		select {
		case <-done:
		case <-ctx.Done():
			return nil, status.FromError(ctx.Err(), "waiting for the turn to end").Err()
		}
		return s.GetAgent(ctx, getAgentRequest)
	}
	if isForwarded(ctx) {
		// The turn ended before the forwarded call arrived.
		return s.GetAgent(ctx, getAgentRequest)
	}

	agent, err := s.GetAgent(ctx, getAgentRequest)
	if err != nil {
		return nil, err
	}
	if agent.DeleteTime != nil {
		return nil, status.Errorf(codes.NotFound, "agent %s does not exist", agent.Name).Err()
	}
	if agent.State != agentpb.Agent_STATE_RUNNING {
		return agent, nil
	}
	lease, err := s.agentPostgresStore.GetLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
	if err != nil {
		return nil, status.FromError(err, "getting lease").Err()
	}
	if lease != nil && !lease.Expired {
		if lease.Replica == s.replica || lease.Address == s.address {
			// Claimed here an instant ago, or by this replica's previous process whose lease is still to expire.
			return nil, status.Errorf(codes.Unavailable, "the turn of agent %s is changing replicas: retry", agent.Name).Err()
		}
		peerClient, err := s.peerClient(ctx, lease.Address)
		if err != nil {
			return nil, status.FromError(err, "connecting to the replica running the turn").Err()
		}
		if _, err := peerClient.CancelAgent(forwardedContext(ctx), request); err != nil {
			return nil, status.FromError(err, "cancelling the turn on the replica running it").Err()
		}
		return s.GetAgent(ctx, getAgentRequest)
	}

	// No replica runs the turn: end it here, unless a replica claimed it meanwhile.
	var claimedMeanwhile bool
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		if agent, err = t.getLiveAgent(ctx, request.Name); err != nil {
			return err
		}
		if agent.State != agentpb.Agent_STATE_RUNNING {
			return nil
		}
		lease, err := t.GetLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
		if err != nil {
			return status.FromError(err, "getting lease").Err()
		}
		if claimedMeanwhile = lease != nil && !lease.Expired; claimedMeanwhile {
			return nil
		}
		if err := t.cancelTurn(ctx, agent); err != nil {
			return err
		}
		return t.saveAgent(ctx, agent)
	}); err != nil {
		return nil, err
	}
	if claimedMeanwhile {
		return s.CancelAgent(ctx, request)
	}
	return agent, nil
}

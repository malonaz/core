package agent_service

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/uuid"
)

// cancelRetryInterval paces the retries of a cancellation waiting for a turn to settle on a replica.
const cancelRetryInterval = 50 * time.Millisecond

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
	if agent.State == agentpb.Agent_STATE_IDLE {
		return agent, nil
	}
	lease, err := s.agentPostgresStore.GetLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
	if err != nil {
		return nil, status.FromError(err, "getting lease").Err()
	}
	if lease != nil && !lease.Expired {
		if lease.Replica == s.replica || lease.Address == s.address {
			// Claimed here an instant ago, or by this replica's previous process whose lease is still to expire: the
			// turn settles shortly.
			select {
			case <-time.After(cancelRetryInterval):
			case <-ctx.Done():
				return nil, status.FromError(ctx.Err(), "waiting for the turn of agent %s to settle", agent.Name).Err()
			}
			return s.CancelAgent(ctx, request)
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

	// No replica runs the turn, awaiting or interrupted: end it here, unless a replica claimed it meanwhile.
	var claimedMeanwhile, cancelled bool
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		if agent, err = t.getLiveAgent(ctx, request.Name); err != nil {
			return err
		}
		if agent.State == agentpb.Agent_STATE_IDLE {
			return nil
		}
		lease, err := t.GetLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
		if err != nil {
			return fmt.Errorf("getting lease: %w", err)
		}
		if claimedMeanwhile = lease != nil && !lease.Expired; claimedMeanwhile {
			return nil
		}
		if err := t.cancelTurn(ctx, agent); err != nil {
			return err
		}
		cancelled = true
		return t.saveAgent(ctx, agent)
	}); err != nil {
		return nil, err
	}
	if claimedMeanwhile {
		return s.CancelAgent(ctx, request)
	}
	if cancelled {
		// No turn ends to report it: report the cancellation, under an id the turn's start fixes.
		turnEnd := &turnEnd{
			outcome:         agentpb.Turn_OUTCOME_CANCELLED,
			report:          "My turn was cancelled.",
			reportRequestID: uuid.NewV5(reportNamespace, agentpb.Turn_OUTCOME_CANCELLED.String()+"/"+agent.Name+"/"+agent.GetLatestTurn().GetStartTime().AsTime().String()).String(),
		}
		if err := s.report(ctx, agent.Name, turnEnd); err != nil {
			return nil, err
		}
	}
	return agent, nil
}

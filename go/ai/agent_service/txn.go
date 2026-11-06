package agent_service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/gengo/ai/agent/model"
	"github.com/malonaz/core/gengo/ai/agent/store"
	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
)

// txn is one Postgres transaction over agents. The snapshots it saves are published, and the turns it claims
// started, only once it commits. Locks are taken a child agent before its parent.
type txn struct {
	*store.Tx
	service *Service
	now     time.Time
	// The agents saved, in order, and the agents whose turns this replica claimed.
	snapshots    []*agentpb.Agent
	claimedTurns []string
}

// transact runs fn in a transaction, retried from scratch on deadlocks, then publishes what it saved and starts
// the turns it claimed. Store errors must reach it wrapped, never converted to a status, for their retries.
func (s *Service) transact(ctx context.Context, fn func(ctx context.Context, t *txn) error) error {
	var t *txn
	if err := s.agentPostgresStore.Transaction(ctx, func(tx *store.Tx) error {
		// Postgres keeps microseconds: what a transaction returns must equal what it stored.
		t = &txn{Tx: tx, service: s, now: time.Now().UTC().Truncate(time.Microsecond)}
		return fn(ctx, t)
	}); err != nil {
		if _, ok := grpcstatus.FromError(err); ok {
			return err
		}
		return status.FromError(err, "running transaction").Err()
	}
	for _, snapshot := range t.snapshots {
		s.publishSnapshot(snapshot)
	}
	for _, agent := range t.claimedTurns {
		s.startTurn(agent)
	}
	return nil
}

// getAgent reads an agent, tombstones included, and locks it until the transaction ends.
func (t *txn) getAgent(ctx context.Context, name string) (*agentpb.Agent, error) {
	agentRn, err := agentpb.ParseAgentRn(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing agent name: %v", err).Err()
	}
	agentModel, err := t.GetAgentForUpdate(ctx, agentRn.Organization, agentRn.User, agentRn.Agent)
	if err != nil {
		if errors.Is(err, model.ErrAgentNotExist) {
			return nil, status.Errorf(codes.NotFound, "agent %s does not exist", name).Err()
		}
		return nil, fmt.Errorf("getting agent: %w", err)
	}
	agent, err := agentModel.ToPb()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting agent from model to pb: %v", err).Err()
	}
	return agent, nil
}

// getLiveAgent reads and locks an agent that is not deleted.
func (t *txn) getLiveAgent(ctx context.Context, name string) (*agentpb.Agent, error) {
	agent, err := t.getAgent(ctx, name)
	if err != nil {
		return nil, err
	}
	if agent.DeleteTime != nil {
		return nil, status.Errorf(codes.NotFound, "agent %s does not exist", name).Err()
	}
	return agent, nil
}

// saveAgent persists an agent locked by getAgent, stamping its update time and etag.
func (t *txn) saveAgent(ctx context.Context, agent *agentpb.Agent) error {
	agent.UpdateTime = timestamppb.New(t.now)
	var err error
	if agent.Etag, err = aip.ComputeETag(agent); err != nil {
		return status.Errorf(codes.Internal, "computing etag: %v", err).Err()
	}
	agentModel, err := model.AgentFromPb(agent)
	if err != nil {
		return status.Errorf(codes.Internal, "converting agent from pb to model: %v", err).Err()
	}
	if err := t.SaveAgent(ctx, agentModel); err != nil {
		return fmt.Errorf("saving agent: %w", err)
	}
	t.snapshots = append(t.snapshots, proto.CloneOf(agent))
	return nil
}

// claimTurn starts the turn of an agent locked by getAgent: it marks the agent RUNNING and, if this replica has room
// and no other holds the agent's lease, claims the lease here, the turn starting once the transaction commits. A
// RUNNING agent without a lease is left to the claim scans.
func (t *txn) claimTurn(ctx context.Context, agent *agentpb.Agent) error {
	if agent.State == agentpb.Agent_STATE_IDLE {
		// A new turn, carrying the results of the calls its predecessor left awaited for it to write.
		agent.LatestTurn = &agentpb.Turn{StartTime: timestamppb.New(t.now), AwaitedToolCalls: agent.GetLatestTurn().GetAwaitedToolCalls()}
	}
	if agent.State != agentpb.Agent_STATE_RUNNING {
		agent.State = agentpb.Agent_STATE_RUNNING
		if err := t.saveAgent(ctx, agent); err != nil {
			return err
		}
	}
	if !t.service.turns.hasRoom(t.service.opts.MaxConcurrentTurns) {
		return nil
	}
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	if err != nil {
		return status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	claimed, err := t.ClaimLease(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, t.service.replica, t.service.address, t.service.opts.LeaseDuration)
	if err != nil {
		return fmt.Errorf("claiming lease: %w", err)
	}
	if claimed {
		t.claimedTurns = append(t.claimedTurns, agent.Name)
	}
	return nil
}

// publishSnapshot publishes an agent's state to the streams of this replica.
func (s *Service) publishSnapshot(agent *agentpb.Agent) {
	s.hub.publish(&agentservicepb.StreamAgentResponse{
		Agent:   agent.Name,
		Content: &agentservicepb.StreamAgentResponse_Snapshot{Snapshot: agent},
	})
}

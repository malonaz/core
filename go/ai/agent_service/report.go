package agent_service

import (
	"context"

	"google.golang.org/grpc/codes"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// handleReport records an event an executor reports for one of its agents.
func (s *executorSession) handleReport(ctx context.Context, report *agentservicepb.ConnectExecutorRequest_Report) error {
	switch report.Event.Kind.(type) {
	case *agentpb.Event_TurnStarted_, *agentpb.Event_MessageCreated_, *agentpb.Event_TurnEnded_:
	default:
		return status.Errorf(codes.InvalidArgument, "executors report turn_started, message_created and turn_ended only").Err()
	}
	return s.service.transact(ctx, func(ctx context.Context, t *txn) error {
		agent, held, err := s.getHeldAgent(ctx, t, report.Agent)
		if err != nil || !held {
			return err
		}
		switch kind := report.Event.Kind.(type) {
		case *agentpb.Event_TurnStarted_:
			events := []*agentpb.Event{{Kind: kind}}
			if agent.State != agentpb.Agent_STATE_RUNNING {
				agent.State = agentpb.Agent_STATE_RUNNING
				events = append(events, newStateChangedEvent(agent.State))
			}
			return t.saveAgent(ctx, agent, events...)

		case *agentpb.Event_MessageCreated_:
			if err := t.consumeInputs(ctx, agent, kind.MessageCreated.Inputs); err != nil {
				return err
			}
			return t.saveAgent(ctx, agent, &agentpb.Event{Kind: kind})

		case *agentpb.Event_TurnEnded_:
			return t.endTurn(ctx, agent, kind.TurnEnded.Outcome, kind.TurnEnded.Status)
		}
		return nil
	})
}

// handleRelease returns an agent the executor cannot serve to pending, and keeps placement off this executor for
// it until the refusal cooldown passes.
func (s *executorSession) handleRelease(ctx context.Context, release *agentservicepb.ConnectExecutorRequest_Release) error {
	var released bool
	if err := s.service.transact(ctx, func(ctx context.Context, t *txn) error {
		agent, held, err := s.getHeldAgent(ctx, t, release.Agent)
		if err != nil || !held {
			return err
		}
		expireTime := t.now.Add(s.service.opts.RefusalCooldown)
		if err := t.InsertRefusal(ctx, agent.Name, s.executorRn.Organization, s.executorRn.User, s.executorRn.Executor, expireTime); err != nil {
			return status.FromError(err, "inserting refusal").Err()
		}
		released = true
		return t.releaseAgent(ctx, agent)
	}); err != nil || !released {
		return err
	}
	s.forget(ctx, release.Agent)
	if err := s.service.placeAgent(ctx, release.Agent); err != nil {
		s.service.log.WarnContext(ctx, "placing released agent", "agent", release.Agent, "error", err)
	}
	return nil
}

// getHeldAgent locks an agent and reports whether this session holds its lease. A superseded session is ABORTED;
// an agent leased elsewhere (e.g. a stale report after a delete) is not held.
func (s *executorSession) getHeldAgent(ctx context.Context, t *txn, name string) (*agentpb.Agent, bool, error) {
	agent, err := t.getAgent(ctx, name)
	if err != nil {
		if status.HasCode(err, codes.NotFound) {
			s.service.log.WarnContext(ctx, "dropping message for a missing agent", "executor", s.executor, "agent", name)
			return nil, false, nil
		}
		return nil, false, err
	}
	// Agent locked first, executor last.
	session, err := t.GetExecutorSession(ctx, s.executorRn.Organization, s.executorRn.User, s.executorRn.Executor)
	if err != nil {
		return nil, false, status.FromError(err, "getting executor session").Err()
	}
	if session != s.id {
		return nil, false, newSupersededError("superseded")
	}
	if agent.DeleteTime != nil || agent.LeasedExecutor != s.executor {
		s.service.log.WarnContext(ctx, "dropping message for an agent not leased to the executor", "executor", s.executor, "agent", name)
		return nil, false, nil
	}
	return agent, true, nil
}

// consumeInputs marks the inputs a message delivered consumed. They must be the agent's.
func (t *txn) consumeInputs(ctx context.Context, agent *agentpb.Agent, inputs []string) error {
	if len(inputs) == 0 {
		return nil
	}
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	if err != nil {
		return status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	inputIDs := make([]string, 0, len(inputs))
	for _, input := range inputs {
		inputRn, err := agentpb.ParseInputRn(input)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "parsing input name: %v", err).Err()
		}
		if inputRn.AgentRn().String() != agent.Name {
			return status.Errorf(codes.InvalidArgument, "input %s is not agent %s's", input, agent.Name).Err()
		}
		inputIDs = append(inputIDs, inputRn.Input)
	}
	if err := t.ConsumeInputs(ctx, agentRn.Organization, agentRn.User, agentRn.Agent, inputIDs, t.now); err != nil {
		return status.FromError(err, "consuming inputs").Err()
	}
	return nil
}

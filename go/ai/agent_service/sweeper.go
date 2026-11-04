package agent_service

import (
	"context"
	"time"

	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// orphanedAgentsBatchSize bounds how many orphaned agents one sweep releases.
const orphanedAgentsBatchSize = 1000

// startSweeper enforces heartbeat deadlines and places pending agents every sweep interval, until stopped.
func (s *Service) startSweeper(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(s.opts.SweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sweep(ctx)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// sweep takes executors silent past their deadline offline, releases the agents leased to offline executors, and
// places pending agents. Errors are logged: the next sweep retries.
func (s *Service) sweep(ctx context.Context) {
	now := time.Now().UTC()
	executorModels, err := s.agentPostgresStore.ExpireExecutors(ctx, now.Add(-s.opts.HeartbeatTimeout), now)
	if err != nil {
		s.log.ErrorContext(ctx, "expiring executors", "error", err)
	}
	for _, executorModel := range executorModels {
		s.log.InfoContext(ctx, "executor missed its heartbeat deadline", "organization", executorModel.OrganizationID,
			"user", executorModel.UserID, "executor", executorModel.ExecutorID)
	}

	agentModels, err := s.agentPostgresStore.ListOrphanedAgents(ctx, orphanedAgentsBatchSize)
	if err != nil {
		s.log.ErrorContext(ctx, "listing orphaned agents", "error", err)
	}
	for _, agentModel := range agentModels {
		agentRn := &agentpb.AgentRn{Organization: agentModel.OrganizationID, User: agentModel.UserID, Agent: agentModel.AgentID}
		if err := s.releaseAgentFromExecutor(ctx, agentRn.String(), agentModel.LeasedExecutor); err != nil {
			s.log.ErrorContext(ctx, "releasing orphaned agent", "agent", agentRn.String(), "error", err)
		}
	}
	s.placePendingAgents(ctx, "", "")
}

// releaseAgentFromExecutor releases a live agent if it is still leased to the executor.
func (s *Service) releaseAgentFromExecutor(ctx context.Context, name, executor string) error {
	if err := s.transact(ctx, func(ctx context.Context, t *txn) error {
		agent, err := t.getAgent(ctx, name)
		if err != nil {
			return err
		}
		if agent.DeleteTime != nil || agent.LeasedExecutor != executor {
			return nil
		}
		return t.releaseAgent(ctx, agent)
	}); err != nil {
		return status.FromError(err, "releasing agent").Err()
	}
	return nil
}

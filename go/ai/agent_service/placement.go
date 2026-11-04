package agent_service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"

	"github.com/malonaz/core/gengo/ai/agent/model"
	"github.com/malonaz/core/gengo/ai/agent/store"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/grpc/status"
)

// pendingAgentsBatchSize bounds how many pending agents one placement pass tries.
const pendingAgentsBatchSize = 1000

// placeAgent leases a live, pending and unleased agent to an executor of its user, if one can take it: its pinned
// executor only if pinned, else its parent's executor, else the least loaded match. Otherwise it stays pending.
func (s *Service) placeAgent(ctx context.Context, name string) error {
	return s.transact(ctx, func(ctx context.Context, t *txn) error {
		agent, err := t.getAgent(ctx, name)
		if err != nil {
			return err
		}
		if agent.DeleteTime != nil || agent.State != agentpb.Agent_STATE_PENDING || agent.LeasedExecutor != "" {
			return nil
		}
		executor, err := t.findExecutor(ctx, agent)
		if err != nil || executor == "" {
			return err
		}
		return t.leaseAgent(ctx, agent, executor)
	})
}

// findExecutor returns the executor an agent locked by getAgent should be leased to, locked, or "" if none can
// take it. Only executors of the agent's own user are ever considered.
func (t *txn) findExecutor(ctx context.Context, agent *agentpb.Agent) (string, error) {
	agentRn, err := agentpb.ParseAgentRn(agent.Name)
	if err != nil {
		return "", status.Errorf(codes.Internal, "parsing agent name: %v", err).Err()
	}
	executorQuery := &store.ExecutorQuery{
		OrganizationID: agentRn.Organization,
		UserID:         agentRn.User,
		Tools:          agent.GetSpec().GetTools(),
		Now:            t.now,
	}
	// An explicit pin overrides the selector and refusals, and never falls through.
	if agent.Executor != "" {
		executorID, ok := userExecutorID(agentRn, agent.Executor)
		if !ok {
			return "", nil
		}
		executorQuery.ExecutorID = executorID
		return t.findExecutorName(ctx, executorQuery)
	}

	executorQuery.Selector = agent.GetSpec().GetExecutorSelector()
	executorQuery.RefusingAgent = agent.Name
	if agent.ParentAgent != "" {
		parentExecutorID, err := t.getParentExecutorID(ctx, agentRn, agent.ParentAgent)
		if err != nil {
			return "", err
		}
		if parentExecutorID != "" {
			parentExecutorQuery := *executorQuery
			parentExecutorQuery.ExecutorID = parentExecutorID
			executor, err := t.findExecutorName(ctx, &parentExecutorQuery)
			if err != nil || executor != "" {
				return executor, err
			}
		}
	}
	return t.findExecutorName(ctx, executorQuery)
}

// getParentExecutorID returns the id of the executor leasing an agent's parent, if it is one of the agent's user's.
// The parent is read unlocked: a stale read only misses the preference.
func (t *txn) getParentExecutorID(ctx context.Context, agentRn *agentpb.AgentRn, parentAgent string) (string, error) {
	parentAgentRn, err := agentpb.ParseAgentRn(parentAgent)
	if err != nil {
		return "", status.Errorf(codes.Internal, "parsing parent agent name: %v", err).Err()
	}
	parentAgentModel, err := t.service.agentPostgresStore.GetAgent(ctx, parentAgentRn.Organization, parentAgentRn.User, parentAgentRn.Agent)
	if err != nil {
		if errors.Is(err, model.ErrAgentNotExist) {
			return "", nil
		}
		return "", status.FromError(err, "getting parent agent").Err()
	}
	if parentAgentModel.LeasedExecutor == "" {
		return "", nil
	}
	executorID, _ := userExecutorID(agentRn, parentAgentModel.LeasedExecutor)
	return executorID, nil
}

func (t *txn) findExecutorName(ctx context.Context, executorQuery *store.ExecutorQuery) (string, error) {
	executorModel, err := t.FindExecutor(ctx, executorQuery)
	if err != nil {
		return "", status.FromError(err, "finding executor").Err()
	}
	if executorModel == nil {
		return "", nil
	}
	executorRn := &agentpb.ExecutorRn{Organization: executorModel.OrganizationID, User: executorModel.UserID, Executor: executorModel.ExecutorID}
	return executorRn.String(), nil
}

// userExecutorID returns the id of an executor if it belongs to the agent's user: agents never run on another's.
func userExecutorID(agentRn *agentpb.AgentRn, executor string) (string, bool) {
	executorRn, err := agentpb.ParseExecutorRn(executor)
	if err != nil || executorRn.Organization != agentRn.Organization || executorRn.User != agentRn.User {
		return "", false
	}
	return executorRn.Executor, true
}

// placePendingAgents tries to place the oldest pending agents of a user, or of every user of an organization (or
// of all of them) when left empty. Best effort: errors are logged.
func (s *Service) placePendingAgents(ctx context.Context, organizationID, userID string) {
	agentModels, err := s.agentPostgresStore.ListPendingAgents(ctx, organizationID, userID, pendingAgentsBatchSize)
	if err != nil {
		s.log.ErrorContext(ctx, "listing pending agents", "organization", organizationID, "user", userID, "error", err)
		return
	}
	for _, agentModel := range agentModels {
		agentRn := &agentpb.AgentRn{Organization: agentModel.OrganizationID, User: agentModel.UserID, Agent: agentModel.AgentID}
		if err := s.placeAgent(ctx, agentRn.String()); err != nil {
			s.log.ErrorContext(ctx, "placing agent", "agent", agentRn.String(), "error", err)
		}
	}
}

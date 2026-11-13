package agent_service

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentpb "github.com/malonaz/core/genproto/agent/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/postgres"
	"github.com/malonaz/core/gengo/agent/model"
)

// claimTask atomically claims the oldest PENDING task: the table is the work
// queue, SKIP LOCKED gives competing consumers for free.
func (s *Service) claimTask(ctx context.Context) (*agentpb.Task, error) {
	var task *agentpb.Task
	err := s.agentPostgresClient.ExecuteTransaction(ctx, pgx.ReadCommitted, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			SELECT organization_id, user_id, agent_id, task_id FROM agent.task
			WHERE state = $1 AND delete_time IS NULL
			ORDER BY create_time LIMIT 1
			FOR UPDATE SKIP LOCKED`, int16(agentpb.TaskState_TASK_STATE_PENDING))
		var organizationID, userID, agentID, taskID string
		if err := row.Scan(&organizationID, &userID, &agentID, &taskID); err != nil {
			if err == pgx.ErrNoRows {
				return nil
			}
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE agent.task SET state = $1, update_time = $2
			WHERE organization_id = $3 AND user_id = $4 AND agent_id = $5 AND task_id = $6`,
			int16(agentpb.TaskState_TASK_STATE_RUNNING), time.Now().UTC(), organizationID, userID, agentID, taskID); err != nil {
			return err
		}
		taskModel, err := s.agentPostgresStore.GetTask(ctx, organizationID, userID, agentID, taskID)
		if err != nil {
			return err
		}
		task, err = taskModel.ToPb()
		if err != nil {
			return err
		}
		task.State = agentpb.TaskState_TASK_STATE_RUNNING
		return nil
	})
	return task, err
}

// claimAgent atomically claims the ACTIVE agent with the oldest wake,
// flipping it to PROCESSING.
func (s *Service) claimAgent(ctx context.Context) (*agentpb.Agent, error) {
	var agent *agentpb.Agent
	err := s.agentPostgresClient.ExecuteTransaction(ctx, pgx.ReadCommitted, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			SELECT organization_id, user_id, agent_id FROM agent.agent
			WHERE state = $1 AND delete_time IS NULL
			ORDER BY update_time LIMIT 1
			FOR UPDATE SKIP LOCKED`, int16(agentpb.AgentState_AGENT_STATE_ACTIVE))
		var organizationID, userID, agentID string
		if err := row.Scan(&organizationID, &userID, &agentID); err != nil {
			if err == pgx.ErrNoRows {
				return nil
			}
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE agent.agent SET state = $1, update_time = $2
			WHERE organization_id = $3 AND user_id = $4 AND agent_id = $5`,
			int16(agentpb.AgentState_AGENT_STATE_PROCESSING), time.Now().UTC(), organizationID, userID, agentID); err != nil {
			return err
		}
		agentModel, err := s.agentPostgresStore.GetAgent(ctx, organizationID, userID, agentID)
		if err != nil {
			return err
		}
		agent, err = agentModel.ToPb()
		if err != nil {
			return err
		}
		agent.State = agentpb.AgentState_AGENT_STATE_PROCESSING
		return nil
	})
	return agent, err
}

// reap re-queues claimed rows whose heartbeat went stale (crashed runner):
// crash recovery is free because every park point is persisted.
func (s *Service) reap(ctx context.Context) bool {
	staleBefore := time.Now().UTC().Add(-s.opts.LeaseTimeout)
	if _, err := s.agentPostgresClient.Exec(ctx, `
		UPDATE agent.task SET state = $1, update_time = $2
		WHERE state = $3 AND update_time < $4 AND delete_time IS NULL`,
		int16(agentpb.TaskState_TASK_STATE_PENDING), time.Now().UTC(),
		int16(agentpb.TaskState_TASK_STATE_RUNNING), staleBefore); err != nil {
		s.log.Error("reaping tasks", "error", err)
	}
	if _, err := s.agentPostgresClient.Exec(ctx, `
		UPDATE agent.agent SET state = $1, update_time = $2
		WHERE state = $3 AND update_time < $4 AND delete_time IS NULL`,
		int16(agentpb.AgentState_AGENT_STATE_ACTIVE), time.Now().UTC(),
		int16(agentpb.AgentState_AGENT_STATE_PROCESSING), staleBefore); err != nil {
		s.log.Error("reaping agents", "error", err)
	}
	return false // The reaper never drains; it runs once per tick.
}

// withLockedTask runs fn holding a row lock on the task, persisting the pb it
// mutates. Serializes concurrent resumptions (e.g. two children finishing at
// once cannot double-resume their parent).
func (s *Service) withLockedTask(ctx context.Context, name string, fn func(task *agentpb.Task) ([]string, error)) error {
	organizationID, userID, agentID, taskID, err := model.ParseTaskName(name)
	if err != nil {
		return err
	}
	return s.agentPostgresClient.ExecuteTransaction(ctx, pgx.ReadCommitted, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+taskColumns()+` FROM agent.task
			WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3 AND task_id = $4
			FOR UPDATE`, organizationID, userID, agentID, taskID)
		if err != nil {
			return err
		}
		taskModel, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByNameLax[model.Task])
		if err != nil {
			return err
		}
		task, err := taskModel.ToPb()
		if err != nil {
			return err
		}
		columns, err := fn(task)
		if err != nil || len(columns) == 0 {
			return err
		}
		return s.flushTask(ctx, tx, task, columns)
	})
}

// withLockedAgent is withLockedTask for agents.
func (s *Service) withLockedAgent(ctx context.Context, name string, fn func(agent *agentpb.Agent) ([]string, error)) error {
	organizationID, userID, agentID, err := model.ParseAgentName(name)
	if err != nil {
		return err
	}
	return s.agentPostgresClient.ExecuteTransaction(ctx, pgx.ReadCommitted, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+agentColumns()+` FROM agent.agent
			WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3
			FOR UPDATE`, organizationID, userID, agentID)
		if err != nil {
			return err
		}
		agentModel, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByNameLax[model.Agent])
		if err != nil {
			return err
		}
		agent, err := agentModel.ToPb()
		if err != nil {
			return err
		}
		columns, err := fn(agent)
		if err != nil || len(columns) == 0 {
			return err
		}
		return s.flushAgent(ctx, tx, agent, columns)
	})
}

// taskColumns renders the task select column list.
func taskColumns() string {
	return postgres.SelectQuery("%s", postgres.GetDBColumns(model.Task{}))
}

// agentColumns renders the agent select column list.
func agentColumns() string {
	return postgres.SelectQuery("%s", postgres.GetDBColumns(model.Agent{}))
}

// flushTask writes the given columns of the task pb inside the transaction.
func (s *Service) flushTask(ctx context.Context, tx pgx.Tx, task *agentpb.Task, columns []string) error {
	task.UpdateTime = timestamppb.Now()
	var err error
	if task.Etag, err = aip.ComputeETag(task); err != nil {
		return fmt.Errorf("computing etag: %w", err)
	}
	taskModel, err := model.TaskFromPb(task)
	if err != nil {
		return err
	}
	columns = append(columns, "update_time", "etag")
	params := postgres.GetParams(taskModel, columns...)
	n := len(params)
	params = append(params, taskModel.OrganizationID, taskModel.UserID, taskModel.AgentID, taskModel.TaskID)
	query := fmt.Sprintf(`UPDATE agent.task SET %s WHERE organization_id = $%d AND user_id = $%d AND agent_id = $%d AND task_id = $%d`,
		buildUpdateClause(columns), n+1, n+2, n+3, n+4)
	_, err = tx.Exec(ctx, query, params...)
	return err
}

// flushAgent writes the given columns of the agent pb inside the transaction.
func (s *Service) flushAgent(ctx context.Context, tx pgx.Tx, agent *agentpb.Agent, columns []string) error {
	agent.UpdateTime = timestamppb.Now()
	var err error
	if agent.Etag, err = aip.ComputeETag(agent); err != nil {
		return fmt.Errorf("computing etag: %w", err)
	}
	agentModel, err := model.AgentFromPb(agent)
	if err != nil {
		return err
	}
	columns = append(columns, "update_time", "etag")
	params := postgres.GetParams(agentModel, columns...)
	n := len(params)
	params = append(params, agentModel.OrganizationID, agentModel.UserID, agentModel.AgentID)
	query := fmt.Sprintf(`UPDATE agent.agent SET %s WHERE organization_id = $%d AND user_id = $%d AND agent_id = $%d`,
		buildUpdateClause(columns), n+1, n+2, n+3)
	_, err = tx.Exec(ctx, query, params...)
	return err
}

package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/malonaz/core/gengo/ai/agent/model"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/postgres"
)

var (
	agentSelectQuery = "SELECT " + postgres.QualifyColumns(AgentPostgresColumns, "agent") + " FROM agent "
	agentSaveQuery   = newSaveQuery("agent", AgentPostgresColumns, "organization_id", "user_id", "agent_id")
	agentStatePending = int16(agentpb.Agent_STATE_PENDING)
)

// GetAgentForUpdate reads an agent, tombstones included, and locks it until the transaction ends.
func (t *Tx) GetAgentForUpdate(ctx context.Context, organizationID, userID, agentID string) (*model.Agent, error) {
	query := agentSelectQuery + "WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3 FOR UPDATE"
	rows, err := t.tx.Query(ctx, query, organizationID, userID, agentID)
	if err != nil {
		return nil, fmt.Errorf("selecting agent: %w", err)
	}
	agent, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByNameLax[model.Agent])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, model.ErrAgentNotExist
		}
		return nil, fmt.Errorf("collecting agent: %w", err)
	}
	return agent, nil
}

// InsertAgent inserts an agent. Replaying a request id returns the agent it inserted and false; an agent with the
// same ids inserted by another request is model.ErrAgentAlreadyExists.
func (t *Tx) InsertAgent(ctx context.Context, requestID string, agent *model.Agent) (*model.Agent, bool, error) {
	row := AgentWithRequestID{RequestID: requestID, Agent: *agent}
	inserted, created, err := insertOnce(ctx, t.tx, "agent", AgentWithRequestIDPostgresColumns, row, requestID, model.ErrAgentAlreadyExists)
	if err != nil {
		return nil, false, err
	}
	return &inserted.Agent, created, nil
}

// SaveAgent rewrites every non-key column of an agent.
func (t *Tx) SaveAgent(ctx context.Context, agent *model.Agent) error {
	commandTag, err := t.tx.Exec(ctx, agentSaveQuery.sql, postgres.GetParams(*agent, agentSaveQuery.columns...)...)
	if err != nil {
		return fmt.Errorf("saving agent: %w", err)
	}
	if commandTag.RowsAffected() == 0 {
		return model.ErrAgentNotExist
	}
	return nil
}

// ListLeasedAgents lists the live agents leased to an executor, by name.
func (s *Store) ListLeasedAgents(ctx context.Context, executor string) ([]*model.Agent, error) {
	query := agentSelectQuery + "WHERE delete_time IS NULL AND leased_executor = $1 ORDER BY create_time, agent_id"
	return s.collectAgents(ctx, query, executor)
}

// ListOrphanedAgents lists live agents leased to an executor that is OFFLINE or missing.
func (s *Store) ListOrphanedAgents(ctx context.Context, limit int) ([]*model.Agent, error) {
	query := agentSelectQuery + `WHERE delete_time IS NULL AND leased_executor <> '' AND NOT EXISTS (
		SELECT 1 FROM executor WHERE executor.state = $1
		-- leased_executor holds a resource name: rebuilt from the executor's ids to join on it.
		AND agent.leased_executor = 'organizations/' || executor.organization_id || '/users/' || executor.user_id || '/executors/' || executor.executor_id
	) ORDER BY create_time, agent_id LIMIT $2`
	return s.collectAgents(ctx, query, executorStateOnline, limit)
}

// ListPendingAgents lists live PENDING agents, oldest first. An empty organizationID (or userID) spans them all.
func (s *Store) ListPendingAgents(ctx context.Context, organizationID, userID string, limit int) ([]*model.Agent, error) {
	query := agentSelectQuery + `WHERE delete_time IS NULL AND state = $1
		AND ($2 = '' OR organization_id = $2) AND ($3 = '' OR user_id = $3)
		ORDER BY create_time, agent_id LIMIT $4`
	return s.collectAgents(ctx, query, agentStatePending, organizationID, userID, limit)
}

// ListChildAgents lists the live agents a parent agent launched, by name.
func (s *Store) ListChildAgents(ctx context.Context, parentAgent string) ([]*model.Agent, error) {
	query := agentSelectQuery + "WHERE delete_time IS NULL AND parent_agent = $1 ORDER BY create_time, agent_id"
	return s.collectAgents(ctx, query, parentAgent)
}

// ListTreeAgents lists every agent of the tree rooted at rootAgent, deleted ones included, oldest first.
func (s *Store) ListTreeAgents(ctx context.Context, rootAgent string) ([]*model.Agent, error) {
	query := agentSelectQuery + "WHERE root_agent = $1 ORDER BY create_time, agent_id"
	return s.collectAgents(ctx, query, rootAgent)
}

func (s *Store) collectAgents(ctx context.Context, query string, params ...any) ([]*model.Agent, error) {
	rows, err := s.client.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("selecting agents: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[model.Agent])
}

// insertOnce inserts a row carrying a request id, tolerating replays: a conflict on the request id returns the row
// that request inserted, a conflict on the primary key alone is alreadyExists.
func insertOnce[T any](ctx context.Context, tx pgx.Tx, table string, columns []string, row T, requestID string, alreadyExists error) (*T, bool, error) {
	returning := " RETURNING " + strings.Join(columns, ",")
	query, params := postgres.InsertQuery("INSERT INTO "+table+" %s VALUES %s ON CONFLICT DO NOTHING", row, columns...)
	rows, err := tx.Query(ctx, query+returning, params...)
	if err != nil {
		return nil, false, fmt.Errorf("inserting %s: %w", table, err)
	}
	inserted, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByNameLax[T])
	if err == nil {
		return inserted, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("collecting inserted %s: %w", table, err)
	}

	query = "SELECT " + strings.Join(columns, ",") + " FROM " + table + " WHERE request_id = $1"
	rows, err = tx.Query(ctx, query, requestID)
	if err != nil {
		return nil, false, fmt.Errorf("selecting %s by request id: %w", table, err)
	}
	existing, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByNameLax[T])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, alreadyExists
		}
		return nil, false, fmt.Errorf("collecting %s by request id: %w", table, err)
	}
	return existing, false, nil
}

// saveQuery rewrites every non-key column of a row by primary key. Its parameters are the row's columns in order.
type saveQuery struct {
	sql     string
	columns []string
}

func newSaveQuery(table string, columns []string, keyColumns ...string) *saveQuery {
	keyColumnSet := make(map[string]struct{}, len(keyColumns))
	for _, keyColumn := range keyColumns {
		keyColumnSet[keyColumn] = struct{}{}
	}
	var assignments, conditions, orderedColumns []string
	for _, column := range columns {
		if _, ok := keyColumnSet[column]; !ok {
			orderedColumns = append(orderedColumns, column)
			assignments = append(assignments, fmt.Sprintf("%s = $%d", column, len(orderedColumns)))
		}
	}
	for _, keyColumn := range keyColumns {
		orderedColumns = append(orderedColumns, keyColumn)
		conditions = append(conditions, fmt.Sprintf("%s = $%d", keyColumn, len(orderedColumns)))
	}
	return &saveQuery{
		sql:     "UPDATE " + table + " SET " + strings.Join(assignments, ", ") + " WHERE " + strings.Join(conditions, " AND "),
		columns: orderedColumns,
	}
}

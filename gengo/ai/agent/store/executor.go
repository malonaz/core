package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/malonaz/core/gengo/ai/agent/model"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/postgres"
	"github.com/malonaz/core/go/uuid"
)

var (
	executorStateOnline  = int16(agentpb.Executor_STATE_ONLINE)
	executorStateOffline = int16(agentpb.Executor_STATE_OFFLINE)
	executorReturning    = " RETURNING " + strings.Join(ExecutorPostgresColumns, ",")
	executorSelectQuery  = "SELECT " + postgres.QualifyColumns(ExecutorPostgresColumns, "executor") + " FROM executor "
	executorAgentCount   = "(SELECT COUNT(*) FROM agent WHERE agent.leased_executor = $%d AND agent.delete_time IS NULL)"
)

// ExecutorQuery selects an executor able to take one more agent.
type ExecutorQuery struct {
	// The organization and user owning the executor. Required.
	OrganizationID string
	UserID         string
	// Restricts the search to this executor (a pin, the parent's). Empty: any.
	ExecutorID string
	// Labels the executor must carry. Empty matches any.
	Selector map[string]string
	// Tools the executor must provide.
	Tools []string
	// An agent name: skips executors holding a refusal for it expiring after Now.
	RefusingAgent string
	Now           time.Time
}

// GetExecutorSession returns the executor's current session, locked FOR SHARE: empty when disconnected or missing.
func (t *Tx) GetExecutorSession(ctx context.Context, organizationID, userID, executorID string) (string, error) {
	query := `SELECT COALESCE(session_id::TEXT, '') FROM executor
		WHERE organization_id = $1 AND user_id = $2 AND executor_id = $3 FOR SHARE`
	var session string
	if err := t.tx.QueryRow(ctx, query, organizationID, userID, executorID).Scan(&session); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("selecting executor session: %w", err)
	}
	return session, nil
}

// FindExecutor returns the least loaded ONLINE, connected executor matching the query with spare capacity, locked
// FOR UPDATE, or nil if there is none.
func (t *Tx) FindExecutor(ctx context.Context, executorQuery *ExecutorQuery) (*model.Executor, error) {
	conditions := []string{
		"organization_id = $1", "user_id = $2", "state = $3", "session_id IS NOT NULL", "agent_count < max_agents", "tools @> $4",
	}
	params := []any{executorQuery.OrganizationID, executorQuery.UserID, executorStateOnline, nonNil(executorQuery.Tools)}
	addCondition := func(condition string, param any) {
		params = append(params, param)
		conditions = append(conditions, fmt.Sprintf(condition, len(params)))
	}
	if executorQuery.ExecutorID != "" {
		addCondition("executor_id = $%d", executorQuery.ExecutorID)
	}
	if len(executorQuery.Selector) > 0 {
		selectorBytes, err := json.Marshal(executorQuery.Selector)
		if err != nil {
			return nil, fmt.Errorf("marshaling selector: %w", err)
		}
		addCondition("labels @> $%d::JSONB", string(selectorBytes))
	}
	if executorQuery.RefusingAgent != "" {
		params = append(params, executorQuery.RefusingAgent, executorQuery.Now)
		conditions = append(conditions, fmt.Sprintf(`NOT EXISTS (
			SELECT 1 FROM executor_refusal WHERE executor_refusal.agent = $%d AND executor_refusal.expire_time > $%d
			AND executor_refusal.organization_id = executor.organization_id AND executor_refusal.user_id = executor.user_id
			AND executor_refusal.executor_id = executor.executor_id
		)`, len(params)-1, len(params)))
	}
	query := executorSelectQuery + "WHERE " + strings.Join(conditions, " AND ") +
		" ORDER BY agent_count, executor_id LIMIT 1 FOR UPDATE"
	rows, err := t.tx.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("selecting executor: %w", err)
	}
	executor, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByNameLax[model.Executor])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("collecting executor: %w", err)
	}
	return executor, nil
}

// RefreshExecutorAgentCount recounts the live agents leased to an executor.
func (t *Tx) RefreshExecutorAgentCount(ctx context.Context, organizationID, userID, executorID string, updateTime time.Time) error {
	if err := lockExecutor(ctx, t.tx, organizationID, userID, executorID); err != nil {
		return err
	}
	query := "UPDATE executor SET agent_count = " + fmt.Sprintf(executorAgentCount, 4) +
		", update_time = $5 WHERE organization_id = $1 AND user_id = $2 AND executor_id = $3"
	executor := executorName(organizationID, userID, executorID)
	if _, err := t.tx.Exec(ctx, query, organizationID, userID, executorID, executor, updateTime); err != nil {
		return fmt.Errorf("refreshing executor agent count: %w", err)
	}
	return nil
}

// lockExecutor locks an executor's row before a statement counting its agents: a statement waiting on the lock
// re-checks only the row, while its count subquery keeps the snapshot taken before the wait, missing the agents
// leased by the transaction it waited on. A statement run after the lock gets a fresh snapshot.
func lockExecutor(ctx context.Context, tx pgx.Tx, organizationID, userID, executorID string) error {
	query := "SELECT 1 FROM executor WHERE organization_id = $1 AND user_id = $2 AND executor_id = $3 FOR UPDATE"
	if _, err := tx.Exec(ctx, query, organizationID, userID, executorID); err != nil {
		return fmt.Errorf("locking executor: %w", err)
	}
	return nil
}

// InsertRefusal records that an executor refuses an agent until expireTime, and drops the agent's expired refusals.
func (t *Tx) InsertRefusal(ctx context.Context, agent, organizationID, userID, executorID string, expireTime time.Time) error {
	query := `WITH expired AS (
			DELETE FROM executor_refusal WHERE agent = $1 AND expire_time <= NOW() AT TIME ZONE 'UTC'
		)
		INSERT INTO executor_refusal (agent, organization_id, user_id, executor_id, expire_time) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (agent, organization_id, user_id, executor_id) DO UPDATE SET expire_time = EXCLUDED.expire_time`
	if _, err := t.tx.Exec(ctx, query, agent, organizationID, userID, executorID, expireTime); err != nil {
		return fmt.Errorf("inserting refusal: %w", err)
	}
	return nil
}

// RegisterExecutor upserts an executor and binds it to a session: labels, display name, capacity, tools, state and
// times are taken from the executor (create time on insert only), and its agent count is recomputed.
func (s *Store) RegisterExecutor(ctx context.Context, executor *model.Executor, sessionID string) (*model.Executor, error) {
	name := executorName(executor.OrganizationID, executor.UserID, executor.ExecutorID)
	requestID := uuid.NewV5(aip.MustGetUUIDNamespace(&agentpb.Executor{}), name).String()
	query := `INSERT INTO executor (
			request_id, organization_id, user_id, executor_id, create_time, update_time, labels, display_name, max_agents,
			tools, state, connect_time, heartbeat_time, agent_count, session_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, ` + fmt.Sprintf(executorAgentCount, 14) + `, $15)
		ON CONFLICT (organization_id, user_id, executor_id) DO UPDATE SET
			update_time = EXCLUDED.update_time, labels = EXCLUDED.labels, display_name = EXCLUDED.display_name,
			max_agents = EXCLUDED.max_agents, tools = EXCLUDED.tools, state = EXCLUDED.state,
			connect_time = EXCLUDED.connect_time, heartbeat_time = EXCLUDED.heartbeat_time,
			agent_count = EXCLUDED.agent_count, session_id = EXCLUDED.session_id` + executorReturning
	params := []any{
		requestID, executor.OrganizationID, executor.UserID, executor.ExecutorID, executor.CreateTime, executor.UpdateTime,
		executor.Labels, executor.DisplayName, executor.MaxAgents, nonNil(executor.Tools), executor.State,
		executor.ConnectTime, executor.HeartbeatTime, name, sessionID,
	}
	var registered *model.Executor
	err := s.Transaction(ctx, func(tx *Tx) error {
		if err := lockExecutor(ctx, tx.tx, executor.OrganizationID, executor.UserID, executor.ExecutorID); err != nil {
			return err
		}
		rows, err := tx.tx.Query(ctx, query, params...)
		if err != nil {
			return fmt.Errorf("registering executor: %w", err)
		}
		registered, err = pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByNameLax[model.Executor])
		return err
	})
	if err != nil {
		return nil, err
	}
	return registered, nil
}

// HeartbeatExecutor stamps an executor's heartbeat while it is ONLINE with that session. It reports whether it did.
func (s *Store) HeartbeatExecutor(ctx context.Context, organizationID, userID, executorID, sessionID string, heartbeatTime time.Time) (bool, error) {
	query := `UPDATE executor SET heartbeat_time = $5
		WHERE organization_id = $1 AND user_id = $2 AND executor_id = $3 AND session_id = $4 AND state = $6`
	return s.execAffects(ctx, query, organizationID, userID, executorID, sessionID, heartbeatTime, executorStateOnline)
}

// DetachExecutor clears an executor's session if it still is that one, keeping it ONLINE until its heartbeat
// deadline: a broken stream may come back. It reports whether it did.
func (s *Store) DetachExecutor(ctx context.Context, organizationID, userID, executorID, sessionID string, updateTime time.Time) (bool, error) {
	query := `UPDATE executor SET session_id = NULL, update_time = $5
		WHERE organization_id = $1 AND user_id = $2 AND executor_id = $3 AND session_id = $4`
	return s.execAffects(ctx, query, organizationID, userID, executorID, sessionID, updateTime)
}

// DisconnectExecutor takes an executor OFFLINE and clears its session if it still is that one. It reports whether
// it did.
func (s *Store) DisconnectExecutor(ctx context.Context, organizationID, userID, executorID, sessionID string, updateTime time.Time) (bool, error) {
	query := `UPDATE executor SET state = $6, session_id = NULL, update_time = $5
		WHERE organization_id = $1 AND user_id = $2 AND executor_id = $3 AND session_id = $4`
	return s.execAffects(ctx, query, organizationID, userID, executorID, sessionID, updateTime, executorStateOffline)
}

// ExpireExecutors takes the ONLINE executors silent since before the deadline OFFLINE, clearing their sessions, and
// returns them.
func (s *Store) ExpireExecutors(ctx context.Context, deadline, updateTime time.Time) ([]*model.Executor, error) {
	query := `UPDATE executor SET state = $1, session_id = NULL, update_time = $2
		WHERE state = $3 AND heartbeat_time < $4` + executorReturning
	rows, err := s.client.Query(ctx, query, executorStateOffline, updateTime, executorStateOnline, deadline)
	if err != nil {
		return nil, fmt.Errorf("expiring executors: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[model.Executor])
}

func (s *Store) execAffects(ctx context.Context, query string, params ...any) (bool, error) {
	commandTag, err := s.client.Exec(ctx, query, params...)
	if err != nil {
		return false, fmt.Errorf("updating executor: %w", err)
	}
	return commandTag.RowsAffected() > 0, nil
}

func executorName(organizationID, userID, executorID string) string {
	executorRn := &agentpb.ExecutorRn{Organization: organizationID, User: userID, Executor: executorID}
	return executorRn.String()
}

// nonNil keeps an empty array an array: pgx encodes a nil slice as NULL.
func nonNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

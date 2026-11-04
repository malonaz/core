package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/malonaz/core/gengo/ai/agent/model"
	"github.com/malonaz/core/go/postgres"
)

var inputSelectQuery = "SELECT " + postgres.QualifyColumns(InputPostgresColumns, "input") + " FROM input "

// InsertInput inserts an input. Replaying a request id returns the input it inserted and false; an input with the
// same ids inserted by another request is model.ErrInputAlreadyExists.
func (t *Tx) InsertInput(ctx context.Context, requestID string, input *model.Input) (*model.Input, bool, error) {
	row := InputWithRequestID{RequestID: requestID, Input: *input}
	inserted, created, err := insertOnce(ctx, t.tx, "input", InputWithRequestIDPostgresColumns, row, requestID, model.ErrInputAlreadyExists)
	if err != nil {
		return nil, false, err
	}
	return &inserted.Input, created, nil
}

// ConsumeInputs stamps the given inputs of an agent consumed, skipping those already consumed or of another agent.
func (t *Tx) ConsumeInputs(ctx context.Context, organizationID, userID, agentID string, inputIDs []string, consumeTime time.Time) error {
	query := `UPDATE input SET consume_time = $5
		WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3 AND input_id = ANY($4) AND consume_time IS NULL`
	if _, err := t.tx.Exec(ctx, query, organizationID, userID, agentID, inputIDs, consumeTime); err != nil {
		return fmt.Errorf("consuming inputs: %w", err)
	}
	return nil
}

// ListPendingInputs lists an agent's unconsumed inputs in the order they were queued.
func (t *Tx) ListPendingInputs(ctx context.Context, organizationID, userID, agentID string) ([]*model.Input, error) {
	query := inputSelectQuery + `WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3 AND consume_time IS NULL
		ORDER BY create_time, input_id`
	rows, err := t.tx.Query(ctx, query, organizationID, userID, agentID)
	if err != nil {
		return nil, fmt.Errorf("selecting pending inputs: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[model.Input])
}

// ListPendingInputsOfExecutor lists the unconsumed inputs of the live agents leased to an executor, by name, in the
// order they were queued.
func (s *Store) ListPendingInputsOfExecutor(ctx context.Context, executor string) ([]*model.Input, error) {
	query := inputSelectQuery + `JOIN agent USING (organization_id, user_id, agent_id)
		WHERE agent.leased_executor = $1 AND agent.delete_time IS NULL AND input.consume_time IS NULL
		ORDER BY input.create_time, input.input_id`
	rows, err := s.client.Query(ctx, query, executor)
	if err != nil {
		return nil, fmt.Errorf("selecting pending inputs: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[model.Input])
}

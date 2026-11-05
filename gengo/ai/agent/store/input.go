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

// HasPendingInputs reports whether an agent has unconsumed inputs.
func (t *Tx) HasPendingInputs(ctx context.Context, organizationID, userID, agentID string) (bool, error) {
	query := `SELECT EXISTS (SELECT 1 FROM input
		WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3 AND consume_time IS NULL)`
	var hasPendingInputs bool
	if err := t.tx.QueryRow(ctx, query, organizationID, userID, agentID).Scan(&hasPendingInputs); err != nil {
		return false, fmt.Errorf("selecting pending inputs: %w", err)
	}
	return hasPendingInputs, nil
}

// ConsumeInputs stamps the given inputs of an agent consumed, skipping those already consumed or of another agent.
func (s *Store) ConsumeInputs(ctx context.Context, organizationID, userID, agentID string, inputIDs []string, consumeTime time.Time) error {
	query := `UPDATE input SET consume_time = $5
		WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3 AND input_id = ANY($4) AND consume_time IS NULL`
	if _, err := s.client.Exec(ctx, query, organizationID, userID, agentID, inputIDs, consumeTime); err != nil {
		return fmt.Errorf("consuming inputs: %w", err)
	}
	return nil
}

// ListPendingInputs lists an agent's unconsumed inputs in the order they were queued.
func (s *Store) ListPendingInputs(ctx context.Context, organizationID, userID, agentID string) ([]*model.Input, error) {
	query := inputSelectQuery + `WHERE organization_id = $1 AND user_id = $2 AND agent_id = $3 AND consume_time IS NULL
		ORDER BY create_time, input_id`
	rows, err := s.client.Query(ctx, query, organizationID, userID, agentID)
	if err != nil {
		return nil, fmt.Errorf("selecting pending inputs: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[model.Input])
}

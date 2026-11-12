package store

import (
	"context"
	"errors"
	"fmt"

	v5 "github.com/jackc/pgx/v5"

	"github.com/malonaz/core/gengo/ai/model"
	"github.com/malonaz/core/go/postgres"
)

var getDecisionByRequestIDQuery = "SELECT " + postgres.QualifyColumns(DecisionPostgresColumns, "decision") + " FROM decision" +
	` WHERE decision.organization_id = $1 AND decision.user_id = $2 AND decision.request_id = $3`

// GetDecisionByRequestID returns the decision a create request of the user
// already committed, so a replayed request can be answered without asking the
// model again. It returns model.ErrDecisionNotExist when the request is new.
func (s *Store) GetDecisionByRequestID(ctx context.Context, organizationID, userID, requestID string) (*model.Decision, error) {
	rows, err := s.client.Query(ctx, getDecisionByRequestIDQuery, organizationID, userID, requestID)
	if err != nil {
		return nil, fmt.Errorf("getting decision by request id: %w", err)
	}
	decision, err := v5.CollectOneRow(rows, v5.RowToAddrOfStructByNameLax[model.Decision])
	if err != nil {
		if errors.Is(err, v5.ErrNoRows) {
			return nil, model.ErrDecisionNotExist
		}
		return nil, fmt.Errorf("collecting row: %w", err)
	}
	return decision, nil
}

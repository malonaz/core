package postgres

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestSQLLiteral_Date(t *testing.T) {
	got, err := sqlLiteral(pgtype.Date{Time: time.Date(2025, time.September, 1, 0, 0, 0, 0, time.UTC), Valid: true})
	require.NoError(t, err)
	require.Equal(t, "'2025-09-01'::date", got)
}

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const sqlSelectQueryExample = `
SELECT %s
FROM table_name
WHERE column_name = $1
`

func TestSelectQuery(t *testing.T) {
	t.Run("SingleColumn", func(t *testing.T) {
		query := SelectQuery(sqlSelectQueryExample, []string{"column1"})
		expectedQuery := `
SELECT column1
FROM table_name
WHERE column_name = $1
`
		require.Equal(t, expectedQuery, query)
	})

	t.Run("MultipleColumns", func(t *testing.T) {
		query := SelectQuery(sqlSelectQueryExample, []string{"column1", "column2", "column3"})
		expectedQuery := `
SELECT column1,column2,column3
FROM table_name
WHERE column_name = $1
`
		require.Equal(t, expectedQuery, query)
	})
}

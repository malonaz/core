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

func TestQualifyColumns(t *testing.T) {
	t.Run("SingleColumn", func(t *testing.T) {
		result := QualifyColumns([]string{"id"}, "t")
		require.Equal(t, "t.id", result)
	})

	t.Run("MultipleColumns", func(t *testing.T) {
		result := QualifyColumns([]string{"id", "name", "email"}, "t")
		require.Equal(t, "t.id,t.name,t.email", result)
	})

	t.Run("WithJoinAlias", func(t *testing.T) {
		result := QualifyColumns([]string{"external_id", "genre"}, "j0")
		require.Equal(t, "j0.external_id,j0.genre", result)
	})
}

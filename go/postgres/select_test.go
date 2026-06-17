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
	t.Run("SingleColumnUnqualified", func(t *testing.T) {
		query := SelectQuery(sqlSelectQueryExample, []string{"column1"})
		expectedQuery := `
SELECT column1
FROM table_name
WHERE column_name = $1
`
		require.Equal(t, expectedQuery, query)
	})

	t.Run("MultipleColumnsUnqualified", func(t *testing.T) {
		query := SelectQuery(sqlSelectQueryExample, []string{"column1", "column2", "column3"})
		expectedQuery := `
SELECT column1,column2,column3
FROM table_name
WHERE column_name = $1
`
		require.Equal(t, expectedQuery, query)
	})

	t.Run("SingleColumnFullyQualified", func(t *testing.T) {
		query := SelectQuery(sqlSelectQueryExample, []string{"public.table_name.column1"})
		expectedQuery := `
SELECT public.table_name.column1 AS "public.table_name.column1"
FROM table_name
WHERE column_name = $1
`
		require.Equal(t, expectedQuery, query)
	})

	t.Run("MultipleColumnsFullyQualified", func(t *testing.T) {
		query := SelectQuery(sqlSelectQueryExample, []string{
			"library.shelf.display_name",
			"library.shelf.genre",
		})
		expectedQuery := `
SELECT library.shelf.display_name AS "library.shelf.display_name",library.shelf.genre AS "library.shelf.genre"
FROM table_name
WHERE column_name = $1
`
		require.Equal(t, expectedQuery, query)
	})

	t.Run("MixedQualifiedAndUnqualified", func(t *testing.T) {
		query := SelectQuery(sqlSelectQueryExample, []string{
			"library.shelf.display_name",
			"plain_column",
		})
		expectedQuery := `
SELECT library.shelf.display_name AS "library.shelf.display_name",plain_column
FROM table_name
WHERE column_name = $1
`
		require.Equal(t, expectedQuery, query)
	})
}

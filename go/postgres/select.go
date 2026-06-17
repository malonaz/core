package postgres

import (
	"fmt"
	"strings"
)

// SelectQuery injects dbColumns into a sqlQuery template.
// Each column is aliased so that the result column name matches the fully qualified db tag.
func SelectQuery(sqlQueryTemplate string, dbColumns []string) string {
	aliased := make([]string, len(dbColumns))
	for i, col := range dbColumns {
		if strings.Contains(col, ".") {
			aliased[i] = fmt.Sprintf("%s AS \"%s\"", col, col)
		} else {
			aliased[i] = col
		}
	}
	columns := strings.Join(aliased, ",")
	query := fmt.Sprintf(sqlQueryTemplate, columns)
	return query
}

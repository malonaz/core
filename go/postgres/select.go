package postgres

import (
	"fmt"
	"strings"
)

// SelectQuery injects dbColumns into a sqlQuery template
func SelectQuery(sqlQueryTemplate string, dbColumns []string) string {
	columns := strings.Join(dbColumns, ",")
	query := fmt.Sprintf(sqlQueryTemplate, columns)
	return query
}

// QualifyColumns returns a comma-separated string of columns prefixed with the given table alias.
func QualifyColumns(columns []string, alias string) string {
	qualified := make([]string, len(columns))
	for i, c := range columns {
		qualified[i] = alias + "." + c
	}
	return strings.Join(qualified, ",")
}

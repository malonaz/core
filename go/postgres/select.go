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

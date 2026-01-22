package postgres

import (
	"strings"

	"go.einride.tech/aip/ordering"
)

func TranspileOrderBy(orderBy ordering.OrderBy) string {
	if len(orderBy.Fields) == 0 {
		return ""
	}
	result := make([]string, 0, len(orderBy.Fields))
	for _, field := range orderBy.Fields {
		order := field.Path
		if field.Desc {
			order += " DESC"
		}
		result = append(result, order)
	}
	return "ORDER BY " + strings.Join(result, ", ")
}

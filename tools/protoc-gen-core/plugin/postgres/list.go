package postgres

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
)

func (mc *msgCtx) generateList() {
	g := mc.g
	pluralGoName := mc.pr.PluralGoName()
	pluralUntitled := untitle(pluralGoName)

	parentParam := ""
	if len(mc.parentBindings) > 0 {
		names := make([]string, len(mc.parentBindings))
		for i, binding := range mc.parentBindings {
			names[i] = untitle(xstrings.ToCamelCase(binding.Variable)) + "Id"
		}
		parentParam = strings.Join(names, ", ") + " string, "
	}
	showDeletedParam := ""
	if mc.hasDeleteTime {
		showDeletedParam = "showDeleted bool, "
	}

	g.P(fmt.Sprintf("func (s *Store) List%s(ctx context.Context, %s%swhereClause, orderByClause, paginationClause string, columns []string, params ...any) ([]*%s, error) {",
		pluralGoName, parentParam, showDeletedParam, mc.goTypeFqi))

	g.P("  if columns == nil {")
	g.P(fmt.Sprintf("    columns = %s", mc.writeColumns()))
	g.P("  }")
	g.P()

	colPrefix := mc.bareTableName + "."

	if len(mc.parentBindings) > 0 {
		for _, binding := range mc.parentBindings {
			paramName := untitle(xstrings.ToCamelCase(binding.Variable))
			if binding.Shared {
				g.P(fmt.Sprintf("  if %sId != \"-\" && %sId != \"\" {", paramName, paramName))
				g.P(fmt.Sprintf("    whereClause = %s(whereClause, %s(\"%s%s = $%%d\", len(params) + 1))",
					mc.postgres("AddToWhereClause"), mc.fmtI("Sprintf"), colPrefix, binding.Column))
				g.P(fmt.Sprintf("    params = append(params, %sId)", paramName))
				g.P("  }")
				continue
			}
			// Non-shared parent identifiers: "" means the matched parent
			// pattern lacks this segment, so the row must not populate it;
			// "-" means any populated value.
			g.P(fmt.Sprintf("  if %sId == \"-\" {", paramName))
			g.P(fmt.Sprintf("    whereClause = %s(whereClause, \"%s%s IS NOT NULL\")", mc.postgres("AddToWhereClause"), colPrefix, binding.Column))
			g.P(fmt.Sprintf("  } else if %sId != \"\" {", paramName))
			g.P(fmt.Sprintf("    whereClause = %s(whereClause, %s(\"%s%s = $%%d\", len(params) + 1))",
				mc.postgres("AddToWhereClause"), mc.fmtI("Sprintf"), colPrefix, binding.Column))
			g.P(fmt.Sprintf("    params = append(params, %sId)", paramName))
			g.P("  } else {")
			g.P(fmt.Sprintf("    whereClause = %s(whereClause, \"%s%s IS NULL\")", mc.postgres("AddToWhereClause"), colPrefix, binding.Column))
			g.P("  }")
		}
		g.P()
	}

	if mc.hasDeleteTime {
		g.P("  if !showDeleted {")
		g.P(fmt.Sprintf("    whereClause = %s(whereClause, \"%sdelete_time IS NULL\")", mc.postgres("AddToWhereClause"), colPrefix))
		g.P("  }")
		g.P()
	}

	g.P(fmt.Sprintf("  query := %s + \" \" + whereClause + \" \" + orderByClause + \" \" + paginationClause", mc.selectExpr("columns")))
	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(\"selecting %s: %%w\", err)", mc.fmtI("Errorf"), pluralUntitled))
	g.P("  }")
	g.P(fmt.Sprintf("  return %s(rows, %s[%s])", mc.pgx("CollectRows"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("}")
	g.P()
}

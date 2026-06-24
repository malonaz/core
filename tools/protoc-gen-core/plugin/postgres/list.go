package postgres

import (
	"fmt"

	"github.com/huandu/xstrings"
)

func (mc *msgCtx) generateList() {
	g := mc.g
	pluralGoName := mc.pr.PluralGoName()
	pluralUntitled := untitle(pluralGoName)

	parentParam := ""
	if mc.pr.Parent != nil {
		parentParam = mc.pr.Parent.PatternVariableIDs(true) + " string, "
	}
	showDeletedParam := ""
	if mc.hasDeleteTime {
		showDeletedParam = "showDeleted bool, "
	}

	g.P(fmt.Sprintf("func (s *Store) List%s(ctx context.Context, %s%swhereClause, orderByClause, paginationClause string, columns []string, params ...any) ([]*%s, error) {",
		pluralGoName, parentParam, showDeletedParam, mc.goTypeFqi))

	if mc.hasJoins {
		g.P("  if columns == nil {")
		g.P(fmt.Sprintf("    columns = %s", mc.writeColumns()))
		g.P("  }")
	} else {
		g.P("  if columns == nil {")
		g.P(fmt.Sprintf("    columns = %sPostgresColumns", mc.goType))
		g.P("  }")
	}
	g.P()

	colPrefix := ""
	if mc.hasJoins {
		colPrefix = mc.bareTableName + "."
	}

	if mc.pr.Parent != nil {
		for _, v := range mc.pr.Parent.PatternVariables {
			camel := untitle(xstrings.ToCamelCase(v))
			g.P(fmt.Sprintf("  if %sId != \"-\" && %sId != \"\" {", camel, camel))
			g.P(fmt.Sprintf("    whereClause = %s(whereClause, %s(\"%s%s_id = $%%d\", len(params) + 1))",
				mc.postgres("AddToWhereClause"), mc.fmtI("Sprintf"), colPrefix, v))
			g.P(fmt.Sprintf("    params = append(params, %sId)", camel))
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

	if mc.hasJoins {
		g.P(fmt.Sprintf("  query := %s(\"SELECT %%s FROM %s \" + %sJoinClause + \" #where# #orderby# #pagination#\", \"#where#\", whereClause)",
			mc.stringsI("ReplaceAll"), mc.tableName, mc.goName))
		g.P(fmt.Sprintf("  query = %s(query, \"#orderby#\", orderByClause)", mc.stringsI("ReplaceAll")))
		g.P(fmt.Sprintf("  query = %s(query, \"#pagination#\", paginationClause)", mc.stringsI("ReplaceAll")))
		g.P(fmt.Sprintf("  query = %s(query, %s(columns, %q) + %sJoinSelectExprs)",
			mc.fmtI("Sprintf"), mc.postgres("QualifyColumns"), mc.bareTableName, mc.goName))
	} else {
		g.P(fmt.Sprintf("  query := %s(\"SELECT %%s FROM %s #where# #orderby# #pagination#\", \"#where#\", whereClause)",
			mc.stringsI("ReplaceAll"), mc.tableName))
		g.P(fmt.Sprintf("  query = %s(query, \"#orderby#\", orderByClause)", mc.stringsI("ReplaceAll")))
		g.P(fmt.Sprintf("  query = %s(query, \"#pagination#\", paginationClause)", mc.stringsI("ReplaceAll")))
		g.P(fmt.Sprintf("  query = %s(query, columns)", mc.postgres("SelectQuery")))
	}
	g.P()

	g.P(fmt.Sprintf("  var %s []*%s", pluralUntitled, mc.goTypeFqi))
	g.P(fmt.Sprintf("  transactionFN := func(tx %s) error {", mc.postgres("Tx")))
	g.P(fmt.Sprintf("    %s = nil", pluralUntitled))
	g.P("    rows, err := tx.Query(ctx, query, params...)")
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      if err == %s {", mc.pgx("ErrNoRows")))
	g.P("        return nil")
	g.P("      }")
	g.P(fmt.Sprintf("      return %s(\"selecting %s: %%w\", err)", mc.fmtI("Errorf"), pluralUntitled))
	g.P("    }")
	g.P(fmt.Sprintf("    %s, err = %s(rows, %s[%s])", pluralUntitled, mc.pgx("CollectRows"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return %s(\"collecting rows: %%w\", err)", mc.fmtI("Errorf")))
	g.P("    }")
	g.P("    return nil")
	g.P("  }")
	g.P(fmt.Sprintf("  return %s, s.client.ExecuteTransaction(ctx, %s, transactionFN)", pluralUntitled, mc.postgres("RepeatableRead")))
	g.P("}")
	g.P()
}

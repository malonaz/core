package postgres

import (
	"fmt"
)

func (mc *msgCtx) generateGet() {
	g := mc.g

	g.P(fmt.Sprintf("func (s *Store) Get%s(ctx context.Context, %s string) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), mc.goTypeFqi))

	if mc.multiPattern {
		g.P(fmt.Sprintf("  conditions := make([]string, 0, %d)", len(mc.columnBindings)))
		g.P(fmt.Sprintf("  params := make([]any, 0, %d)", len(mc.columnBindings)))
		mc.emitIDConditionAppends("  ", idParamName)
		g.P(fmt.Sprintf("  query := %s(\"SELECT %%%%s FROM %s WHERE %%s\", %s(conditions, \" AND \"))",
			mc.fmtI("Sprintf"), mc.tableName, mc.stringsI("Join")))
		g.P(fmt.Sprintf("  query = %s(query, %sPostgresColumns)", mc.postgres("SelectQuery"), mc.goType))
		g.P("  rows, err := s.client.Query(ctx, query, params...)")
	} else if mc.hasJoins {
		g.P(fmt.Sprintf("  query := `SELECT %%s FROM %s ` + %sJoinClause + ` WHERE %s`",
			mc.tableName, mc.goName, mc.qualifiedPlaceholderDecls()))
		g.P(fmt.Sprintf("  query = %s(query, %s(%s, %q) + %sJoinSelectExprs)",
			mc.fmtI("Sprintf"), mc.postgres("QualifyColumns"), mc.writeColumns(), mc.bareTableName, mc.goName))
		g.P(fmt.Sprintf("  rows, err := s.client.Query(ctx, query, %s)", mc.patternVarIDsGoTrue()))
	} else {
		g.P(fmt.Sprintf("  query := `SELECT %%s FROM %s WHERE %s`", mc.tableName, mc.placeholderDecls))
		g.P(fmt.Sprintf("  query = %s(query, %sPostgresColumns)", mc.postgres("SelectQuery"), mc.goType))
		g.P(fmt.Sprintf("  rows, err := s.client.Query(ctx, query, %s)", mc.patternVarIDsGoTrue()))
	}

	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(\"getting %s: %%w\", err)", mc.fmtI("Errorf"), mc.goName))
	g.P("  }")
	g.P(fmt.Sprintf("  row, err := %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if err == %s {", mc.pgx("ErrNoRows")))
	g.P(fmt.Sprintf("      return nil, %s", mc.errNotExist))
	g.P("    }")
	g.P(fmt.Sprintf("    return nil, %s(\"collecting row: %%w\", err)", mc.fmtI("Errorf")))
	g.P("  }")
	g.P("  return row, nil")
	g.P("}")
	g.P()
}

package postgres

import (
	"fmt"
)

func (mc *msgCtx) generateGet() {
	g := mc.g

	g.P(fmt.Sprintf("func (s *Store) Get%s(ctx context.Context, %s string) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), mc.goTypeFqi))
	g.P(fmt.Sprintf("  query := `SELECT %%s FROM %s WHERE %s`", mc.fqTableName, mc.placeholderDecls))
	g.P(fmt.Sprintf("  query = %s(query, %sPostgresColumns)", mc.postgres("SelectQuery"), mc.goType))
	g.P(fmt.Sprintf("  rows, err := s.client.Query(ctx, query, %s)", mc.patternVarIDsGoTrue()))
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

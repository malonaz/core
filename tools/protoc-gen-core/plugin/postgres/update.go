package postgres

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
)

func (mc *msgCtx) generateUpdate() {
	g := mc.g
	writeColumns := mc.writeColumns()
	returning := mc.returningExpr(writeColumns)

	g.P(fmt.Sprintf("var update%sPostgresQuery = `UPDATE %s SET #update_clause# WHERE #where_clause# RETURNING ` +", mc.goType, mc.tableName))
	g.P(returning)
	g.P()

	etagParam := ""
	if mc.hasEtag {
		etagParam = ", etag string"
	}
	g.P(fmt.Sprintf("func (s *Store) Update%s(ctx context.Context, %s *%s, updateClause string, updateColumns []string%s) (*%s, error) {",
		mc.goType, mc.goParam, mc.goTypeFqi, etagParam, mc.goTypeFqi))
	g.P(fmt.Sprintf("  updateParams := %s(%s, updateColumns...)", mc.postgres("GetParams"), mc.goParam))
	g.P()
	g.P(fmt.Sprintf("  query := %s(update%sPostgresQuery, \"#update_clause#\", updateClause, 1)", mc.stringsI("Replace"), mc.goType))

	var whereArgs []string
	for i := range mc.pr.PatternVariables {
		whereArgs = append(whereArgs, fmt.Sprintf("numUpdateParams+%d", i+1))
	}
	g.P("  numUpdateParams := len(updateParams)")
	g.P(fmt.Sprintf("  whereClause := %s(\"%s\", %s)", mc.fmtI("Sprintf"), mc.whereClause, strings.Join(whereArgs, ", ")))
	g.P(fmt.Sprintf("  query = %s(query, \"#where_clause#\", whereClause, 1)", mc.stringsI("Replace")))
	g.P()

	g.P("  params := append(updateParams,")
	for _, v := range mc.pr.PatternVariables {
		camel := xstrings.ToCamelCase(v)
		g.P(fmt.Sprintf("    %s.%sID,", mc.goParam, title(camel)))
	}
	g.P("  )")
	g.P()

	if mc.hasEtag {
		g.P("  if etag != \"\" {")
		g.P(fmt.Sprintf("    query = %s(query, \"RETURNING\", %s(\"AND etag = $%%d RETURNING\", len(params)+1), 1)",
			mc.stringsI("Replace"), mc.fmtI("Sprintf")))
		g.P("    params = append(params, etag)")
		g.P("  }")
	}
	g.P()

	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P(fmt.Sprintf("  row, err := %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if err == %s {", mc.pgx("ErrNoRows")))
	if mc.hasEtag {
		mc.generateETagCheck("update", mc.patternVarFieldAccess(), false)
	}
	g.P(fmt.Sprintf("      return nil, %s", mc.errNotExist))
	g.P("    }")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return row, nil")
	g.P("}")
	g.P()
}

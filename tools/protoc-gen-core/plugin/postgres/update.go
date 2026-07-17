package postgres

import (
	"fmt"
	"strings"
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

	if mc.multiPattern {
		mc.generateMultiPatternUpdateBody()
	} else {
		mc.generateSinglePatternUpdateBody()
	}

	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P(fmt.Sprintf("  row, err := %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if err == %s {", mc.pgx("ErrNoRows")))
	if mc.hasEtag {
		if mc.multiPattern {
			mc.generateETagCheck("update", mc.patternVarIDUntitled(), false)
		} else {
			mc.generateETagCheck("update", mc.patternVarFieldAccess(), false)
		}
	}
	g.P(fmt.Sprintf("      return nil, %s", mc.errNotExist))
	g.P("    }")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return row, nil")
	g.P("}")
	g.P()
}

func (mc *msgCtx) generateSinglePatternUpdateBody() {
	g := mc.g

	g.P(fmt.Sprintf("  updateParams := %s(%s, updateColumns...)", mc.postgres("GetParams"), mc.goParam))
	g.P()
	g.P(fmt.Sprintf("  query := %s(update%sPostgresQuery, \"#update_clause#\", updateClause, 1)", mc.stringsI("Replace"), mc.goType))

	whereArgs := make([]string, len(mc.columnBindings))
	for i := range mc.columnBindings {
		whereArgs[i] = fmt.Sprintf("numUpdateParams+%d", i+1)
	}
	g.P("  numUpdateParams := len(updateParams)")
	g.P(fmt.Sprintf("  whereClause := %s(\"%s\", %s)", mc.fmtI("Sprintf"), mc.whereClause, strings.Join(whereArgs, ", ")))
	g.P(fmt.Sprintf("  query = %s(query, \"#where_clause#\", whereClause, 1)", mc.stringsI("Replace")))
	g.P()

	g.P("  params := append(updateParams,")
	for _, binding := range mc.columnBindings {
		g.P(fmt.Sprintf("    %s.%s,", mc.goParam, binding.GoFieldName()))
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
}

// generateMultiPatternUpdateBody builds the WHERE clause at runtime: only the
// identifiers of the pattern the row was created under are populated, the
// others must be NULL.
func (mc *msgCtx) generateMultiPatternUpdateBody() {
	g := mc.g

	for _, binding := range mc.columnBindings {
		name := idParamName(binding)
		if binding.Shared {
			g.P("  ", name, " := ", mc.goParam, ".", binding.GoFieldName())
		} else {
			g.P("  ", name, " := \"\"")
			g.P("  if ", mc.goParam, ".", binding.GoFieldName(), " != nil {")
			g.P("    ", name, " = *", mc.goParam, ".", binding.GoFieldName())
			g.P("  }")
		}
	}
	g.P()

	g.P(fmt.Sprintf("  params := %s(%s, updateColumns...)", mc.postgres("GetParams"), mc.goParam))
	g.P(fmt.Sprintf("  conditions := make([]string, 0, %d)", len(mc.columnBindings)))
	mc.emitIDConditionAppends("  ", idParamName)
	g.P()

	g.P(fmt.Sprintf("  query := %s(update%sPostgresQuery, \"#update_clause#\", updateClause, 1)", mc.stringsI("Replace"), mc.goType))
	g.P(fmt.Sprintf("  query = %s(query, \"#where_clause#\", %s(conditions, \" AND \"), 1)", mc.stringsI("Replace"), mc.stringsI("Join")))
	g.P()

	if mc.hasEtag {
		g.P("  if etag != \"\" {")
		g.P(fmt.Sprintf("    query = %s(query, \"RETURNING\", %s(\"AND etag = $%%d RETURNING\", len(params)+1), 1)",
			mc.stringsI("Replace"), mc.fmtI("Sprintf")))
		g.P("    params = append(params, etag)")
		g.P("  }")
	}
	g.P()
}

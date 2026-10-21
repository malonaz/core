package postgres

import (
	"fmt"
	"strings"
)

func (mc *msgCtx) generateUpdate() {
	if mc.lazy {
		mc.generateUpsert()
		return
	}
	g := mc.g
	writeColumns := mc.writeColumns()
	returning := mc.returningExpr(writeColumns)

	g.P(fmt.Sprintf("var update%sPostgresQuery = `UPDATE %s SET #update_clause# WHERE #where_clause# RETURNING ` +", mc.goType, mc.tableName))
	g.P(returning)
	g.P()

	g.P(fmt.Sprintf("func (s *Store) Update%s(ctx context.Context, %s *%s, updateClause string, updateColumns []string%s) (*%s, error) {",
		mc.goType, mc.goParam, mc.goTypeFqi, mc.etagMatchParam(), mc.goTypeFqi))

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
	// A tombstone reads as not existing: Update only ever addresses live rows.
	ids := mc.patternVarFieldAccess()
	if mc.multiPattern {
		ids = mc.patternVarIDsGoTrue()
	}
	mc.emitNoRowsProbe(ids, false, true, mc.errNotExist, fmt.Sprintf("%s(\"update matched no rows but %s exists\")", mc.fmtI("Errorf"), mc.goName))
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

	mc.emitEtagFilter()
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

	mc.emitEtagFilter()
	g.P()
}

// generateUpsert emits Upsert{R} for lazy singletons: the first write inserts
// the row, later ones update the given columns on conflict. A client etag
// narrows the conflict update, so a mismatch returns no row.
func (mc *msgCtx) generateUpsert() {
	g := mc.g
	writeColumns := mc.writeColumns()

	g.P(fmt.Sprintf("const upsert%sPostgresQuery = `INSERT INTO %s AS %s %%s VALUES %%s ON CONFLICT (%s) DO UPDATE SET `",
		mc.goType, mc.tableName, mc.bareTableName, mc.columnNames))
	g.P()

	g.P(fmt.Sprintf("func (s *Store) Upsert%s(ctx context.Context, %s *%s, updateColumns []string%s) (*%s, error) {",
		mc.goType, mc.goParam, mc.goTypeFqi, mc.etagMatchParam(), mc.goTypeFqi))
	g.P(fmt.Sprintf("  query, params := %s(upsert%sPostgresQuery, %s, %s...)", mc.postgres("InsertQuery"), mc.goType, mc.goParam, writeColumns))
	g.P("  assignments := make([]string, len(updateColumns))")
	g.P("  for i, column := range updateColumns {")
	g.P("    assignments[i] = column + \" = EXCLUDED.\" + column")
	g.P("  }")
	g.P(fmt.Sprintf("  query += %s(assignments, \", \")", mc.stringsI("Join")))
	if mc.hasEtag {
		g.P("  if etag != \"\" {")
		g.P("    params = append(params, etag)")
		g.P(fmt.Sprintf("    query += %s(\" WHERE %s.etag = $%%d\", len(params))", mc.fmtI("Sprintf"), mc.bareTableName))
		g.P("  }")
	}
	g.P(fmt.Sprintf("  query += \" RETURNING \" + %s", mc.returningExpr(writeColumns)))
	g.P()

	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P(fmt.Sprintf("  row, err := %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	if mc.hasEtag {
		g.P(fmt.Sprintf("  if err == %s {", mc.pgx("ErrNoRows")))
		g.P(fmt.Sprintf("    return nil, %s", mc.errETagChanged))
		g.P("  }")
	}
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return row, nil")
	g.P("}")
	g.P()
}

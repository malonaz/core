package postgres

import (
	"fmt"
)

func (mc *msgCtx) generateDelete() {
	if mc.hasDeleteTime {
		mc.generateSoftDelete()
	} else {
		mc.generateHardDelete()
	}
}

func (mc *msgCtx) generateSoftDelete() {
	g := mc.g
	numVars := len(mc.pr.PatternVariables)
	writeColumns := mc.writeColumns()

	etagSet := ""
	if mc.hasEtag {
		etagSet = fmt.Sprintf(", etag = $%d", numVars+2)
	}

	returningExpr := mc.returningExpr(writeColumns)

	g.P(fmt.Sprintf("var softDelete%sPostgresQuery = `UPDATE %s SET delete_time = COALESCE(delete_time, $%d)%s WHERE %s RETURNING (delete_time < $%d) AS was_already_deleted, ` +",
		mc.goType, mc.tableName, numVars+1, etagSet, mc.placeholderDecls, numVars+1))
	g.P(fmt.Sprintf("  %s", returningExpr))
	g.P()

	g.P(fmt.Sprintf("type softDelete%sResult struct {", mc.goType))
	g.P("  WasAlreadyDeleted bool `db:\"was_already_deleted\"`")
	g.P(fmt.Sprintf("  %s", mc.goTypeFqi))
	g.P("}")
	g.P()

	etagParam := ""
	if mc.hasEtag {
		etagParam = ", etag, newEtag string"
	}
	g.P(fmt.Sprintf("func (s *Store) SoftDelete%s(ctx context.Context, %s string%s, deleteTime %s) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), etagParam, mc.gen.ident(timePkg, "Time"), mc.goTypeFqi))
	g.P(fmt.Sprintf("  query := softDelete%sPostgresQuery", mc.goType))

	if mc.hasEtag {
		g.P(fmt.Sprintf("  params := []any{ %s, deleteTime, newEtag}", mc.patternVarIDsGoTrue()))
	} else {
		g.P(fmt.Sprintf("  params := []any{ %s, deleteTime}", mc.patternVarIDsGoTrue()))
	}

	if mc.hasEtag {
		g.P("  if etag != \"\" {")
		g.P(fmt.Sprintf("    query = %s(query, \"RETURNING\", %s(\"AND etag = $%%d RETURNING\", len(params)+1), 1)",
			mc.stringsI("Replace"), mc.fmtI("Sprintf")))
		g.P("    params = append(params, etag)")
		g.P("  }")
	}

	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(\"soft deleting %s: %%w\", err)", mc.fmtI("Errorf"), mc.goName))
	g.P("  }")
	g.P(fmt.Sprintf("  row, err := %s(rows, %s[softDelete%sResult])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goType))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if err == %s {", mc.pgx("ErrNoRows")))
	if mc.hasEtag {
		mc.generateETagCheck("soft delete", mc.patternVarIDUntitled())
	}
	g.P(fmt.Sprintf("      return nil, %s", mc.errNotExist))
	g.P("    }")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  if row.WasAlreadyDeleted {")
	g.P(fmt.Sprintf("    return nil, %s", mc.errAlreadyDeleted))
	g.P("  }")
	g.P(fmt.Sprintf("  return &row.%s, nil", mc.goType))
	g.P("}")
	g.P()
}

func (mc *msgCtx) generateHardDelete() {
	g := mc.g
	writeColumns := mc.writeColumns()
	returningExpr := mc.returningExpr(writeColumns)

	g.P(fmt.Sprintf("var delete%sPostgresQuery = `DELETE FROM %s WHERE %s RETURNING ` + ", mc.goType, mc.tableName, mc.placeholderDecls))
	g.P(returningExpr)
	g.P()

	etagParam := ""
	if mc.hasEtag {
		etagParam = ", etag string"
	}
	g.P(fmt.Sprintf("func (s *Store) Delete%s(ctx context.Context, %s string%s) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), etagParam, mc.goTypeFqi))
	g.P(fmt.Sprintf("  query := delete%sPostgresQuery", mc.goType))
	g.P(fmt.Sprintf("  params := []any{ %s }", mc.patternVarIDsGoTrue()))

	if mc.hasEtag {
		g.P("  if etag != \"\" {")
		g.P(fmt.Sprintf("    query = %s(query, \"RETURNING\", %s(\"AND etag = $%%d RETURNING\", len(params)+1), 1)",
			mc.stringsI("Replace"), mc.fmtI("Sprintf")))
		g.P("    params = append(params, etag)")
		g.P("  }")
	}

	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P(fmt.Sprintf("  row, err := %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if err == %s {", mc.pgx("ErrNoRows")))
	if mc.hasEtag {
		mc.generateETagCheck("delete", mc.patternVarIDUntitled())
	}
	g.P(fmt.Sprintf("      return nil, %s", mc.errNotExist))
	g.P("    }")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return row, nil")
	g.P("}")
	g.P()
}

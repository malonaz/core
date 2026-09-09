package postgres

import (
	"fmt"
)

// generateUndelete emits Undelete{R} (AIP-164) for soft-deletable collection
// resources: the tombstone is cleared, and so are the lifecycle singletons the
// Delete cascade tombstoned with it. Collection descendants are left alone —
// Delete only takes them under an explicit `force`, and Undelete has no such
// opt-in, so they stay individually recoverable instead.
func (mc *msgCtx) generateUndelete() {
	if !mc.hasDeleteTime || mc.singleton {
		return
	}
	mc.generateUndeleteNoRows()
	if mc.multiPattern {
		mc.generateMultiPatternUndelete()
		return
	}

	g := mc.g
	numVars := len(mc.columnBindings)

	etagSet := ""
	if mc.hasEtag {
		etagSet = fmt.Sprintf(", etag = $%d", numVars+1)
	}
	g.P(fmt.Sprintf("var undelete%sPostgresQuery = `UPDATE %s SET delete_time = NULL%s WHERE %s AND delete_time IS NOT NULL RETURNING ` +",
		mc.goType, mc.tableName, etagSet, mc.placeholderDecls))
	g.P(fmt.Sprintf("  %s", mc.returningExpr(mc.writeColumns())))
	g.P()

	g.P(fmt.Sprintf("func (s *Store) Undelete%s(ctx context.Context, %s string%s) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), mc.etagParams(), mc.goTypeFqi))
	g.P(fmt.Sprintf("  query := undelete%sPostgresQuery", mc.goType))
	if mc.hasEtag {
		g.P(fmt.Sprintf("  params := []any{ %s, newEtag }", mc.patternVarIDsGoTrue()))
	} else {
		g.P(fmt.Sprintf("  params := []any{ %s }", mc.patternVarIDsGoTrue()))
	}
	mc.emitEtagFilter()

	if mc.hasLifecycleDescendants() {
		mc.generateUndeleteWithTransaction()
	} else {
		mc.generateUndeleteDirect()
	}
	g.P("}")
	g.P()
}

// generateMultiPatternUndelete builds the WHERE clause at runtime, matching
// unset pattern-specific identifiers against NULL.
func (mc *msgCtx) generateMultiPatternUndelete() {
	g := mc.g

	g.P(fmt.Sprintf("func (s *Store) Undelete%s(ctx context.Context, %s string%s) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), mc.etagParams(), mc.goTypeFqi))
	g.P(fmt.Sprintf("  conditions := make([]string, 0, %d)", len(mc.columnBindings)))
	g.P(fmt.Sprintf("  params := make([]any, 0, %d)", len(mc.columnBindings)+2))
	mc.emitIDConditionAppends("  ", idParamName)
	if mc.hasEtag {
		g.P("  params = append(params, newEtag)")
		g.P(fmt.Sprintf("  query := %s(\"UPDATE %s SET delete_time = NULL, etag = $%%d WHERE %%s AND delete_time IS NOT NULL RETURNING \", len(params), %s(conditions, \" AND \")) + %s",
			mc.fmtI("Sprintf"), mc.tableName, mc.stringsI("Join"), mc.returningExpr(mc.writeColumns())))
	} else {
		g.P(fmt.Sprintf("  query := %s(\"UPDATE %s SET delete_time = NULL WHERE %%s AND delete_time IS NOT NULL RETURNING \", %s(conditions, \" AND \")) + %s",
			mc.fmtI("Sprintf"), mc.tableName, mc.stringsI("Join"), mc.returningExpr(mc.writeColumns())))
	}
	mc.emitEtagFilter()
	mc.generateUndeleteDirect()
	g.P("}")
	g.P()
}

func (mc *msgCtx) etagParams() string {
	if mc.hasEtag {
		return ", etag, newEtag string"
	}
	return ""
}

func (mc *msgCtx) etagArg() string {
	if mc.hasEtag {
		return ", etag"
	}
	return ""
}

// emitEtagFilter narrows the write to the client's etag when one is supplied.
func (mc *msgCtx) emitEtagFilter() {
	if !mc.hasEtag {
		return
	}
	g := mc.g
	g.P("  if etag != \"\" {")
	g.P(fmt.Sprintf("    query = %s(query, \"RETURNING\", %s(\"AND etag = $%%d RETURNING\", len(params)+1), 1)",
		mc.stringsI("Replace"), mc.fmtI("Sprintf")))
	g.P("    params = append(params, etag)")
	g.P("  }")
}

// hasLifecycleDescendants reports whether any singleton rides on this
// resource's tombstone. Singletons of a soft-deletable resource are always
// soft-deletable themselves (codegen enforces it), so Lifecycle alone decides.
func (mc *msgCtx) hasLifecycleDescendants() bool {
	for _, dc := range mc.descendants {
		if dc.Lifecycle {
			return true
		}
	}
	return false
}

func (mc *msgCtx) generateUndeleteDirect() {
	g := mc.g
	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(\"undeleting %s: %%w\", err)", mc.fmtI("Errorf"), mc.goName))
	g.P("  }")
	g.P(fmt.Sprintf("  row, err := %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if err == %s {", mc.pgx("ErrNoRows")))
	g.P(fmt.Sprintf("      return nil, s.undelete%sNoRows(ctx, %s%s)", mc.goType, mc.patternVarIDsGoTrue(), mc.etagArg()))
	g.P("    }")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return row, nil")
}

func (mc *msgCtx) generateUndeleteWithTransaction() {
	g := mc.g
	ids := mc.patternVarIDsGoTrue()

	g.P(fmt.Sprintf("  var result *%s", mc.goTypeFqi))
	g.P(fmt.Sprintf("  transactionFN := func(tx %s) error {", mc.postgres("Tx")))
	g.P("    result = nil")
	g.P("    rows, err := tx.Query(ctx, query, params...)")
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return %s(\"undeleting %s: %%w\", err)", mc.fmtI("Errorf"), mc.goName))
	g.P("    }")
	g.P(fmt.Sprintf("    result, err = %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      if err == %s {", mc.pgx("ErrNoRows")))
	g.P(fmt.Sprintf("        return s.undelete%sNoRows(ctx, %s%s)", mc.goType, ids, mc.etagArg()))
	g.P("      }")
	g.P("      return err")
	g.P("    }")
	for _, dc := range mc.descendants {
		if !dc.Lifecycle {
			continue
		}
		g.P(fmt.Sprintf("    if _, err := tx.Exec(ctx, `UPDATE %s SET delete_time = NULL WHERE %s`, %s); err != nil {", dc.tableName, dc.whereClause, ids))
		g.P(fmt.Sprintf("      return %s(\"restoring %s with %s: %%w\", err)", mc.fmtI("Errorf"), dc.tableName, mc.goName))
		g.P("    }")
	}
	g.P("    return nil")
	g.P("  }")
	g.P()
	g.P(fmt.Sprintf("  if err := s.client.ExecuteTransaction(ctx, %s, transactionFN); err != nil {", mc.postgres("ReadCommitted")))
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return result, nil")
}

// generateUndeleteNoRows emits the probe that explains an undelete matching
// nothing: the row never existed, is live (AIP-164: ALREADY_EXISTS), or its
// etag moved.
func (mc *msgCtx) generateUndeleteNoRows() {
	g := mc.g
	etagParam, etagSelect, etagScan := "", "", ""
	if mc.hasEtag {
		etagParam, etagSelect, etagScan = ", etag string", ", etag", ", &currentEtag"
	}

	g.P(fmt.Sprintf("func (s *Store) undelete%sNoRows(ctx context.Context, %s string%s) error {",
		mc.goType, mc.patternVarIDsGoTrue(), etagParam))
	if mc.multiPattern {
		g.P(fmt.Sprintf("  conditions := make([]string, 0, %d)", len(mc.columnBindings)))
		g.P(fmt.Sprintf("  params := make([]any, 0, %d)", len(mc.columnBindings)))
		mc.emitIDConditionAppends("  ", idParamName)
		g.P(fmt.Sprintf("  query := %s(\"SELECT delete_time IS NULL%s FROM %s WHERE %%s\", %s(conditions, \" AND \"))",
			mc.fmtI("Sprintf"), etagSelect, mc.tableName, mc.stringsI("Join")))
	} else {
		g.P(fmt.Sprintf("  query := `SELECT delete_time IS NULL%s FROM %s WHERE %s`", etagSelect, mc.tableName, mc.placeholderDecls))
		g.P(fmt.Sprintf("  params := []any{ %s }", mc.patternVarIDsGoTrue()))
	}
	g.P("  var live bool")
	if mc.hasEtag {
		g.P("  var currentEtag string")
	}
	g.P(fmt.Sprintf("  if err := s.client.QueryRow(ctx, query, params...).Scan(&live%s); err != nil {", etagScan))
	g.P(fmt.Sprintf("    if err == %s {", mc.pgx("ErrNoRows")))
	g.P(fmt.Sprintf("      return %s", mc.errNotExist))
	g.P("    }")
	g.P(fmt.Sprintf("    return %s(\"probing %s: %%w\", err)", mc.fmtI("Errorf"), mc.goName))
	g.P("  }")
	g.P("  if live {")
	g.P(fmt.Sprintf("    return %s", mc.errNotDeleted))
	g.P("  }")
	if mc.hasEtag {
		g.P("  if etag != \"\" && currentEtag != etag {")
		g.P(fmt.Sprintf("    return %s", mc.errETagChanged))
		g.P("  }")
	}
	g.P(fmt.Sprintf("  return %s(\"undelete matched no rows but %s is deleted\")", mc.fmtI("Errorf"), mc.goName))
	g.P("}")
	g.P()
}

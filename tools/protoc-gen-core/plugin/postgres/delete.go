package postgres

import (
	"fmt"
	"strings"
)

// generateDelete emits Delete{R} or SoftDelete{R}. Singletons get neither:
// they are deleted by their parent's cascade.
func (mc *msgCtx) generateDelete() {
	if mc.singleton {
		return
	}
	if mc.hasDeleteTime {
		mc.generateSoftDelete()
	} else {
		mc.generateHardDelete()
	}
}

// forceParam is the opt-in for cascading over gating descendants (AIP-135).
func (mc *msgCtx) forceParam() string {
	if mc.gated {
		return ", force bool"
	}
	return ""
}

// generateSoftDelete emits SoftDelete{R}. The UPDATE only matches a live row,
// so a tombstone is never rewritten (its etag in particular must survive a
// redundant delete); a miss is explained by the probe.
func (mc *msgCtx) generateSoftDelete() {
	if mc.multiPattern {
		mc.generateMultiPatternSoftDelete()
		return
	}

	g := mc.g
	numVars := len(mc.columnBindings)

	etagSet := ""
	if mc.hasEtag {
		etagSet = fmt.Sprintf(", etag = $%d", numVars+2)
	}

	g.P(fmt.Sprintf("var softDelete%sPostgresQuery = `UPDATE %s SET delete_time = $%d%s WHERE %s AND delete_time IS NULL RETURNING ` +",
		mc.goType, mc.tableName, numVars+1, etagSet, mc.placeholderDecls))
	g.P(fmt.Sprintf("  %s", mc.returningExpr(mc.writeColumns())))
	g.P()

	g.P(fmt.Sprintf("func (s *Store) SoftDelete%s(ctx context.Context, %s string%s%s, deleteTime %s) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), mc.etagWriteParams(), mc.forceParam(), mc.gen.ident(timePkg, "Time"), mc.goTypeFqi))
	g.P(fmt.Sprintf("  query := softDelete%sPostgresQuery", mc.goType))

	if mc.hasEtag {
		g.P(fmt.Sprintf("  params := []any{ %s, deleteTime, newEtag}", mc.patternVarIDsGoTrue()))
	} else {
		g.P(fmt.Sprintf("  params := []any{ %s, deleteTime}", mc.patternVarIDsGoTrue()))
	}

	mc.emitEtagFilter()

	if len(mc.descendants) > 0 {
		mc.generateSoftDeleteWithTransaction()
	} else {
		mc.generateSoftDeleteDirect()
	}

	g.P("}")
	g.P()
}

// generateMultiPatternSoftDelete builds the WHERE clause and parameter indexes
// at runtime, matching unset pattern-specific identifiers against NULL.
func (mc *msgCtx) generateMultiPatternSoftDelete() {
	g := mc.g
	returningExpr := mc.returningExpr(mc.writeColumns())

	g.P(fmt.Sprintf("func (s *Store) SoftDelete%s(ctx context.Context, %s string%s, deleteTime %s) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), mc.etagWriteParams(), mc.gen.ident(timePkg, "Time"), mc.goTypeFqi))
	g.P(fmt.Sprintf("  conditions := make([]string, 0, %d)", len(mc.columnBindings)))
	g.P(fmt.Sprintf("  params := make([]any, 0, %d)", len(mc.columnBindings)+3))
	mc.emitIDConditionAppends("  ", idParamName)
	g.P("  params = append(params, deleteTime)")
	g.P(fmt.Sprintf("  setClause := %s(\"delete_time = $%%d\", len(params))", mc.fmtI("Sprintf")))
	if mc.hasEtag {
		g.P("  params = append(params, newEtag)")
		g.P(fmt.Sprintf("  setClause += %s(\", etag = $%%d\", len(params))", mc.fmtI("Sprintf")))
	}
	g.P(fmt.Sprintf("  query := \"UPDATE %s SET \" + setClause + \" WHERE \" + %s(conditions, \" AND \") + \" AND delete_time IS NULL RETURNING \" + %s",
		mc.tableName, mc.stringsI("Join"), returningExpr))
	mc.emitEtagFilter()

	mc.generateSoftDeleteDirect()

	g.P("}")
	g.P()
}

func (mc *msgCtx) generateSoftDeleteDirect() {
	g := mc.g

	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(\"soft deleting %s: %%w\", err)", mc.fmtI("Errorf"), mc.goName))
	g.P("  }")
	g.P(fmt.Sprintf("  row, err := %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if err == %s {", mc.pgx("ErrNoRows")))
	mc.emitNoRowsProbe(mc.patternVarIDsGoTrue(), false, true, mc.errAlreadyDeleted, mc.softDeleteUnexpected())
	g.P("    }")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return row, nil")
}

// softDeleteUnexpected is the error for a live, etag-matching row the soft
// delete nonetheless missed: a concurrent writer moved it between the two
// statements.
func (mc *msgCtx) softDeleteUnexpected() string {
	return fmt.Sprintf("%s(\"soft delete matched no rows but %s is live\")", mc.fmtI("Errorf"), mc.goName)
}

func (mc *msgCtx) generateSoftDeleteWithTransaction() {
	g := mc.g

	g.P(fmt.Sprintf("  var result *%s", mc.goTypeFqi))
	g.P(fmt.Sprintf("  transactionFN := func(tx %s) error {", mc.postgres("Tx")))
	g.P("    result = nil")
	g.P("    rows, err := tx.Query(ctx, query, params...)")
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return %s(\"soft deleting %s: %%w\", err)", mc.fmtI("Errorf"), mc.goName))
	g.P("    }")
	g.P(fmt.Sprintf("    result, err = %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      if err == %s {", mc.pgx("ErrNoRows")))
	mc.emitNoRowsProbe(mc.patternVarIDsGoTrue(), true, true, mc.errAlreadyDeleted, mc.softDeleteUnexpected())
	g.P("      }")
	g.P("      return err")
	g.P("    }")
	g.P()
	mc.generateCascade()
	g.P("    return nil")
	g.P("  }")
	g.P()
	g.P(fmt.Sprintf("  if err := s.client.ExecuteTransaction(ctx, %s, transactionFN); err != nil {", mc.postgres("ReadCommitted")))
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return result, nil")
}

func (mc *msgCtx) generateHardDelete() {
	if mc.multiPattern {
		mc.generateMultiPatternHardDelete()
		return
	}

	g := mc.g
	writeColumns := mc.writeColumns()
	returningExpr := mc.returningExpr(writeColumns)

	g.P(fmt.Sprintf("var delete%sPostgresQuery = `DELETE FROM %s WHERE %s RETURNING ` + ", mc.goType, mc.tableName, mc.placeholderDecls))
	g.P(returningExpr)
	g.P()

	g.P(fmt.Sprintf("func (s *Store) Delete%s(ctx context.Context, %s string%s%s) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), mc.etagMatchParam(), mc.forceParam(), mc.goTypeFqi))
	g.P(fmt.Sprintf("  query := delete%sPostgresQuery", mc.goType))
	g.P(fmt.Sprintf("  params := []any{ %s }", mc.patternVarIDsGoTrue()))

	mc.emitEtagFilter()

	if len(mc.descendants) > 0 {
		mc.generateHardDeleteWithTransaction()
	} else {
		mc.generateHardDeleteDirect()
	}

	g.P("}")
	g.P()
}

// generateMultiPatternHardDelete builds the WHERE clause at runtime, matching
// unset pattern-specific identifiers against NULL.
func (mc *msgCtx) generateMultiPatternHardDelete() {
	g := mc.g
	returningExpr := mc.returningExpr(mc.writeColumns())

	g.P(fmt.Sprintf("func (s *Store) Delete%s(ctx context.Context, %s string%s) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), mc.etagMatchParam(), mc.goTypeFqi))
	g.P(fmt.Sprintf("  conditions := make([]string, 0, %d)", len(mc.columnBindings)))
	g.P(fmt.Sprintf("  params := make([]any, 0, %d)", len(mc.columnBindings)+1))
	mc.emitIDConditionAppends("  ", idParamName)
	g.P(fmt.Sprintf("  query := %s(\"DELETE FROM %s WHERE %%s RETURNING \", %s(conditions, \" AND \")) + %s",
		mc.fmtI("Sprintf"), mc.tableName, mc.stringsI("Join"), returningExpr))
	mc.emitEtagFilter()

	mc.generateHardDeleteDirect()

	g.P("}")
	g.P()
}

func (mc *msgCtx) generateHardDeleteDirect() {
	g := mc.g

	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P(fmt.Sprintf("  row, err := %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if err == %s {", mc.pgx("ErrNoRows")))
	mc.emitNoRowsProbe(mc.patternVarIDsGoTrue(), false, true, "", mc.hardDeleteUnexpected())
	g.P("    }")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return row, nil")
}

func (mc *msgCtx) generateHardDeleteWithTransaction() {
	g := mc.g

	g.P(fmt.Sprintf("  var deleted *%s", mc.goTypeFqi))
	g.P(fmt.Sprintf("  transactionFN := func(tx %s) error {", mc.postgres("Tx")))
	g.P("    deleted = nil")
	// Descendants go first to respect foreign keys.
	mc.generateCascade()
	g.P("    rows, err := tx.Query(ctx, query, params...)")
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P(fmt.Sprintf("    deleted, err = %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      if err == %s {", mc.pgx("ErrNoRows")))
	mc.emitNoRowsProbe(mc.patternVarIDsGoTrue(), true, true, "", mc.hardDeleteUnexpected())
	g.P("      }")
	g.P("      return err")
	g.P("    }")
	g.P("    return nil")
	g.P("  }")
	g.P()
	g.P(fmt.Sprintf("  if err := s.client.ExecuteTransaction(ctx, %s, transactionFN); err != nil {", mc.postgres("ReadCommitted")))
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return deleted, nil")
}

// hardDeleteUnexpected is the error for an etag-matching row the delete
// nonetheless missed: a concurrent writer moved it between the two statements.
func (mc *msgCtx) hardDeleteUnexpected() string {
	return fmt.Sprintf("%s(\"delete matched no rows but %s exists\")", mc.fmtI("Errorf"), mc.goName)
}

// generateCascade emits, inside a transaction, the AIP-135 children guard and
// the deletion of every descendant, deepest first. Once the guard has passed
// the cascade is idempotent: live rows can only remain beneath a forced delete
// or be tombstones a hard-deleted parent must take with it.
func (mc *msgCtx) generateCascade() {
	g := mc.g
	ids := mc.patternVarIDsGoTrue()
	numVars := len(mc.columnBindings)

	if mc.gated {
		var present []string
		for _, dc := range mc.descendants {
			if !dc.Gating {
				continue
			}
			live := ""
			if dc.HasDeleteTime {
				live = " AND delete_time IS NULL"
			}
			present = append(present, fmt.Sprintf("EXISTS (SELECT 1 FROM %s WHERE %s%s)", dc.tableName, dc.whereClause, live))
		}
		g.P("    if !force {")
		g.P("      var hasChildren bool")
		g.P(fmt.Sprintf("      if err := tx.QueryRow(ctx, `SELECT %s`, %s).Scan(&hasChildren); err != nil {", strings.Join(present, " OR "), ids))
		g.P(fmt.Sprintf("        return %s(\"checking %s children: %%w\", err)", mc.fmtI("Errorf"), mc.goName))
		g.P("      }")
		g.P("      if hasChildren {")
		g.P(fmt.Sprintf("        return %s", mc.errHasChildren))
		g.P("      }")
		g.P("    }")
	}

	for _, dc := range mc.descendants {
		if dc.SoftDelete {
			g.P(fmt.Sprintf("    if _, err := tx.Exec(ctx, `UPDATE %s SET delete_time = COALESCE(delete_time, $%d) WHERE %s`, %s, deleteTime); err != nil {",
				dc.tableName, numVars+1, dc.whereClause, ids))
		} else {
			g.P(fmt.Sprintf("    if _, err := tx.Exec(ctx, `DELETE FROM %s WHERE %s`, %s); err != nil {", dc.tableName, dc.whereClause, ids))
		}
		g.P(fmt.Sprintf("      return %s(\"cascading %s delete to %s: %%w\", err)", mc.fmtI("Errorf"), mc.goName, dc.tableName))
		g.P("    }")
	}
	g.P()
}

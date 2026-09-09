package postgres

import (
	"fmt"
	"strings"
)

func (mc *msgCtx) generateDelete() {
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

func (mc *msgCtx) generateSoftDelete() {
	if mc.multiPattern {
		mc.generateMultiPatternSoftDelete()
		return
	}

	g := mc.g
	numVars := len(mc.columnBindings)
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

	mc.generateSoftDeleteResultStruct()

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

func (mc *msgCtx) generateSoftDeleteResultStruct() {
	g := mc.g
	g.P(fmt.Sprintf("type softDelete%sResult struct {", mc.goType))
	g.P("  WasAlreadyDeleted bool `db:\"was_already_deleted\"`")
	g.P(fmt.Sprintf("  %s", mc.goTypeFqi))
	g.P("}")
	g.P()
}

// generateMultiPatternSoftDelete builds the WHERE clause and parameter indexes
// at runtime, matching unset pattern-specific identifiers against NULL.
func (mc *msgCtx) generateMultiPatternSoftDelete() {
	g := mc.g
	returningExpr := mc.returningExpr(mc.writeColumns())

	mc.generateSoftDeleteResultStruct()

	g.P(fmt.Sprintf("func (s *Store) SoftDelete%s(ctx context.Context, %s string%s, deleteTime %s) (*%s, error) {",
		mc.goType, mc.patternVarIDsGoTrue(), mc.etagWriteParams(), mc.gen.ident(timePkg, "Time"), mc.goTypeFqi))
	g.P(fmt.Sprintf("  conditions := make([]string, 0, %d)", len(mc.columnBindings)))
	g.P(fmt.Sprintf("  params := make([]any, 0, %d)", len(mc.columnBindings)+3))
	mc.emitIDConditionAppends("  ", idParamName)
	g.P("  deleteTimeIndex := len(params) + 1")
	g.P("  params = append(params, deleteTime)")
	if mc.hasEtag {
		g.P("  newEtagIndex := len(params) + 1")
		g.P("  params = append(params, newEtag)")
		g.P(fmt.Sprintf("  query := %s(\"UPDATE %s SET delete_time = COALESCE(delete_time, $%%d), etag = $%%d WHERE %%s RETURNING (delete_time < $%%d) AS was_already_deleted, \", deleteTimeIndex, newEtagIndex, %s(conditions, \" AND \"), deleteTimeIndex) + %s",
			mc.fmtI("Sprintf"), mc.tableName, mc.stringsI("Join"), returningExpr))
	} else {
		g.P(fmt.Sprintf("  query := %s(\"UPDATE %s SET delete_time = COALESCE(delete_time, $%%d) WHERE %%s RETURNING (delete_time < $%%d) AS was_already_deleted, \", deleteTimeIndex, %s(conditions, \" AND \"), deleteTimeIndex) + %s",
			mc.fmtI("Sprintf"), mc.tableName, mc.stringsI("Join"), returningExpr))
	}
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
	g.P(fmt.Sprintf("  row, err := %s(rows, %s[softDelete%sResult])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goType))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if err == %s {", mc.pgx("ErrNoRows")))
	if mc.hasEtag {
		mc.generateETagCheck("soft delete", mc.patternVarIDUntitled(), false)
	}
	g.P(fmt.Sprintf("      return nil, %s", mc.errNotExist))
	g.P("    }")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  if row.WasAlreadyDeleted {")
	g.P(fmt.Sprintf("    return nil, %s", mc.errAlreadyDeleted))
	g.P("  }")
	g.P(fmt.Sprintf("  return &row.%s, nil", mc.goType))
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
	g.P(fmt.Sprintf("    row, err := %s(rows, %s[softDelete%sResult])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goType))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      if err == %s {", mc.pgx("ErrNoRows")))
	if mc.hasEtag {
		mc.generateETagCheck("soft delete", mc.patternVarIDUntitled(), true)
	}
	g.P(fmt.Sprintf("        return %s", mc.errNotExist))
	g.P("      }")
	g.P("      return err")
	g.P("    }")
	g.P("    if row.WasAlreadyDeleted {")
	g.P(fmt.Sprintf("      return %s", mc.errAlreadyDeleted))
	g.P("    }")
	g.P(fmt.Sprintf("    result = &row.%s", mc.goType))
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
	if mc.hasEtag {
		mc.generateETagCheck("delete", mc.patternVarIDUntitled(), false)
	}
	g.P(fmt.Sprintf("      return nil, %s", mc.errNotExist))
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
	if mc.hasEtag {
		mc.generateETagCheck("delete", mc.patternVarIDUntitled(), true)
	}
	g.P(fmt.Sprintf("        return %s", mc.errNotExist))
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

package postgres

import (
	"fmt"

	"github.com/huandu/xstrings"
)

func (mc *msgCtx) generateInsertVars() {
	g := mc.g

	if mc.singleton {
		g.P(fmt.Sprintf("const %sInsertSingletonPostgresQuery = `INSERT INTO %s %%s VALUES %%s ON CONFLICT(%s) DO NOTHING`", mc.goType, mc.tableName, mc.columnNames))
		g.P()
	}

	g.P(fmt.Sprintf("type %sWithRequestID struct {", mc.goType))
	g.P("  RequestID string `db:\"request_id\"`")
	g.P(fmt.Sprintf("  %s", mc.goTypeFqi))
	g.P("}")
	g.P()

	g.P("var (")
	g.P(fmt.Sprintf("  %sWithRequestIDPostgresColumns = %s(%sWithRequestID{})", mc.goType, mc.postgres("GetDBColumns"), mc.goType))
	if mc.hasJoins {
		g.P(fmt.Sprintf("  %sWithRequestIDWritePostgresColumns = %s(%sWithRequestID{}, %s(%s))",
			mc.goType, mc.postgres("GetDBColumns"), mc.goType, mc.postgres("ExceptColumns"), mc.exceptColumnsArgs()))
	}
	// The no-op upsert makes RETURNING yield the existing row on conflict, so
	// the caller can tell a replay (same request id) from a true collision.
	g.P(fmt.Sprintf("  %sInsertPostgresQuery = `INSERT INTO %s %%s VALUES %%s ON CONFLICT(%s) DO UPDATE SET %s = EXCLUDED.%s RETURNING ` + %s",
		mc.goName, mc.tableName, mc.columnNames, mc.identifier, mc.identifier, mc.returningExpr(mc.withRequestIDWriteColumns())))
	if mc.hasJoins {
		g.P(fmt.Sprintf("  %sGetByRequestIDsQuery = %s(`SELECT %%s FROM %s ` + %sJoinClause + ` WHERE %s.request_id = ANY($1)`, %s(%s, %q) + %sJoinSelectExprs)",
			mc.goName, mc.fmtI("Sprintf"), mc.tableName, mc.goName, mc.bareTableName, mc.postgres("QualifyColumns"), mc.withRequestIDWriteColumns(), mc.bareTableName, mc.goName))
	} else {
		g.P(fmt.Sprintf("  %sGetByRequestIDsQuery = `SELECT ` + %s(\"%%s\", %sWithRequestIDPostgresColumns) + ` FROM %s WHERE request_id = ANY($1)`",
			mc.goName, mc.postgres("SelectQuery"), mc.goType, mc.tableName))
	}
	g.P(")")
	g.P()
}

func (mc *msgCtx) withRequestIDWriteColumns() string {
	if mc.hasJoins {
		return mc.goType + "WithRequestIDWritePostgresColumns"
	}
	return mc.goType + "WithRequestIDPostgresColumns"
}

// pluralParam returns the Go parameter name of a slice of the resource, e.g. "authors".
func (mc *msgCtx) pluralParam() string {
	return untitle(xstrings.ToCamelCase(mc.pr.PluralGoName()))
}

// pluralParam returns the Go parameter name of a slice of the child, e.g. "authorProfiles".
func (cc *childCtx) pluralParam() string {
	return untitle(xstrings.ToCamelCase(cc.Resource.PluralGoName()))
}

// generateBatchInsert emits BatchInsert{Plural}: one multi-row INSERT, with
// singleton children inserted in the same transaction. The batch is atomic and
// idempotent on request id: a row that already exists under a request id
// outside this batch fails the whole insert with ErrAlreadyExists, while a
// replay of a previously committed batch returns the existing rows. Rows are
// returned in request order, matched on request id since RETURNING order is
// not guaranteed.
func (mc *msgCtx) generateBatchInsert() {
	g := mc.g
	batchInsertQuery := mc.postgres("BatchInsertQuery")
	collectRows := mc.pgx("CollectRows")
	rowToAddrLax := mc.pgx("RowToAddrOfStructByNameLax")
	withRequestID := mc.goType + "WithRequestID"
	orderFn := fmt.Sprintf("order%sByRequestID", mc.pr.PluralGoName())

	g.P(fmt.Sprintf("func %s(requestIDs []string, rows []*%s) ([]*%s, error) {", orderFn, withRequestID, mc.goTypeFqi))
	g.P("  indexByRequestID := make(map[string]int, len(requestIDs))")
	g.P("  for i, requestID := range requestIDs {")
	g.P("    indexByRequestID[requestID] = i")
	g.P("  }")
	g.P(fmt.Sprintf("  ordered := make([]*%s, len(requestIDs))", mc.goTypeFqi))
	g.P("  for _, row := range rows {")
	g.P("    // A returned request id outside this batch is a pre-existing row.")
	g.P("    i, ok := indexByRequestID[row.RequestID]")
	g.P("    if !ok {")
	g.P(fmt.Sprintf("      return nil, %s", mc.errAlreadyExists))
	g.P("    }")
	g.P(fmt.Sprintf("    ordered[i] = &row.%s", mc.goType))
	g.P("  }")
	g.P("  for i, row := range ordered {")
	g.P("    if row == nil {")
	g.P(fmt.Sprintf("      return nil, %s(\"inserted %s with request id %%q was not returned\", requestIDs[i])", mc.fmtI("Errorf"), mc.goName))
	g.P("    }")
	g.P("  }")
	g.P("  return ordered, nil")
	g.P("}")
	g.P()

	sig := fmt.Sprintf("func (s *Store) BatchInsert%s(ctx context.Context, requestIDs []string, %s []*%s",
		mc.pr.PluralGoName(), mc.pluralParam(), mc.goTypeFqi)
	for _, cc := range mc.singletonChildren {
		sig += fmt.Sprintf(", %s []*%s", cc.pluralParam(), mc.gen.modelIdent(cc.goType))
	}
	sig += fmt.Sprintf(") ([]*%s, error) {", mc.goTypeFqi)
	g.P(sig)

	g.P(fmt.Sprintf("  n := len(%s)", mc.pluralParam()))
	slices := []string{"requestIDs"}
	for _, cc := range mc.singletonChildren {
		slices = append(slices, cc.pluralParam())
	}
	for _, slice := range slices {
		g.P(fmt.Sprintf("  if len(%s) != n {", slice))
		g.P(fmt.Sprintf("    return nil, %s(\"mismatched slice lengths\")", mc.fmtI("Errorf")))
		g.P("  }")
	}
	g.P("  if n == 0 {")
	g.P("    return nil, nil")
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  withRequestIDs := make([]*%s, n)", withRequestID))
	g.P(fmt.Sprintf("  for i, %s := range %s {", mc.goParam, mc.pluralParam()))
	g.P(fmt.Sprintf("    withRequestIDs[i] = &%s{RequestID: requestIDs[i], %s: *%s}", withRequestID, mc.goType, mc.goParam))
	g.P("  }")
	if mc.hasJoins {
		g.P(fmt.Sprintf("  query, params := %s(%sInsertPostgresQuery, withRequestIDs, %s...)", batchInsertQuery, mc.goName, mc.withRequestIDWriteColumns()))
	} else {
		g.P(fmt.Sprintf("  query, params := %s(%sInsertPostgresQuery, withRequestIDs)", batchInsertQuery, mc.goName))
	}
	for i, cc := range mc.singletonChildren {
		idx := i + 2
		if cc.writeColumnsVar != "" {
			g.P(fmt.Sprintf("  query%d, params%d := %s(%sInsertSingletonPostgresQuery, %s, %s...)", idx, idx, batchInsertQuery, cc.goType, cc.pluralParam(), cc.writeColumnsVar))
		} else {
			g.P(fmt.Sprintf("  query%d, params%d := %s(%sInsertSingletonPostgresQuery, %s)", idx, idx, batchInsertQuery, cc.goType, cc.pluralParam()))
		}
	}
	g.P()

	g.P(fmt.Sprintf("  var inserted []*%s", mc.goTypeFqi))
	g.P(fmt.Sprintf("  transactionFN := func(tx %s) error {", mc.postgres("Tx")))
	g.P("    inserted = nil")
	g.P("    rows, err := tx.Query(ctx, query, params...)")
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P(fmt.Sprintf("    upserted, err := %s(rows, %s[%s])", collectRows, rowToAddrLax, withRequestID))
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P(fmt.Sprintf("    inserted, err = %s(requestIDs, upserted)", orderFn))
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P()
	for i := range mc.singletonChildren {
		g.P(fmt.Sprintf("    if _, err := tx.Exec(ctx, query%d, params%d...); err != nil {", i+2, i+2))
		g.P("      return err")
		g.P("    }")
	}
	g.P("    return nil")
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  if err := s.client.ExecuteTransaction(ctx, %s, transactionFN); err != nil {", mc.postgres("ReadCommitted")))
	g.P("    // A replay with server-generated ids collides on request_id rather than")
	g.P("    // on the primary key; return the committed batch if it is whole.")
	g.P(fmt.Sprintf("    if %s(err) {", mc.postgres("IsUniqueViolation")))
	g.P(fmt.Sprintf("      rows, lookupErr := s.client.Query(ctx, %sGetByRequestIDsQuery, requestIDs)", mc.goName))
	g.P("      if lookupErr != nil {")
	g.P("        return nil, lookupErr")
	g.P("      }")
	g.P(fmt.Sprintf("      existing, lookupErr := %s(rows, %s[%s])", collectRows, rowToAddrLax, withRequestID))
	g.P("      if lookupErr != nil {")
	g.P("        return nil, lookupErr")
	g.P("      }")
	g.P("      if len(existing) == n {")
	g.P(fmt.Sprintf("        return %s(requestIDs, existing)", orderFn))
	g.P("      }")
	g.P("      // Not a whole replay: another unique constraint of the table fired.")
	g.P(fmt.Sprintf("      return nil, %s", mc.errAlreadyExists))
	g.P("    }")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return inserted, nil")
	g.P("}")
	g.P()
}

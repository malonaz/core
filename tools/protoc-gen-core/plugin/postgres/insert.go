package postgres

import (
	"fmt"
)

func (mc *msgCtx) generateInsertVars() {
	g := mc.g

	if mc.singleton {
		g.P(fmt.Sprintf("const %sInsertSingletonPostgresQuery = `INSERT INTO %s %%s VALUES %%s ON CONFLICT(%s) DO NOTHING`", mc.goType, mc.tableName, mc.columnNames))
		g.P()
	}

	writeColumns := mc.writeColumns()

	g.P("var (")
	g.P(fmt.Sprintf("  %sWithRequestIDPostgresColumns = %s(%sWithRequestID{})", mc.goType, mc.postgres("GetDBColumns"), mc.goType))
	if mc.hasJoins {
		exceptArgs := mc.exceptColumnsArgs()
		g.P(fmt.Sprintf("  %sWithRequestIDWritePostgresColumns = %s(%sWithRequestID{}, %s(%s))",
			mc.goType, mc.postgres("GetDBColumns"), mc.goType, mc.postgres("ExceptColumns"), exceptArgs))
	}
	g.P(fmt.Sprintf("  _%sInsertPostgresQuery = `INSERT INTO %s %%s VALUES %%s ON CONFLICT(%s) DO UPDATE SET %s = EXCLUDED.%s RETURNING `",
		mc.goName, mc.tableName, mc.columnNames, mc.identifier, mc.identifier))

	withReqIDWriteCols := mc.goType + "WithRequestIDPostgresColumns"
	if mc.hasJoins {
		withReqIDWriteCols = mc.goType + "WithRequestIDWritePostgresColumns"
	}
	withReqIDReturning := mc.returningExpr(withReqIDWriteCols)
	g.P(fmt.Sprintf("  %sWithRequestIDInsertPostgresQuery = _%sInsertPostgresQuery + %s",
		mc.goName, mc.goName, withReqIDReturning))

	returning := mc.returningExpr(writeColumns)
	g.P(fmt.Sprintf("  %sInsertPostgresQuery = _%sInsertPostgresQuery + %s",
		mc.goName, mc.goName, returning))

	g.P(")")
	g.P()
}

func (mc *msgCtx) generateSingletonInsertQuery(idx int, cc *childCtx) {
	g := mc.g
	insertQuery := mc.postgres("InsertQuery")
	if cc.writeColumnsVar != "" {
		g.P(fmt.Sprintf("  query%d, params%d := %s(%sInsertSingletonPostgresQuery, %s, %s...)", idx, idx, insertQuery, cc.goType, cc.paramName, cc.writeColumnsVar))
	} else {
		g.P(fmt.Sprintf("  query%d, params%d := %s(%sInsertSingletonPostgresQuery, %s)", idx, idx, insertQuery, cc.goType, cc.paramName))
	}
}

func (mc *msgCtx) generateInsert() {
	g := mc.g
	insertQuery := mc.postgres("InsertQuery")

	sig := fmt.Sprintf("func (s *Store) Insert%s(ctx context.Context, %s *%s", mc.goType, mc.goParam, mc.goTypeFqi)
	for _, cc := range mc.singletonChildren {
		sig += fmt.Sprintf(", %s *%s", cc.paramName, mc.gen.modelIdent(cc.goType))
	}
	sig += fmt.Sprintf(") (*%s, error) {", mc.goTypeFqi)
	g.P(sig)

	if mc.hasJoins {
		g.P(fmt.Sprintf("  query, params := %s(%sInsertPostgresQuery, %s, %s...)", insertQuery, mc.goName, mc.goParam, mc.writeColumns()))
	} else {
		g.P(fmt.Sprintf("  query, params := %s(%sInsertPostgresQuery, %s)", insertQuery, mc.goName, mc.goParam))
	}
	for i, cc := range mc.singletonChildren {
		mc.generateSingletonInsertQuery(i+2, cc)
	}
	g.P()

	if len(mc.singletonChildren) == 0 {
		g.P("  rows, err := s.client.Query(ctx, query, params...)")
		g.P("  if err != nil {")
		g.P("    return nil, err")
		g.P("  }")
		g.P(fmt.Sprintf("  row, err := %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
		g.P("  if err != nil {")
		g.P("    return nil, err")
		g.P("  }")
		g.P("  return row, nil")
	} else {
		mc.generateInsertWithTransaction()
	}

	g.P("}")
	g.P()
}

func (mc *msgCtx) generateInsertWithTransaction() {
	g := mc.g
	g.P(fmt.Sprintf("  var inserted *%s", mc.goTypeFqi))
	g.P(fmt.Sprintf("  transactionFN := func(tx %s) error {", mc.postgres("Tx")))
	g.P("    inserted = nil")
	g.P("    rows, err := tx.Query(ctx, query, params...)")
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P(fmt.Sprintf("    inserted, err = %s(rows, %s[%s])", mc.pgx("CollectOneRow"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
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
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return inserted, nil")
}

func (mc *msgCtx) generateWithRequestIDStruct() {
	g := mc.g
	g.P(fmt.Sprintf("type %sWithRequestID struct {", mc.goType))
	g.P("  RequestID string `db:\"request_id\"`")
	g.P(fmt.Sprintf("  %s", mc.goTypeFqi))
	g.P("}")
	g.P()

	if mc.hasJoins {
		g.P(fmt.Sprintf("var %sGetByRequestIDQuery = %s(`SELECT %%s FROM %s ` + %sJoinClause + ` WHERE %s.request_id = $1`, %s(%s, %q) + %sJoinSelectExprs)",
			mc.goName, mc.fmtI("Sprintf"), mc.tableName, mc.goName, mc.bareTableName, mc.postgres("QualifyColumns"), mc.writeColumns(), mc.bareTableName, mc.goName))
	} else {
		g.P(fmt.Sprintf("var %sGetByRequestIDQuery = `SELECT ` + %s(\"%%s\", %sPostgresColumns) + ` FROM %s WHERE request_id = $1`",
			mc.goName, mc.postgres("SelectQuery"), mc.goType, mc.tableName))
	}
	g.P()
}

func (mc *msgCtx) generateInsertIdempotently() {
	g := mc.g
	insertQuery := mc.postgres("InsertQuery")
	collectOneRow := mc.pgx("CollectOneRow")
	rowToAddrLax := mc.pgx("RowToAddrOfStructByNameLax")

	withReqIDWriteCols := mc.goType + "WithRequestIDPostgresColumns"
	if mc.hasJoins {
		withReqIDWriteCols = mc.goType + "WithRequestIDWritePostgresColumns"
	}

	sig := fmt.Sprintf("func (s *Store) Insert%sIdempotently(ctx context.Context, requestID string, raw%s *%s",
		mc.goType, title(mc.goParam), mc.goTypeFqi)
	for _, cc := range mc.singletonChildren {
		sig += fmt.Sprintf(", %s *%s", cc.paramName, mc.gen.modelIdent(cc.goType))
	}
	sig += fmt.Sprintf(") (*%s, error) {", mc.goTypeFqi)
	g.P(sig)

	g.P(fmt.Sprintf("  %s := &%sWithRequestID{", mc.goParam, mc.goType))
	g.P("    RequestID: requestID,")
	g.P(fmt.Sprintf("    %s: *raw%s,", mc.goType, title(mc.goParam)))
	g.P("  }")
	if mc.hasJoins {
		g.P(fmt.Sprintf("  query, params := %s(%sWithRequestIDInsertPostgresQuery, %s, %s...)", insertQuery, mc.goName, mc.goParam, withReqIDWriteCols))
	} else {
		g.P(fmt.Sprintf("  query, params := %s(%sWithRequestIDInsertPostgresQuery, %s)", insertQuery, mc.goName, mc.goParam))
	}

	for i, cc := range mc.singletonChildren {
		mc.generateSingletonInsertQuery(i+2, cc)
	}
	g.P()

	g.P(fmt.Sprintf("  var inserted *%s", mc.goTypeFqi))
	g.P(fmt.Sprintf("  transactionFN := func(tx %s) error {", mc.postgres("Tx")))
	g.P("    inserted = nil")
	g.P("    rows, err := tx.Query(ctx, query, params...)")
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P(fmt.Sprintf("    row, err := %s(rows, %s[%sWithRequestID])", collectOneRow, rowToAddrLax, mc.goType))
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P("    if row.RequestID != requestID {")
	g.P(fmt.Sprintf("      return %s", mc.errAlreadyExists))
	g.P("    }")
	g.P(fmt.Sprintf("    inserted = &row.%s", mc.goType))
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
	g.P(fmt.Sprintf("    if %s(err) {", mc.postgres("IsUniqueViolation")))
	g.P(fmt.Sprintf("      rows, err := s.client.Query(ctx, %sGetByRequestIDQuery, requestID)", mc.goName))
	g.P("      if err != nil {")
	g.P("        return nil, err")
	g.P("      }")
	g.P(fmt.Sprintf("      existing, lookupErr := %s(rows, %s[%s])", collectOneRow, rowToAddrLax, mc.goTypeFqi))
	g.P("      if lookupErr == nil {")
	g.P("        return existing, nil")
	g.P("      }")
	g.P("    }")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return inserted, nil")
	g.P("}")
	g.P()
}

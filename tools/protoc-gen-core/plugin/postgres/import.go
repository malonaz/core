package postgres

import (
	"fmt"

	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

// generateImport emits the bulk-load path of a resource: a pgx CopyFrom of
// every row into the resource's table, plus one row per singleton child.
//
// CopyFrom speaks the postgres COPY protocol, so it has no ON CONFLICT clause
// and returns no rows: an identity collision surfaces as a unique violation,
// which we map to the resource's AlreadyExists error. Callers that need the
// stored rows back must read them.
//
// Two variants are emitted, mirroring Insert/InsertIdempotently: the plain one
// writes the model's own columns, and the WithRequestIDs one also writes the
// per-row request_id that idempotent creates rely on.
func (mc *msgCtx) generateImport() {
	// A singleton has no identifier of its own, so a batch of them is not
	// addressable; they are imported alongside their parent instead.
	if mc.singleton {
		return
	}
	mc.generateImportFunc(false)
	mc.generateImportFunc(true)
}

func (mc *msgCtx) generateImportFunc(withRequestIDs bool) {
	g := mc.g
	plural := mc.pr.PluralGoName()
	param := untitle(plural)

	name := "Import" + plural
	if withRequestIDs {
		name += "WithRequestIDs"
	}

	sig := fmt.Sprintf("func (s *Store) %s(ctx context.Context, ", name)
	if withRequestIDs {
		sig += "requestIDs []string, "
	}
	sig += fmt.Sprintf("%s []*%s", param, mc.goTypeFqi)
	for _, cc := range mc.singletonChildren {
		sig += fmt.Sprintf(", %ss []*%s", cc.paramName, mc.gen.modelIdent(cc.goType))
	}
	sig += ") (int64, error) {"
	g.P(sig)

	g.P(fmt.Sprintf("  if len(%s) == 0 {", param))
	g.P("    return 0, nil")
	g.P("  }")
	if withRequestIDs {
		g.P(fmt.Sprintf("  if len(requestIDs) != len(%s) {", param))
		g.P(fmt.Sprintf("    return 0, %s(\"mismatched slice lengths\")", mc.fmtI("Errorf")))
		g.P("  }")
	}
	for _, cc := range mc.singletonChildren {
		g.P(fmt.Sprintf("  if len(%ss) != len(%s) {", cc.paramName, param))
		g.P(fmt.Sprintf("    return 0, %s(\"mismatched slice lengths\")", mc.fmtI("Errorf")))
		g.P("  }")
	}
	g.P()

	// Rows are copied as the model itself, or wrapped to carry a request_id.
	rowsVar := param
	rowsColumns := mc.writeColumns()
	if withRequestIDs {
		rowsVar = "rows"
		rowsColumns = mc.goType + "WithRequestIDPostgresColumns"
		if mc.hasJoins {
			rowsColumns = mc.goType + "WithRequestIDWritePostgresColumns"
		}
		g.P(fmt.Sprintf("  rows := make([]*%sWithRequestID, len(%s))", mc.goType, param))
		g.P(fmt.Sprintf("  for i, %s := range %s {", mc.goParam, param))
		g.P(fmt.Sprintf("    rows[i] = &%sWithRequestID{", mc.goType))
		g.P("      RequestID: requestIDs[i],")
		g.P(fmt.Sprintf("      %s: *%s,", mc.goType, mc.goParam))
		g.P("    }")
		g.P("  }")
		g.P()
	}

	// A resource with singleton children must land both tables atomically.
	if len(mc.singletonChildren) == 0 {
		g.P("  copied, err := s.client.CopyFrom(")
		mc.emitCopyFromArgs("  ", mc.table, rowsColumns, rowsVar)
		g.P("  )")
		g.P("  if err != nil {")
		g.P(fmt.Sprintf("    if %s(err) {", mc.postgres("IsUniqueViolation")))
		g.P(fmt.Sprintf("      return 0, %s", mc.errAlreadyExists))
		g.P("    }")
		g.P("    return 0, err")
		g.P("  }")
		g.P("  return copied, nil")
		g.P("}")
		g.P()
		return
	}

	g.P("  var copied int64")
	g.P(fmt.Sprintf("  transactionFN := func(tx %s) error {", mc.postgres("Tx")))
	g.P("    copied = 0")
	g.P("    n, err := tx.CopyFrom(")
	mc.emitCopyFromArgs("    ", mc.table, rowsColumns, rowsVar)
	g.P("    )")
	g.P("    if err != nil {")
	g.P("      return err")
	g.P("    }")
	g.P("    copied = n")
	g.P()
	for _, cc := range mc.singletonChildren {
		childColumns := cc.writeColumnsVar
		if childColumns == "" {
			childColumns = cc.goType + "PostgresColumns"
		}
		g.P("    if _, err := tx.CopyFrom(")
		mc.emitCopyFromArgs("    ", cc.table, childColumns, cc.paramName+"s")
		g.P("    ); err != nil {")
		g.P("      return err")
		g.P("    }")
	}
	g.P("    return nil")
	g.P("  }")
	g.P()
	g.P(fmt.Sprintf("  if err := s.client.ExecuteTransaction(ctx, %s, transactionFN); err != nil {", mc.postgres("ReadCommitted")))
	g.P(fmt.Sprintf("    if %s(err) {", mc.postgres("IsUniqueViolation")))
	g.P(fmt.Sprintf("      return 0, %s", mc.errAlreadyExists))
	g.P("    }")
	g.P("    return 0, err")
	g.P("  }")
	g.P("  return copied, nil")
	g.P("}")
	g.P()
}

// emitCopyFromArgs emits the destination, column list and row source of a
// CopyFrom call, one argument per line. An unqualified table stays unqualified,
// so it resolves through search_path like every other query we emit.
func (mc *msgCtx) emitCopyFromArgs(indent string, table schema.Table, columnsVar, rowsVar string) {
	g := mc.g
	identifier := fmt.Sprintf("%s{%q}", mc.pgx("Identifier"), table.Name)
	if table.Schema != "" {
		identifier = fmt.Sprintf("%s{%q, %q}", mc.pgx("Identifier"), table.Schema, table.Name)
	}
	g.P(indent, "  ctx,")
	g.P(indent, "  ", identifier, ",")
	g.P(indent, "  ", columnsVar, ",")
	g.P(indent, "  ", mc.pgx("CopyFromSlice"), "(len(", rowsVar, "), func(i int) ([]any, error) {")
	g.P(indent, "    return ", mc.postgres("GetParams"), "(", rowsVar, "[i], ", columnsVar, "...), nil")
	g.P(indent, "  }),")
}

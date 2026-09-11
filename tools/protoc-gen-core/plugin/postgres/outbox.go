package postgres

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"

	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

var (
	outboxPkg = protogen.GoImportPath("github.com/malonaz/core/go/outbox")
	aipGenPkg = protogen.GoImportPath("github.com/malonaz/core/genproto/aip/v1")
)

// generateJournal emits a schema's journal: the table its outbox resources
// write their events to, and the reads the relay drains it with. One journal
// serves every resource of the schema, so it is emitted once per schema.
func (gen *generator) generateJournal(table schema.Table) {
	g := gen.g
	schemaName := table.SchemaOrPublic()
	tableVar := schema.JournalTableVar(schemaName)
	entry := gen.ident(outboxPkg, "Entry")

	g.P(fmt.Sprintf("// %s is the outbox journal of the %s schema: one row per event a write", tableVar, schemaName))
	g.P("// journaled, cleared once the relay has handed it to the scheduler.")
	g.P(fmt.Sprintf("const %s = \"%s\"", tableVar, schema.Journal(table).Qualified()))
	g.P()

	listFN := schema.JournalListFN(schemaName)
	g.P(fmt.Sprintf("// %s returns the oldest undelivered entries of the %s journal.", listFN, schemaName))
	g.P(fmt.Sprintf("func (s *Store) %s(ctx context.Context, limit int) ([]*%s, error) {", listFN, entry))
	g.P(fmt.Sprintf("  return %s(ctx, s.client, %s, limit)", gen.ident(outboxPkg, "List"), tableVar))
	g.P("}")
	g.P()

	writeFN := schema.JournalWriteFN(schemaName)
	g.P(fmt.Sprintf("// %s journals events about rows that are already committed,", writeFN))
	g.P("// which a write's own transaction is not there to carry.")
	g.P(fmt.Sprintf("func (s *Store) %s(ctx context.Context, events []*%s) error {", writeFN, gen.ident(aipGenPkg, "ResourceEvent")))
	g.P(fmt.Sprintf("  return %s(ctx, s.client, %s, events)", gen.ident(outboxPkg, "Write"), tableVar))
	g.P("}")
	g.P()

	deleteFN := schema.JournalDeleteFN(schemaName)
	g.P(fmt.Sprintf("// %s clears the entries the relay has delivered.", deleteFN))
	g.P(fmt.Sprintf("func (s *Store) %s(ctx context.Context, ids []string) error {", deleteFN))
	g.P(fmt.Sprintf("  return %s(ctx, s.client, %s, ids)", gen.ident(outboxPkg, "Delete"), tableVar))
	g.P("}")
	g.P()
}

// journalParam is the trailing parameter of an outbox resource's writes: the
// events to journal, built from the rows the write committed so that they
// carry the resource as stored.
func (mc *msgCtx) journalParam() string {
	if !mc.outbox {
		return ""
	}
	return fmt.Sprintf(", journal %s[*%s]", mc.gen.ident(outboxPkg, "EventFn"), mc.goTypeFqi)
}

// emitJournalWrite emits, inside a transaction, the journaling of the events
// built from the rows the expression yields.
func (mc *msgCtx) emitJournalWrite(indent, rowsExpr string) {
	if !mc.outbox {
		return
	}
	g := mc.g
	g.P(fmt.Sprintf("%sevents, err := journal(%s)", indent, rowsExpr))
	g.P(fmt.Sprintf("%sif err != nil {", indent))
	g.P(fmt.Sprintf("%s  return err", indent))
	g.P(fmt.Sprintf("%s}", indent))
	g.P(fmt.Sprintf("%sif err := %s(ctx, tx, %s, events); err != nil {",
		indent, mc.gen.ident(outboxPkg, "Write"), schema.JournalTableVar(mc.schemaName)))
	g.P(fmt.Sprintf("%s  return err", indent))
	g.P(fmt.Sprintf("%s}", indent))
}

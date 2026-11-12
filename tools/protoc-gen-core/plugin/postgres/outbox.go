package postgres

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"

	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

var outboxPkg = protogen.GoImportPath("github.com/malonaz/core/go/outbox")

// generateJournal emits a schema's journal: the table its outbox resources
// publish their events to, and the claim its poller drains it with. One
// journal serves every resource of the schema, so it is emitted once per schema.
func (gen *generator) generateJournal(table schema.Table) {
	g := gen.g
	schemaName := table.SchemaOrPublic()
	tableVar := schema.JournalTableVar(schemaName)

	g.P(fmt.Sprintf("// %s is the outbox journal of the %s schema: one row per message a", tableVar, schemaName))
	g.P("// write published, deleted once the poller has published it to NATS.")
	g.P(fmt.Sprintf("const %s = \"%s\"", tableVar, schema.Journal(table).Qualified()))
	g.P()

	journalFN := schema.JournalFN(schemaName)
	g.P(fmt.Sprintf("// %s publishes to the %s journal outside any write's transaction,", journalFN, schemaName))
	g.P("// for events about rows that are already committed.")
	g.P(fmt.Sprintf("func (s *Store) %s() *%s {", journalFN, gen.ident(outboxPkg, "Journal")))
	g.P(fmt.Sprintf("  return %s(s.client, %s)", gen.ident(outboxPkg, "NewJournal"), tableVar))
	g.P("}")
	g.P()

	claimFN := schema.JournalClaimFN(schemaName)
	g.P(fmt.Sprintf("// %s claims the oldest entries of the %s journal and deletes the ones publish published.", claimFN, schemaName))
	g.P(fmt.Sprintf("func (s *Store) %s(ctx context.Context, limit int, publish %s) (int, error) {", claimFN, gen.ident(outboxPkg, "PublishFn")))
	g.P(fmt.Sprintf("  return %s(ctx, s.client, %s, limit, publish)", gen.ident(outboxPkg, "Claim"), tableVar))
	g.P("}")
	g.P()
}

// journalParam is the trailing parameter of an outbox resource's writes: the
// publisher of the events of the rows the write committed. Nil publishes none.
func (mc *msgCtx) journalParam() string {
	if !mc.outbox {
		return ""
	}
	return fmt.Sprintf(", journal %s[*%s]", mc.gen.ident(outboxPkg, "JournalFn"), mc.goTypeFqi)
}

// emitJournalWrite emits, inside a transaction, the publishing of the events
// of the rows the expression yields to the journal.
func (mc *msgCtx) emitJournalWrite(indent, rowsExpr string) {
	if !mc.outbox {
		return
	}
	g := mc.g
	g.P(fmt.Sprintf("%sif journal != nil {", indent))
	g.P(fmt.Sprintf("%s  if err := journal(ctx, %s(tx, %s), %s); err != nil {",
		indent, mc.gen.ident(outboxPkg, "NewJournal"), schema.JournalTableVar(mc.schemaName), rowsExpr))
	g.P(fmt.Sprintf("%s    return err", indent))
	g.P(fmt.Sprintf("%s  }", indent))
	g.P(fmt.Sprintf("%s}", indent))
}

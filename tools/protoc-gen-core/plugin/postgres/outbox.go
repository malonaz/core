package postgres

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"
)

var outboxPkg = protogen.GoImportPath("github.com/malonaz/core/go/outbox")

// generateJournal emits the store's access to its database's outbox journal:
// the publisher of events about rows already committed, and the claim the
// poller drains it with.
func (gen *generator) generateJournal() {
	g := gen.g
	g.P("// Journal publishes to the outbox journal outside any write's transaction,")
	g.P("// for events about rows that are already committed.")
	g.P(fmt.Sprintf("func (s *Store) Journal() *%s {", gen.ident(outboxPkg, "Journal")))
	g.P(fmt.Sprintf("  return %s(s.client)", gen.ident(outboxPkg, "NewJournal")))
	g.P("}")
	g.P()

	g.P("// ClaimJournal claims the oldest entries of the outbox journal and deletes the ones publish published.")
	g.P(fmt.Sprintf("func (s *Store) ClaimJournal(ctx context.Context, limit int, publish %s) (int, error) {", gen.ident(outboxPkg, "PublishFn")))
	g.P(fmt.Sprintf("  return %s(ctx, s.client, limit, publish)", gen.ident(outboxPkg, "Claim")))
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
	g.P(fmt.Sprintf("%s  if err := journal(ctx, %s(tx), %s); err != nil {",
		indent, mc.gen.ident(outboxPkg, "NewJournal"), rowsExpr))
	g.P(fmt.Sprintf("%s    return err", indent))
	g.P(fmt.Sprintf("%s  }", indent))
	g.P(fmt.Sprintf("%s}", indent))
}

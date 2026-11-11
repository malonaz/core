package rpc

import (
	"errors"
	"fmt"
	"slices"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"

	codegennatspb "github.com/malonaz/core/genproto/codegen/nats/v1"
	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/tools/protoc-gen-core/goident"
	"github.com/malonaz/core/tools/protoc-gen-core/resource"
	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

var (
	outboxPkg      = protogen.GoImportPath("github.com/malonaz/core/go/outbox")
	fieldmaskpbPkg = protogen.GoImportPath("google.golang.org/protobuf/types/known/fieldmaskpb")
)

// schemaOf returns the database schema a resource's table, and so its journal, lives in.
func schemaOf(message *protogen.Message, pr *resource.ParsedResource) (string, error) {
	modelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](message.Desc.Options(), modelpb.E_ModelOpts)
	if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
		return "", fmt.Errorf("getting model_opts for %s: %w", message.GoIdent.GoName, err)
	}
	return schema.TableOf(pr, modelOpts).SchemaOrPublic(), nil
}

// pollerName names a schema's poller routine, for its logs and metrics.
func pollerName(si *serviceInfo, schemaName string) string {
	return xstrings.ToKebabCase(si.service.GoName) + "-" + schemaName + "-outbox"
}

// sortedSchemas returns the distinct schemas of a service's outbox resources.
func sortedSchemas(schemaSet map[string]bool) []string {
	names := make([]string, 0, len(schemaSet))
	for name := range schemaSet {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// generateOutboxStoreInterface emits the claims the service's pollers drain its journals with.
func (gen *generator) generateOutboxStoreInterface(si *serviceInfo) {
	for _, schemaName := range si.outboxSchemas {
		gen.g.P(fmt.Sprintf("  %s(ctx %s, limit int, publish %s) (int, error)",
			schema.JournalClaimFN(schemaName), gen.ident(contextPkg, "Context"), gen.ident(outboxPkg, "PublishFn")))
	}
}

// generateOutboxPollers emits, inside the server's Start, one poller per journal the service writes to.
func (gen *generator) generateOutboxPollers(si *serviceInfo) {
	if !si.outbox() {
		return
	}
	g := gen.g
	g.P(fmt.Sprintf("  s.outboxPollers = []*%s{", gen.ident(outboxPkg, "Poller")))
	for _, schemaName := range si.outboxSchemas {
		g.P(fmt.Sprintf("    %s(%s{", gen.ident(outboxPkg, "NewPoller"), gen.ident(outboxPkg, "PollerOpts")))
		g.P(fmt.Sprintf("      Name: \"%s\",", pollerName(si, schemaName)))
		g.P(fmt.Sprintf("      Claim: s.store.%s,", schema.JournalClaimFN(schemaName)))
		g.P("    }, s.natsClient).Start(ctx),")
	}
	g.P("  }")
}

// generateClose emits Close, which stops the server's pollers. It is emitted
// whether or not the service has any, so the hosting service can always call it.
func (gen *generator) generateClose(si *serviceInfo) {
	g := gen.g
	g.P(fmt.Sprintf("// Close stops the %s's background work.", si.service.GoName))
	g.P(fmt.Sprintf("func (s *%sServer) Close() {", si.service.GoName))
	if si.outbox() {
		g.P("  for _, poller := range s.outboxPollers {")
		g.P("    poller.Close()")
		g.P("  }")
	}
	g.P("}")
	g.P()
}

// outboxEvent is one event type an outbox resource may declare.
type outboxEvent struct {
	kind      string
	eventOpts func(*codegennatspb.EventOptions) []*codegennatspb.EventMethodOptions
}

var outboxEvents = []outboxEvent{
	{"Created", (*codegennatspb.EventOptions).GetCreated},
	{"Updated", (*codegennatspb.EventOptions).GetUpdated},
	{"Deleted", (*codegennatspb.EventOptions).GetDeleted},
	{"Undeleted", (*codegennatspb.EventOptions).GetUndeleted},
	{"Imported", (*codegennatspb.EventOptions).GetImported},
}

// generateOutboxResourceLevel emits, per event type the resource declares, the
// publisher of its subjects and the journal fn its writes hand their store.
func (mc *methodCtx) generateOutboxResourceLevel() {
	if !mc.outbox {
		return
	}
	for _, event := range outboxEvents {
		eventOpts := event.eventOpts(mc.mi.natsEventOpts)
		if len(eventOpts) == 0 {
			continue
		}
		mc.generateOutboxPublisher(event.kind, eventOpts)
		mc.generateJournalFN(event.kind)
	}
}

// updatedParams are what an updated event needs beyond the resource.
func (mc *methodCtx) updatedParams() string {
	return fmt.Sprintf(", previous%s *%s, updateMask *%s", mc.resourceGoName, mc.protoType(), mc.gen.ident(fieldmaskpbPkg, "FieldMask"))
}

// outboxPublisherFN names the publisher of one event type's subjects.
func (mc *methodCtx) outboxPublisherFN(kind string) string {
	return "publish" + mc.resourceGoName + kind + "Events"
}

// generateOutboxPublisher emits the publishing of one event type's subjects through a publisher.
func (mc *methodCtx) generateOutboxPublisher(kind string, eventOpts []*codegennatspb.EventMethodOptions) {
	g := mc.g
	resourceVar := goident.Var(mc.resourceGoName)
	extraParams, extraArgs := "", ""
	if kind == "Updated" {
		extraParams = mc.updatedParams()
		extraArgs = fmt.Sprintf(", previous%s, updateMask", mc.resourceGoName)
	}

	g.P(fmt.Sprintf("// %s publishes the %s %s events through the publisher.", mc.outboxPublisherFN(kind), mc.pr.Desc.Singular, xstrings.ToSnakeCase(kind)))
	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, publisher %s, %s *%s%s) error {",
		mc.serverGoName, mc.outboxPublisherFN(kind), mc.gen.ident(contextPkg, "Context"), mc.gen.ident(natsPkg, "Publisher"),
		resourceVar, mc.protoType(), extraParams))
	for _, eventOpt := range eventOpts {
		subject := eventOpt.GetSubject()
		g.P("  {")
		g.P(fmt.Sprintf("    subject := %s().Get%sSubject()",
			mc.gen.resourcePkgIdent(mc.mi.rpc.Message, "Get"+mc.natsStreamGoName), xstrings.ToPascalCase(subject)))
		g.P(fmt.Sprintf("    if err := subject.Publish(ctx, publisher, %s%s); err != nil {", resourceVar, extraArgs))
		g.P(fmt.Sprintf("      return %s(%s, \"journaling %s event: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), subject))
		g.P("    }")
		g.P("  }")
	}
	g.P("  return nil")
	g.P("}")
	g.P()
}

// journalFN names the journal fn of one event type.
func (mc *methodCtx) journalFN(kind string) string {
	return goident.Var(mc.resourceGoName) + kind + "Journal"
}

// generateJournalFN emits the journal fn a write hands its store: it publishes
// the events of the rows the write committed, inside its transaction. An
// updated event's fn is built over the update's previous resource and mask.
func (mc *methodCtx) generateJournalFN(kind string) {
	g := mc.g
	resourceVar := goident.Var(mc.resourceGoName)
	journalFn := fmt.Sprintf("%s[*%s]", mc.gen.ident(outboxPkg, "JournalFn"), mc.goTypeQgi)
	body := fmt.Sprintf("(ctx %s, publisher %s, rows []*%s) error {",
		mc.gen.ident(contextPkg, "Context"), mc.gen.ident(natsPkg, "Publisher"), mc.goTypeQgi)
	extraArgs, indent := "", ""

	g.P(fmt.Sprintf("// %s journals the %s events of a write's committed rows.", mc.journalFN(kind), xstrings.ToSnakeCase(kind)))
	if kind == "Updated" {
		g.P(fmt.Sprintf("func (s *%s) %s(%s) %s {", mc.serverGoName, mc.journalFN(kind), mc.updatedParams()[2:], journalFn))
		g.P("  return func" + body)
		extraArgs = fmt.Sprintf(", previous%s, updateMask", mc.resourceGoName)
		indent = "  "
	} else {
		g.P(fmt.Sprintf("func (s *%s) %s%s", mc.serverGoName, mc.journalFN(kind), body))
	}
	g.P(indent + "  for _, row := range rows {")
	g.P(fmt.Sprintf("%s    %s, err := row.ToPb()", indent, resourceVar))
	g.P(indent + "    if err != nil {")
	g.P(fmt.Sprintf("%s      return %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
		indent, mc.statusErrorf(), mc.codes("Internal"), mc.pr.Desc.Singular))
	g.P(indent + "    }")
	g.P(fmt.Sprintf("%s    if err := s.%s(ctx, publisher, %s%s); err != nil {", indent, mc.outboxPublisherFN(kind), resourceVar, extraArgs))
	g.P(indent + "      return err")
	g.P(indent + "    }")
	g.P(indent + "  }")
	g.P(indent + "  return nil")
	if indent != "" {
		g.P("  }")
	}
	g.P("}")
	g.P()
}

// declares reports whether the resource declares events of the kind.
func (mc *methodCtx) declares(kind string) bool {
	for _, event := range outboxEvents {
		if event.kind == kind {
			return len(event.eventOpts(mc.mi.natsEventOpts)) > 0
		}
	}
	return false
}

// journalParam is the trailing store parameter of an outbox resource's writes.
func (mc *methodCtx) journalParam() string {
	if !mc.outbox {
		return ""
	}
	return fmt.Sprintf(", journal %s[*%s]", mc.gen.ident(outboxPkg, "JournalFn"), mc.goTypeQgi)
}

// journalArg is the journal fn a write hands its store through receiver: nil
// when the resource declares no event of the kind, empty when it does not journal.
func (mc *methodCtx) journalArg(receiver, kind string) string {
	if !mc.outbox {
		return ""
	}
	if !mc.declares(kind) {
		return ", nil"
	}
	return ", " + receiver + "." + mc.journalFN(kind)
}

// updatedJournalArg is the journal fn an update hands its store, over the
// resource it patched and the request's mask.
func (mc *methodCtx) updatedJournalArg(previousVar string) string {
	if !mc.outbox {
		return ""
	}
	if !mc.declares("Updated") {
		return ", nil"
	}
	return fmt.Sprintf(", s.%s(%s, request.GetUpdateMask())", mc.journalFN("Updated"), previousVar)
}

// publishesInline reports whether the RPC publishes events of the kind itself,
// rather than its store journaling them.
func (mc *methodCtx) publishesInline(kind string) bool {
	return !mc.outbox && mc.mi.natsEventOpts != nil && mc.declares(kind)
}

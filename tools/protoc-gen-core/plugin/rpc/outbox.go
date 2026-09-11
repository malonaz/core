package rpc

import (
	"errors"
	"fmt"
	"slices"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	codegennatspb "github.com/malonaz/core/genproto/codegen/nats/v1"
	codegenschedulerpb "github.com/malonaz/core/genproto/codegen/scheduler/v1"
	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	aippb "github.com/malonaz/core/genproto/aip/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/tools/protoc-gen-core/resource"
	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

var (
	outboxPkg         = protogen.GoImportPath("github.com/malonaz/core/go/outbox")
	fieldmaskpbPkg    = protogen.GoImportPath("google.golang.org/protobuf/types/known/fieldmaskpb")
	resourceEventName = (&aippb.ResourceEvent{}).ProtoReflect().Descriptor().FullName()
	emptyFullName     = (&emptypb.Empty{}).ProtoReflect().Descriptor().FullName()
)

// outboxMethod is the RPC the scheduler delivers a service's journaled events
// to. The service declares it; this generator implements it, publishing each
// event's NATS subjects the way an inline publish would have.
type outboxMethod struct {
	method *protogen.Method
	// eventField is the request's malonaz.aip.v1.ResourceEvent field.
	eventField *protogen.Field
}

// outboxDelivery is one resource the service's outbox method dispatches to.
type outboxDelivery struct {
	message   *protogen.Message
	deliverFN string
}

// parseOutboxMethod returns nil when the method is not the service's outbox
// delivery endpoint. One that is must take a single ResourceEvent, return
// google.protobuf.Empty and carry the scheduler's method annotation, which is
// what gives it the queue its jobs are delivered through.
func parseOutboxMethod(method *protogen.Method) (*outboxMethod, error) {
	annotated, err := pbutil.GetExtension[bool](method.Desc.Options(), codegennatspb.E_Outbox)
	if err != nil {
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting outbox extension of %s: %w", method.GoName, err)
	}
	if !annotated {
		return nil, nil
	}
	if method.Output.Desc.FullName() != emptyFullName {
		return nil, fmt.Errorf("%s is an outbox method: it must return %s", method.GoName, emptyFullName)
	}
	if !proto.HasExtension(method.Desc.Options(), codegenschedulerpb.E_Method) {
		return nil, fmt.Errorf("%s is an outbox method: it must declare a (malonaz.codegen.scheduler.v1.method) annotation, which is what the scheduler delivers its jobs through", method.GoName)
	}
	var eventField *protogen.Field
	for _, field := range method.Input.Fields {
		if field.Message == nil || field.Message.Desc.FullName() != resourceEventName {
			continue
		}
		if eventField != nil {
			return nil, fmt.Errorf("%s declares more than one %s field", method.Input.Desc.FullName(), resourceEventName)
		}
		eventField = field
	}
	if eventField == nil {
		return nil, fmt.Errorf("%s is an outbox method: %s must declare a %s field", method.GoName, method.Input.Desc.FullName(), resourceEventName)
	}
	return &outboxMethod{method: method, eventField: eventField}, nil
}

// requireOutboxMethod enforces that a service journaling events declares the
// method they are delivered to, and that one declaring it has something to
// deliver.
func requireOutboxMethod(si *serviceInfo) error {
	if len(si.outboxSchemas) > 0 && si.outboxMethod == nil {
		return fmt.Errorf("%s has resources with (malonaz.codegen.nats.v1.event).outbox but declares no outbox method to deliver them to", si.service.GoName)
	}
	if len(si.outboxSchemas) == 0 && si.outboxMethod != nil {
		return fmt.Errorf("%s declares outbox method %s but no resource of it sets (malonaz.codegen.nats.v1.event).outbox", si.service.GoName, si.outboxMethod.method.GoName)
	}
	return nil
}

// schemaOf returns the database schema a resource's table, and so its journal,
// lives in.
func schemaOf(message *protogen.Message, pr *resource.ParsedResource) (string, error) {
	modelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](message.Desc.Options(), modelpb.E_ModelOpts)
	if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
		return "", fmt.Errorf("getting model_opts for %s: %w", message.GoIdent.GoName, err)
	}
	return schema.TableOf(pr, modelOpts).SchemaOrPublic(), nil
}

// relayName names a schema's relay routine, for its logs and metrics.
func relayName(si *serviceInfo, schemaName string) string {
	return xstrings.ToKebabCase(si.service.GoName) + "-" + schemaName + "-outbox"
}

// generateOutboxServiceLevel emits the store reads the relay drains a journal
// with. The relay itself is started by the server's Start.
func (gen *generator) generateOutboxStoreInterface(si *serviceInfo) {
	g := gen.g
	for _, schemaName := range si.outboxSchemas {
		g.P(fmt.Sprintf("  %s(ctx %s, limit int) ([]*%s, error)",
			schema.JournalListFN(schemaName), gen.ident(contextPkg, "Context"), gen.ident(outboxPkg, "Entry")))
		g.P(fmt.Sprintf("  %s(ctx %s, ids []string) error",
			schema.JournalDeleteFN(schemaName), gen.ident(contextPkg, "Context")))
	}
}

// generateOutboxRelays emits, inside the server's Start, one relay per journal
// the service writes to: it hands each entry to the scheduler as a job of the
// outbox method and clears it once it has.
func (gen *generator) generateOutboxRelays(si *serviceInfo) {
	if si.outboxMethod == nil {
		return
	}
	g := gen.g
	g.P(fmt.Sprintf("  s.outboxRelays = []*%s{", gen.ident(outboxPkg, "Relay")))
	for _, schemaName := range si.outboxSchemas {
		g.P(fmt.Sprintf("    %s(%s{", gen.ident(outboxPkg, "New"), gen.ident(outboxPkg, "Opts")))
		g.P(fmt.Sprintf("      Name: \"%s\",", relayName(si, schemaName)))
		g.P(fmt.Sprintf("      List: s.store.%s,", schema.JournalListFN(schemaName)))
		g.P(fmt.Sprintf("      Delete: s.store.%s,", schema.JournalDeleteFN(schemaName)))
		g.P(fmt.Sprintf("      Payload: func(event *%s) %s {", gen.ident(aipGenPkg, "ResourceEvent"), gen.ident(protoPkg, "Message")))
		g.P(fmt.Sprintf("        return &%s{%s: event}", gen.qgi(si.outboxMethod.method.Input.GoIdent), si.outboxMethod.eventField.GoName))
		g.P("      },")
		g.P("      SchedulerServiceClient: s.schedulerServiceClient,")
		g.P("    }).Start(ctx),")
	}
	g.P("  }")
}

// generateOutboxClose emits Close, which stops the server's relays. It is
// emitted whether or not the service has any, so that the hosting service can
// always call it.
func (gen *generator) generateOutboxClose(si *serviceInfo) {
	g := gen.g
	g.P(fmt.Sprintf("// Close stops the %s's background work.", si.service.GoName))
	g.P(fmt.Sprintf("func (s *%sServer) Close() {", si.service.GoName))
	if si.outboxMethod != nil {
		g.P("  for _, relay := range s.outboxRelays {")
		g.P("    relay.Close()")
		g.P("  }")
	}
	g.P("}")
	g.P()
}

// generateOutboxDelivery emits the service's outbox method: it routes an event
// to the resource it is about, which publishes it.
func (gen *generator) generateOutboxDelivery(si *serviceInfo) {
	if si.outboxMethod == nil {
		return
	}
	g := gen.g
	method := si.outboxMethod.method
	statusErrorf := gen.ident(statusPkg, "Errorf")

	g.P(fmt.Sprintf("// %s publishes one journaled event, called by the scheduler once per", method.GoName))
	g.P("// entry the outbox relay handed it. The event carries the resource as it was")
	g.P("// written, so what is published is the write that happened rather than the")
	g.P("// resource as it stands now.")
	g.P(fmt.Sprintf("func (s *%sServer) %s(ctx %s, request *%s) (*%s, error) {",
		si.service.GoName, method.GoName, gen.ident(contextPkg, "Context"),
		gen.qgi(method.Input.GoIdent), gen.ident(emptypbPkg, "Empty")))
	g.P(fmt.Sprintf("  event := request.Get%s()", si.outboxMethod.eventField.GoName))
	g.P("  switch {")
	for _, delivery := range si.outboxDeliveries {
		g.P(fmt.Sprintf("  case event.GetResource().MessageIs(&%s{}):", gen.qgi(delivery.message.GoIdent)))
		g.P(fmt.Sprintf("    if err := s.%s(ctx, event); err != nil {", delivery.deliverFN))
		g.P("      return nil, err")
		g.P("    }")
	}
	g.P("  default:")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"no outbox resource of type %%q\", event.GetResource().GetTypeUrl()).Err()",
		statusErrorf, gen.ident(codesPkg, "InvalidArgument")))
	g.P("  }")
	g.P(fmt.Sprintf("  return &%s{}, nil", gen.ident(emptypbPkg, "Empty")))
	g.P("}")
	g.P()
}

// deliverFN is the resource's half of the outbox method: the publishes of one
// of its journaled events.
func (mc *methodCtx) deliverFN() string {
	return "deliver" + mc.resourceGoName + "Event"
}

// generateOutboxResourceLevel emits everything an outbox resource needs: the
// builders its writes journal their events with, the publishers of each event
// type, and the delivery the service's outbox method routes into.
func (mc *methodCtx) generateOutboxResourceLevel() {
	if !mc.outbox {
		return
	}
	eventOpts := mc.mi.natsEventOpts
	mc.generateJournalBuilders()
	if len(eventOpts.GetCreated()) > 0 {
		mc.generateEventPublisher("publishResourceCreatedEvent", eventOpts.GetCreated(), "", "")
	}
	if len(eventOpts.GetUpdated()) > 0 {
		mc.generateEventPublisher("publishResourceUpdatedEvent", eventOpts.GetUpdated(),
			fmt.Sprintf(", previous%s *%s, updateMask *%s", mc.resourceGoName, mc.protoType(), mc.gen.ident(fieldmaskpbPkg, "FieldMask")),
			fmt.Sprintf(", previous%s, updateMask", mc.resourceGoName))
	}
	if len(eventOpts.GetDeleted()) > 0 {
		mc.generateEventPublisher("publishResourceDeletedEvent", eventOpts.GetDeleted(), "", "")
		mc.generateDeletedEventJournaler()
	}
	if len(eventOpts.GetUndeleted()) > 0 {
		mc.generateEventPublisher("publishResourceUndeletedEvent", eventOpts.GetUndeleted(), "", "")
	}
	mc.generateDeliver()
}

// generateDeletedEventJournaler emits the journaling of a deleted event about
// a resource that is already a tombstone, which an idempotent delete re-emits
// without a write of its own to ride on.
func (mc *methodCtx) generateDeletedEventJournaler() {
	g := mc.g
	resourceVar := xstrings.ToCamelCase(mc.resourceGoName)

	g.P(fmt.Sprintf("func (s *%s) journalResourceDeletedEvent(ctx %s, %s *%s) error {",
		mc.serverGoName, mc.gen.ident(contextPkg, "Context"), resourceVar, mc.protoType()))
	g.P(fmt.Sprintf("  event, err := %s(%s)", mc.gen.ident(aipPkg, "NewResourceDeletedEvent"), resourceVar))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return %s(%s, \"building %s deleted event: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), mc.pr.Desc.Singular))
	g.P("  }")
	g.P(fmt.Sprintf("  if err := s.store.%s(ctx, []*%s{event}); err != nil {",
		schema.JournalWriteFN(mc.schemaName), mc.gen.ident(aipGenPkg, "ResourceEvent")))
	g.P(fmt.Sprintf("    return %s(err, \"journaling %s deleted event\").Err()", mc.statusFromError(), mc.pr.Desc.Singular))
	g.P("  }")
	g.P("  return nil")
	g.P("}")
	g.P()
}

// generateDeliver emits the resource's delivery: it reads the resource out of
// the event and publishes the subjects of the event's type.
func (mc *methodCtx) generateDeliver() {
	g := mc.g
	eventOpts := mc.mi.natsEventOpts
	resourceVar := xstrings.ToCamelCase(mc.resourceGoName)

	g.P(fmt.Sprintf("// %s publishes one journaled %s event.", mc.deliverFN(), mc.pr.Desc.Singular))
	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, event *%s) error {",
		mc.serverGoName, mc.deliverFN(), mc.gen.ident(contextPkg, "Context"), mc.gen.ident(aipGenPkg, "ResourceEvent")))
	g.P(fmt.Sprintf("  %s, err := %s[*%s](event)", resourceVar, mc.gen.ident(aipPkg, "ParseEventResource"), mc.protoType()))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return %s(%s, \"parsing %s of event: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument"), mc.pr.Desc.Singular))
	g.P("  }")
	g.P("  switch event.GetType() {")
	if len(eventOpts.GetCreated()) > 0 {
		g.P(fmt.Sprintf("  case %s:", mc.eventType("CREATED")))
		g.P(fmt.Sprintf("    return s.publishResourceCreatedEvent(ctx, %s)", resourceVar))
	}
	if len(eventOpts.GetUpdated()) > 0 {
		g.P(fmt.Sprintf("  case %s:", mc.eventType("UPDATED")))
		g.P(fmt.Sprintf("    previous%s, err := %s[*%s](event)",
			mc.resourceGoName, mc.gen.ident(aipPkg, "ParseEventPreviousResource"), mc.protoType()))
		g.P("    if err != nil {")
		g.P(fmt.Sprintf("      return %s(%s, \"parsing previous %s of event: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument"), mc.pr.Desc.Singular))
		g.P("    }")
		g.P(fmt.Sprintf("    return s.publishResourceUpdatedEvent(ctx, %s, previous%s, event.GetUpdateMask())", resourceVar, mc.resourceGoName))
	}
	if len(eventOpts.GetDeleted()) > 0 {
		g.P(fmt.Sprintf("  case %s:", mc.eventType("DELETED")))
		g.P(fmt.Sprintf("    return s.publishResourceDeletedEvent(ctx, %s)", resourceVar))
	}
	if len(eventOpts.GetUndeleted()) > 0 {
		g.P(fmt.Sprintf("  case %s:", mc.eventType("UNDELETED")))
		g.P(fmt.Sprintf("    return s.publishResourceUndeletedEvent(ctx, %s)", resourceVar))
	}
	g.P("  }")
	g.P(fmt.Sprintf("  return %s(%s, \"%s declares no %%s event\", event.GetType()).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument"), mc.pr.Desc.Singular))
	g.P("}")
	g.P()
}

// eventType qualifies a malonaz.aip.v1.ResourceEventType value.
func (mc *methodCtx) eventType(name string) string {
	return mc.gen.ident(aipGenPkg, "ResourceEventType_RESOURCE_EVENT_TYPE_"+name)
}

// generateEventPublisher emits a publisher of one event type: the Publish of
// every subject the resource declares for it.
func (mc *methodCtx) generateEventPublisher(name string, eventOpts []*codegennatspb.EventMethodOptions, extraParams, extraArgs string) {
	g := mc.g
	resourceVar := xstrings.ToCamelCase(mc.resourceGoName)

	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, %s *%s%s) error {",
		mc.serverGoName, name, mc.gen.ident(contextPkg, "Context"), resourceVar, mc.protoType(), extraParams))
	for _, eventOpt := range eventOpts {
		subject := eventOpt.GetSubject()
		g.P("  {")
		g.P(fmt.Sprintf("    subject := %s().Get%sSubject()",
			mc.gen.resourcePkgIdent(mc.mi.rpc.Message, "Get"+mc.natsStreamGoName),
			xstrings.ToPascalCase(subject)))
		g.P(fmt.Sprintf("    if err := subject.Publish(ctx, s.natsClient, %s%s); err != nil {", resourceVar, extraArgs))
		g.P(fmt.Sprintf("      return %s(%s, \"publishing %s event: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), subject))
		g.P("    }")
		g.P("  }")
	}
	g.P("  return nil")
	g.P("}")
	g.P()
}

// journalBuilderFN names the builder of one event type's journal entries.
func (mc *methodCtx) journalBuilderFN(kind string) string {
	return xstrings.ToCamelCase(mc.resourceGoName) + kind + "Events"
}

// generateJournalBuilders emits the builders a write hands its store so that
// its events are journaled in its own transaction, built from the rows it
// committed. A resource declaring no event of a type builds none, which
// journals nothing.
func (mc *methodCtx) generateJournalBuilders() {
	eventOpts := mc.mi.natsEventOpts
	mc.generateJournalBuilder("Created", "NewResourceCreatedEvent", len(eventOpts.GetCreated()) > 0, "", "")
	mc.generateJournalBuilder("Updated", "NewResourceUpdatedEvent", len(eventOpts.GetUpdated()) > 0,
		fmt.Sprintf("previous%s *%s, updateMask *%s", mc.resourceGoName, mc.protoType(), mc.gen.ident(fieldmaskpbPkg, "FieldMask")),
		fmt.Sprintf(", previous%s, updateMask", mc.resourceGoName))
	mc.generateJournalBuilder("Deleted", "NewResourceDeletedEvent", len(eventOpts.GetDeleted()) > 0, "", "")
	mc.generateJournalBuilder("Undeleted", "NewResourceUndeletedEvent", len(eventOpts.GetUndeleted()) > 0, "", "")
}

// generateJournalBuilder emits one builder. With params it is a constructor of
// the builder, closing over what the event needs beyond the committed row.
func (mc *methodCtx) generateJournalBuilder(kind, constructor string, declared bool, params, extraArgs string) {
	g := mc.g
	name := mc.journalBuilderFN(kind)
	eventFn := fmt.Sprintf("%s[*%s]", mc.gen.ident(outboxPkg, "EventFn"), mc.goTypeQgi)
	resourceVar := xstrings.ToCamelCase(mc.resourceGoName)
	indent := ""

	if params != "" {
		g.P(fmt.Sprintf("// %s builds the %s events of a write, over what it is patching.", name, mc.pr.Desc.Singular))
		g.P(fmt.Sprintf("func (s *%s) %s(%s) %s {", mc.serverGoName, name, params, eventFn))
		g.P(fmt.Sprintf("  return func(rows []*%s) ([]*%s, error) {", mc.goTypeQgi, mc.gen.ident(aipGenPkg, "ResourceEvent")))
		indent = "  "
	} else {
		g.P(fmt.Sprintf("// %s builds the %s events of a write.", name, mc.pr.Desc.Singular))
		g.P(fmt.Sprintf("func (s *%s) %s(rows []*%s) ([]*%s, error) {",
			mc.serverGoName, name, mc.goTypeQgi, mc.gen.ident(aipGenPkg, "ResourceEvent")))
	}

	if !declared {
		g.P(fmt.Sprintf("%s  // %s declares no %s event.", indent, mc.pr.Desc.Singular, xstrings.ToSnakeCase(kind)))
		g.P(fmt.Sprintf("%s  return nil, nil", indent))
	} else {
		g.P(fmt.Sprintf("%s  events := make([]*%s, 0, len(rows))", indent, mc.gen.ident(aipGenPkg, "ResourceEvent")))
		g.P(fmt.Sprintf("%s  for _, row := range rows {", indent))
		g.P(fmt.Sprintf("%s    %s, err := row.ToPb()", indent, resourceVar))
		g.P(fmt.Sprintf("%s    if err != nil {", indent))
		g.P(fmt.Sprintf("%s      return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
			indent, mc.statusErrorf(), mc.codes("Internal"), mc.pr.Desc.Singular))
		g.P(fmt.Sprintf("%s    }", indent))
		g.P(fmt.Sprintf("%s    event, err := %s(%s%s)", indent, mc.gen.ident(aipPkg, constructor), resourceVar, extraArgs))
		g.P(fmt.Sprintf("%s    if err != nil {", indent))
		g.P(fmt.Sprintf("%s      return nil, %s(%s, \"building %s %s event: %%v\", err).Err()",
			indent, mc.statusErrorf(), mc.codes("Internal"), mc.pr.Desc.Singular, xstrings.ToSnakeCase(kind)))
		g.P(fmt.Sprintf("%s    }", indent))
		g.P(fmt.Sprintf("%s    events = append(events, event)", indent))
		g.P(fmt.Sprintf("%s  }", indent))
		g.P(fmt.Sprintf("%s  return events, nil", indent))
	}

	if params != "" {
		g.P("  }")
	}
	g.P("}")
	g.P()
}

// journalArg is the builder a write hands its store, empty when the resource
// does not journal.
func (mc *methodCtx) journalArg(kind string) string {
	if !mc.outbox {
		return ""
	}
	return ", s." + mc.journalBuilderFN(kind)
}

// updatedJournalArg is the builder an update hands its store: the previous
// resource and the mask are the update's, not the row's.
func (mc *methodCtx) updatedJournalArg(previousVar string) string {
	if !mc.outbox {
		return ""
	}
	return fmt.Sprintf(", s.%s(%s, request.GetUpdateMask())", mc.journalBuilderFN("Updated"), previousVar)
}

// sortedSchemas returns the distinct schemas of a service's outbox resources.
func sortedSchemas(schemas map[string]bool) []string {
	names := make([]string, 0, len(schemas))
	for name := range schemas {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

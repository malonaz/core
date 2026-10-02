package rpc

import (
	"errors"
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	codegenaippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

const (
	exportMetadataType    = "malonaz.aip.v1.ExportMetadata"
	inlineDestinationName = "InlineDestination"
	// Rows per page of an export; also the granularity of its progress.
	exportPageSize = 500
)

var (
	slicesPkg   = protogen.GoImportPath("slices")
	postgresPkg = protogen.GoImportPath("github.com/malonaz/core/go/postgres")
)

// exportMethod is Export{Plural} (AIP-153) on a resource the service owns: a
// long-running standard method whose request names its destination in a oneof.
// Generated code reads the resources out of the store; the inline destination
// is written by generated code too, every other one by a method of the runner.
type exportMethod struct {
	lro *longrunningMethod
	mi  *methodInfo
	// The `destination` variants: the inline one and, in declaration order, the custom ones.
	inline       *protogen.Field
	destinations []*protogen.Field
	// The request's optional `filter` and `show_deleted` fields.
	filter      *protogen.Field
	showDeleted *protogen.Field
	// The operation's response and its repeated item field.
	response *protogen.Message
	items    *protogen.Field
	// Set when the item is not the resource itself: the runner builds the items
	// out of each page of resources.
	aggregate bool
}

// runnerMethodGoName is the runner method writing to a custom destination, e.g. ExportBooksToCsv.
func (exp *exportMethod) runnerMethodGoName(destination *protogen.Field) string {
	return exp.lro.method.GoName + "To" + strings.TrimSuffix(destination.GoName, "Destination")
}

// aggregateGoName is the runner method building the items of a page, e.g. AggregateExportBooks.
func (exp *exportMethod) aggregateGoName() string { return "Aggregate" + exp.lro.method.GoName }

func (exp *exportMethod) readerGoName() string { return exp.lro.method.GoName + "Reader" }

// parseExportMethod validates the AIP-153 shape of an Export{Plural} method.
func parseExportMethod(gen *generator, lro *longrunningMethod, mi *methodInfo) (*exportMethod, error) {
	if err := checkAIP153Method(lro, mi, "export", exportMetadataType); err != nil {
		return nil, err
	}
	exp := &exportMethod{lro: lro, mi: mi}
	if err := exp.parseRequest(); err != nil {
		return nil, err
	}
	response, err := gen.responseMessage(lro)
	if err != nil {
		return nil, err
	}
	if err := exp.parseResponse(response); err != nil {
		return nil, err
	}
	return exp, nil
}

func (exp *exportMethod) parseRequest() error {
	request := exp.lro.method.Input
	destination, err := requiredOneof(request, "destination", "Destination")
	if err != nil {
		return err
	}
	for _, field := range destination.Fields {
		if field.Message.Desc.Name() != inlineDestinationName {
			exp.destinations = append(exp.destinations, field)
			continue
		}
		if exp.inline != nil {
			return fmt.Errorf("%s.destination declares %s twice", request.GoIdent.GoName, inlineDestinationName)
		}
		if field.Message.Desc.Parent() != request.Desc || len(field.Message.Fields) != 0 {
			return fmt.Errorf("%s.%s must be an empty `message InlineDestination {}` nested in the request (AIP-153)", request.GoIdent.GoName, field.Desc.Name())
		}
		exp.inline = field
	}
	if exp.inline == nil {
		return fmt.Errorf("%s.destination must declare `InlineDestination inline_destination` with an empty `message InlineDestination {}` nested in the request (AIP-153)",
			request.GoIdent.GoName)
	}

	for _, field := range request.Fields {
		switch field.Desc.Name() {
		case "filter":
			if field.Desc.Kind() != protoreflect.StringKind || field.Desc.Cardinality() == protoreflect.Repeated {
				return fmt.Errorf("%s.filter must be a `string` (AIP-160)", request.GoIdent.GoName)
			}
			if _, err := pbutil.GetExtension[*codegenaippb.FilteringOptions](request.Desc.Options(), codegenaippb.E_Filtering); err != nil {
				return fmt.Errorf("%s declares a filter: it must set `option (malonaz.codegen.aip.v1.filtering)`", request.GoIdent.GoName)
			}
			exp.filter = field
		case "show_deleted":
			if field.Desc.Kind() != protoreflect.BoolKind || field.Desc.Cardinality() == protoreflect.Repeated {
				return fmt.Errorf("%s.show_deleted must be a `bool`", request.GoIdent.GoName)
			}
			if exp.mi.rpc.Message.Desc.Fields().ByName("delete_time") == nil {
				return fmt.Errorf("%s.show_deleted: %s is not soft-deletable", request.GoIdent.GoName, exp.mi.rpc.ParsedResource.Desc.Type)
			}
			exp.showDeleted = field
		}
	}
	return nil
}

// parseResponse resolves the response's item field: its first, `repeated {Item}
// {plural}`, where the item is the resource or a message the runner aggregates.
func (exp *exportMethod) parseResponse(response *protogen.Message) error {
	pluralSnake := xstrings.ToSnakeCase(exp.mi.rpc.ParsedResource.Desc.Plural)
	if len(response.Fields) == 0 || string(response.Fields[0].Desc.Name()) != pluralSnake ||
		response.Fields[0].Desc.Cardinality() != protoreflect.Repeated || response.Fields[0].Message == nil {
		return fmt.Errorf("%s must declare `repeated {Item} %s` as its first field, {Item} being %s or a message aggregating it (AIP-153)",
			response.GoIdent.GoName, pluralSnake, exp.mi.rpc.Message.Desc.FullName())
	}
	exp.response = response
	exp.items = response.Fields[0]
	exp.aggregate = exp.items.Message.Desc.FullName() != exp.mi.rpc.Message.Desc.FullName()
	return nil
}

// checkInlineFormats enforces AIP-153's "the same format must be used for
// both import and export": an Import{Plural} inlines the resource itself, so an
// Export{Plural} of the same resource cannot inline an aggregate.
func checkInlineFormats(si *serviceInfo) error {
	imported := map[string]bool{}
	for _, lro := range si.lroMethods {
		if lro.imp != nil {
			imported[lro.imp.mi.rpc.ParsedResource.Desc.Type] = true
		}
	}
	for _, lro := range si.lroMethods {
		if lro.exp != nil && lro.exp.aggregate && imported[lro.exp.mi.rpc.ParsedResource.Desc.Type] {
			return fmt.Errorf("%s: %s is imported inline as %s, so it must be exported as %s too, not %s (AIP-153)",
				lro.method.GoName, lro.exp.mi.rpc.ParsedResource.Desc.Type, lro.exp.mi.rpc.Message.Desc.FullName(),
				lro.exp.mi.rpc.Message.Desc.FullName(), lro.exp.items.Message.Desc.FullName())
		}
	}
	return nil
}

// generateExportRunnerMethods emits the export's methods of the runner interface.
func (gen *generator) generateExportRunnerMethods(exp *exportMethod) {
	g := gen.g
	request := gen.qgi(exp.lro.method.Input.GoIdent)
	if exp.aggregate {
		g.P(fmt.Sprintf("  %s(ctx %s, request *%s, %s []*%s) ([]*%s, error)", exp.aggregateGoName(), gen.ident(contextPkg, "Context"), request,
			xstrings.ToCamelCase(exp.mi.rpc.ParsedResource.PluralGoName()), gen.qgi(exp.mi.rpc.Message.GoIdent), gen.qgi(exp.items.Message.GoIdent)))
	}
	for _, destination := range exp.destinations {
		g.P(fmt.Sprintf("  %s(ctx %s, request *%s, reader *%s) (*%s, error)", exp.runnerMethodGoName(destination), gen.ident(contextPkg, "Context"), request,
			exp.readerGoName(), gen.qgi(exp.response.GoIdent)))
	}
}

// generateExport emits the reader and Run{Export} of an export method.
func (mc *methodCtx) generateExport(exp *exportMethod) error {
	modelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](mc.mi.rpc.Message.Desc.Options(), modelpb.E_ModelOpts)
	if err != nil {
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			return fmt.Errorf("%s: exporting %s requires it to declare model_opts", exp.lro.method.GoName, mc.pr.Desc.Type)
		}
		return fmt.Errorf("getting model_opts of %s: %w", mc.pr.Desc.Type, err)
	}
	bindings, err := schema.UnionColumnBindings(mc.pr, modelOpts)
	if err != nil {
		return err
	}
	mc.generateExportReader(exp, schema.TableOf(mc.pr, modelOpts).Name, bindings)
	mc.generateRunExport(exp)
	return nil
}

func (mc *methodCtx) generateExportReader(exp *exportMethod, table string, bindings []schema.ColumnBinding) {
	g := mc.g
	pr := mc.pr
	method := exp.lro.method
	reader := exp.readerGoName()
	serviceServer := mc.si.service.GoName + "Server"
	itemType := mc.gen.qgi(exp.items.Message.GoIdent)
	parentIDNames := mc.parentIDNames()
	resourceVar := xstrings.ToCamelCase(mc.resourceGoName)
	pluralVar := xstrings.ToCamelCase(pr.PluralGoName())
	dbPlural := "db" + pr.PluralGoName()
	parserVar := xstrings.ToCamelCase(method.Input.GoIdent.GoName) + "FilteringParser"

	if exp.filter != nil {
		g.P(fmt.Sprintf("var %s = %s[*%s, *%s](%s())", parserVar,
			mc.gen.ident(aipPkg, "MustNewFilteringRequestParser"), mc.inputType(), mc.protoType(), mc.gen.ident(aipPkg, "WithFQN")))
		g.P()
	}

	// The keyset: the identifier columns, in pattern order, which the store's
	// primary key indexes. A column absent from some patterns is NULL there.
	keyColumns := make([]string, len(bindings))
	keyPlaceholders := make([]string, len(bindings))
	keyArgs := make([]string, len(bindings))
	for i, binding := range bindings {
		keyColumns[i] = table + "." + binding.Column
		if !binding.Shared {
			keyColumns[i] = fmt.Sprintf("COALESCE(%s, '')", keyColumns[i])
		}
		keyPlaceholders[i] = "$%d"
		keyArgs[i] = fmt.Sprintf("len(whereParams)+%d", i+1)
	}
	orderBy := "ORDER BY " + strings.Join(keyColumns, ", ")
	keyset := fmt.Sprintf("(%s) > (%s)", strings.Join(keyColumns, ", "), strings.Join(keyPlaceholders, ", "))

	g.P(fmt.Sprintf("// %s reads the items of %s out of the store (AIP-153): the %s under", reader, method.GoName, pr.Desc.Plural))
	g.P("// the request's parent matching its filter, a page at a time in primary key order, each")
	g.P("// counted as exported in the operation's metadata.")
	g.P(fmt.Sprintf("type %s struct {", reader))
	g.P(fmt.Sprintf("  server *%s", mc.serverGoName))
	if exp.aggregate {
		g.P(fmt.Sprintf("  runner %s", runnerGoName(mc.si)))
	}
	g.P(fmt.Sprintf("  request *%s", mc.inputType()))
	if len(parentIDNames) > 0 {
		g.P(fmt.Sprintf("  %s string", strings.Join(parentIDNames, ", ")))
	}
	g.P("  whereClause string")
	g.P("  whereParams []any")
	g.P(fmt.Sprintf("  progress *%s", mc.gen.ident(longrunningPkg, "ExportProgress")))
	g.P("  // The key of the last row read, which the next page starts after; nil before the first.")
	g.P("  after []any")
	g.P("  done bool")
	g.P("}")
	g.P()

	g.P(fmt.Sprintf("func (s *%s) new%s(request *%s) (*%s, error) {", serviceServer, reader, mc.inputType(), reader))
	mc.generateParseParent("request.GetParent()")
	g.P("  var whereClause string")
	g.P("  var whereParams []any")
	if exp.filter != nil {
		g.P(fmt.Sprintf("  filteringRequest, err := %s.Parse(request)", parserVar))
		g.P("  if err != nil {")
		g.P(fmt.Sprintf("    return nil, %s(%s, err.Error()).Err()", mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("  }")
		g.P("  whereClause, whereParams = filteringRequest.GetSQLWhereClause()")
	}
	g.P(fmt.Sprintf("  return &%s{", reader))
	g.P(fmt.Sprintf("    server: s.%s,", mc.serverGoName))
	if exp.aggregate {
		g.P("    runner: s.runner,")
	}
	g.P("    request: request,")
	for _, name := range parentIDNames {
		g.P(fmt.Sprintf("    %s: %s,", name, name))
	}
	g.P("    whereClause: whereClause,")
	g.P("    whereParams: whereParams,")
	g.P(fmt.Sprintf("    progress: %s(s.schedulerServiceClient),", mc.gen.ident(longrunningPkg, "NewExportProgress")))
	g.P("  }, nil")
	g.P("}")
	g.P()

	// Next: one page per store call, skipping pages the aggregate empties.
	listArgs := []string{"ctx"}
	for _, name := range parentIDNames {
		listArgs = append(listArgs, "r."+name)
	}
	if mc.softDeletable {
		if exp.showDeleted != nil {
			listArgs = append(listArgs, "r.request.Get"+exp.showDeleted.GoName+"()")
		} else {
			listArgs = append(listArgs, "false")
		}
	}
	listArgs = append(listArgs, "whereClause", fmt.Sprintf("%q", orderBy), fmt.Sprintf("\"LIMIT %d\"", exportPageSize), "nil", "whereParams...")

	g.P("// Next returns the next items, nil once the export is exhausted. They are counted")
	g.P("// as exported; the error returned means the operation was cancelled or the read")
	g.P("// failed: stop exporting.")
	g.P(fmt.Sprintf("func (r *%s) Next(ctx %s) ([]*%s, error) {", reader, mc.gen.ident(contextPkg, "Context"), itemType))
	g.P("  for !r.done {")
	g.P(fmt.Sprintf("    whereClause, whereParams := r.whereClause, %s(r.whereParams)", mc.gen.ident(slicesPkg, "Clone")))
	g.P("    if r.after != nil {")
	g.P(fmt.Sprintf("      whereClause = %s(whereClause, %s(%q, %s))",
		mc.gen.ident(postgresPkg, "AddToWhereClause"), mc.fmtSprintf(), keyset, strings.Join(keyArgs, ", ")))
	g.P("      whereParams = append(whereParams, r.after...)")
	g.P("    }")
	g.P(fmt.Sprintf("    %s, err := r.server.store.List%s(%s)", dbPlural, pr.PluralGoName(), strings.Join(listArgs, ", ")))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(err, \"listing %s\").Err()", mc.statusFromError(), xstrings.ToSnakeCase(pr.PluralGoName())))
	g.P("    }")
	g.P(fmt.Sprintf("    r.done = len(%s) < %d", dbPlural, exportPageSize))
	g.P(fmt.Sprintf("    if len(%s) == 0 {", dbPlural))
	g.P("      return nil, nil")
	g.P("    }")
	g.P(fmt.Sprintf("    last := %s[len(%s)-1]", dbPlural, dbPlural))
	afterValues := make([]string, len(bindings))
	for i, binding := range bindings {
		afterValues[i] = "last." + binding.GoFieldName()
		if binding.Shared {
			continue
		}
		// NULL never compares: key the row on the COALESCE the keyset compares.
		keyVar := xstrings.ToCamelCase(binding.Variable) + "Key"
		g.P(fmt.Sprintf("    %s := \"\"", keyVar))
		g.P(fmt.Sprintf("    if last.%s != nil {", binding.GoFieldName()))
		g.P(fmt.Sprintf("      %s = *last.%s", keyVar, binding.GoFieldName()))
		g.P("    }")
		afterValues[i] = keyVar
	}
	g.P(fmt.Sprintf("    r.after = []any{%s}", strings.Join(afterValues, ", ")))
	g.P(fmt.Sprintf("    %s := make([]*%s, 0, len(%s))", pluralVar, mc.protoType(), dbPlural))
	g.P(fmt.Sprintf("    for _, db%s := range %s {", mc.modelGoName, dbPlural))
	g.P(fmt.Sprintf("      %s, err := db%s.ToPb()", resourceVar, mc.modelGoName))
	g.P("      if err != nil {")
	g.P(fmt.Sprintf("        return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()", mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("      }")
	g.P(fmt.Sprintf("      %s = append(%s, %s)", pluralVar, pluralVar, resourceVar))
	g.P("    }")
	if exp.aggregate {
		g.P(fmt.Sprintf("    items, err := r.runner.%s(ctx, r.request, %s)", exp.aggregateGoName(), pluralVar))
		g.P("    if err != nil {")
		g.P("      return nil, err")
		g.P("    }")
	} else {
		g.P(fmt.Sprintf("    items := %s", pluralVar))
	}
	g.P("    if len(items) == 0 {")
	g.P("      continue")
	g.P("    }")
	g.P("    if err := r.progress.Succeeded(ctx, int32(len(items))); err != nil {")
	g.P("      return nil, err")
	g.P("    }")
	g.P("    return items, nil")
	g.P("  }")
	g.P("  return nil, nil")
	g.P("}")
	g.P()

	g.P("// SetTotal records how many items the export holds, when the destination knows.")
	g.P(fmt.Sprintf("func (r *%s) SetTotal(ctx %s, total int32) error {", reader, mc.gen.ident(contextPkg, "Context")))
	g.P("  return r.progress.SetTotal(ctx, total)")
	g.P("}")
	g.P()

	g.P("// Fail records an item Next returned that the destination could not write (AIP-193).")
	g.P("// The error returned means the operation was cancelled: stop exporting.")
	g.P(fmt.Sprintf("func (r *%s) Fail(ctx %s, err error) error {", reader, mc.gen.ident(contextPkg, "Context")))
	g.P("  return r.progress.Failed(ctx, err)")
	g.P("}")
	g.P()

	g.P("// exportInline answers with every item.")
	g.P(fmt.Sprintf("func (r *%s) exportInline(ctx %s) (*%s, error) {", reader, mc.gen.ident(contextPkg, "Context"), mc.gen.qgi(exp.response.GoIdent)))
	g.P(fmt.Sprintf("  var items []*%s", itemType))
	g.P("  for {")
	g.P("    page, err := r.Next(ctx)")
	g.P("    if err != nil {")
	g.P("      return nil, err")
	g.P("    }")
	g.P("    if len(page) == 0 {")
	g.P(fmt.Sprintf("      return &%s{%s: items}, nil", mc.gen.qgi(exp.response.GoIdent), exp.items.GoName))
	g.P("    }")
	g.P("    items = append(items, page...)")
	g.P("  }")
	g.P("}")
	g.P()
}

// generateRunExport emits Run{Export}: it hands the reader to the generated
// inline export or to the runner's destination.
func (mc *methodCtx) generateRunExport(exp *exportMethod) {
	g := mc.g
	method := exp.lro.method
	serviceServer := mc.si.service.GoName + "Server"

	g.P(fmt.Sprintf("// Run%s exports %s to the request's destination (AIP-153).", method.GoName, mc.pr.Desc.Plural))
	g.P(fmt.Sprintf("func (s *%s) Run%s(ctx %s, request *%s) (*%s, error) {",
		serviceServer, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.gen.qgi(exp.response.GoIdent)))
	g.P(fmt.Sprintf("  reader, err := s.new%s(request)", exp.readerGoName()))
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  switch request.GetDestination().(type) {")
	g.P(fmt.Sprintf("  case *%s:", mc.gen.qgi(exp.inline.GoIdent)))
	g.P("    return reader.exportInline(ctx)")
	for _, field := range exp.destinations {
		g.P(fmt.Sprintf("  case *%s:", mc.gen.qgi(field.GoIdent)))
		g.P(fmt.Sprintf("    return s.runner.%s(ctx, request, reader)", exp.runnerMethodGoName(field)))
	}
	g.P("  }")
	g.P(fmt.Sprintf("  return nil, %s(%s, \"destination is required\").Err()", mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("}")
	g.P()
}

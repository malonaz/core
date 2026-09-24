package rpc

import (
	"fmt"
	"strings"

	validate "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/huandu/xstrings"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/malonaz/core/go/pbutil"
)

const (
	importMetadataType = "malonaz.aip.v1.ImportMetadata"
	inlineSourceName   = "InlineSource"
	// Rows per insert of an inline import; also the granularity of its progress.
	importBatchSize = 500
)

var (
	strconvPkg = protogen.GoImportPath("strconv")
	stringsPkg = protogen.GoImportPath("strings")
)

// importMethod is Import{Plural} (AIP-153) on a resource the service owns: a
// long-running standard method whose request names its source in a oneof. The
// inline source is imported by generated code; every other source by a method
// of the runner, through the generated sink.
type importMethod struct {
	lro *longrunningMethod
	mi  *methodInfo
	// The `source` variants: the inline one and, in declaration order, the custom ones.
	inline  *protogen.Field
	sources []*protogen.Field
	// InlineSource's repeated resource field.
	items *protogen.Field
	// The operation's response and its `names` field.
	response *protogen.Message
	names    *protogen.Field
}

// sourceLabel is the value of the import-source label for a source variant:
// the field name minus `_source`, in kebab-case.
func sourceLabel(source *protogen.Field) string {
	return strings.ReplaceAll(strings.TrimSuffix(string(source.Desc.Name()), "_source"), "_", "-")
}

// runnerMethodGoName is the runner method importing from a custom source, e.g. ImportBooksFromTitles.
func (imp *importMethod) runnerMethodGoName(source *protogen.Field) string {
	return imp.lro.method.GoName + "From" + strings.TrimSuffix(source.GoName, "Source")
}

func (imp *importMethod) sinkGoName() string { return imp.lro.method.GoName + "Sink" }

// parseImportMethod validates the AIP-153 shape of an Import{Plural} method.
func parseImportMethod(lro *longrunningMethod, mi *methodInfo) (*importMethod, error) {
	method := lro.method
	if !mi.rpc.Import {
		return nil, fmt.Errorf("%s: only Import{Plural} may be both a standard method and long-running", method.GoName)
	}
	if mi.natsEventOpts != nil && mi.rpc.StandardMethod.GetEmitEvent() {
		return nil, fmt.Errorf("%s: an import never emits events; drop standard_method.emit_event", method.GoName)
	}
	httpRule, err := pbutil.GetExtension[*annotations.HttpRule](method.Desc.Options(), annotations.E_Http)
	if err != nil {
		return nil, fmt.Errorf("%s: getting google.api.http: %w", method.GoName, err)
	}
	if !strings.HasSuffix(httpRule.GetPost(), ":import") || httpRule.GetBody() != "*" {
		return nil, fmt.Errorf("%s: google.api.http must be `post: \"/v1/{parent=...}/%s:import\"` with `body: \"*\"` (AIP-153)",
			method.GoName, xstrings.ToSnakeCase(mi.rpc.ParsedResource.Desc.Plural))
	}
	if lro.responseType != method.Desc.ParentFile().Package().Append(protoreflect.Name(method.GoName+"Response")) {
		return nil, fmt.Errorf("%s: operation_info.response_type must be %sResponse", method.GoName, method.GoName)
	}
	metadataType := protoreflect.FullName(strings.TrimPrefix(lro.operationInfo.GetMetadataType(), "."))
	if metadataType != importMetadataType {
		return nil, fmt.Errorf("%s: operation_info.metadata_type must be %s", method.GoName, importMetadataType)
	}

	imp := &importMethod{lro: lro, mi: mi}
	if err := imp.parseRequest(); err != nil {
		return nil, err
	}
	return imp, nil
}

func (imp *importMethod) parseRequest() error {
	method := imp.lro.method
	request := method.Input
	resource := imp.mi.rpc.Message
	pluralSnake := xstrings.ToSnakeCase(imp.mi.rpc.ParsedResource.Desc.Plural)

	if imp.lro.resourceField.Desc.Name() != "parent" {
		return fmt.Errorf("%s must declare a `parent` field: an import lands under a collection (AIP-153)", request.GoIdent.GoName)
	}
	if imp.lro.requestIDField == nil {
		return fmt.Errorf("%s must declare a `request_id` field", request.GoIdent.GoName)
	}
	requestIDRules, err := pbutil.GetExtension[*validate.FieldRules](imp.lro.requestIDField.Desc.Options(), validate.E_Field)
	if err != nil || !requestIDRules.GetRequired() || !requestIDRules.GetString().GetUuid() {
		return fmt.Errorf("%s.request_id must be `(buf.validate.field).required = true` and `.string.uuid = true`", request.GoIdent.GoName)
	}

	var source *protogen.Oneof
	for _, oneof := range request.Oneofs {
		if oneof.Desc.Name() == "source" {
			source = oneof
		}
	}
	if source == nil {
		return fmt.Errorf("%s must declare `oneof source` (AIP-153)", request.GoIdent.GoName)
	}
	oneofRules, err := pbutil.GetExtension[*validate.OneofRules](source.Desc.Options(), validate.E_Oneof)
	if err != nil || !oneofRules.GetRequired() {
		return fmt.Errorf("%s.source must be `(buf.validate.oneof).required = true`", request.GoIdent.GoName)
	}
	for _, field := range source.Fields {
		if field.Message == nil || !strings.HasSuffix(field.Message.GoIdent.GoName, "Source") {
			return fmt.Errorf("%s.%s must be a message named *Source (AIP-153)", request.GoIdent.GoName, field.Desc.Name())
		}
		if field.Message.Desc.Name() != inlineSourceName {
			imp.sources = append(imp.sources, field)
			continue
		}
		if imp.inline != nil {
			return fmt.Errorf("%s.source declares %s twice", request.GoIdent.GoName, inlineSourceName)
		}
		// Nested in the request: a package holds one import per resource, and a top-level
		// InlineSource would let it hold one import only.
		if field.Message.Desc.Parent() != request.Desc {
			return fmt.Errorf("%s.%s must be a message nested in the request (AIP-153)", request.GoIdent.GoName, field.Desc.Name())
		}
		imp.inline = field
		items := field.Message.Fields
		if len(items) != 1 || items[0].Desc.Cardinality() != protoreflect.Repeated || items[0].Message == nil ||
			items[0].Message.Desc.FullName() != resource.Desc.FullName() || string(items[0].Desc.Name()) != pluralSnake {
			return fmt.Errorf("%s must be exactly `message InlineSource { repeated %s %s = 1; }` (AIP-153)",
				field.Message.Desc.FullName(), resource.Desc.FullName(), pluralSnake)
		}
		imp.items = items[0]
	}
	if imp.inline == nil {
		return fmt.Errorf("%s.source must declare `InlineSource inline_source` with `message InlineSource { repeated %s %s = 1; }` nested in it (AIP-153)",
			request.GoIdent.GoName, resource.Desc.FullName(), pluralSnake)
	}
	return nil
}

// parseResponse resolves the response's `names` field once the compilation
// unit's messages are known.
func (imp *importMethod) parseResponse(response *protogen.Message) error {
	fields := response.Fields
	if len(fields) != 1 || fields[0].Desc.Name() != "names" || fields[0].Desc.Cardinality() != protoreflect.Repeated || fields[0].Desc.Kind() != protoreflect.StringKind {
		return fmt.Errorf("%s must be exactly `repeated string names = 1` with a google.api.resource_reference to %s",
			response.GoIdent.GoName, imp.mi.rpc.ParsedResource.Desc.Type)
	}
	reference, err := pbutil.GetExtension[*annotations.ResourceReference](fields[0].Desc.Options(), annotations.E_ResourceReference)
	if err != nil || reference.GetType() != imp.mi.rpc.ParsedResource.Desc.Type {
		return fmt.Errorf("%s.names must carry `(google.api.resource_reference).type = %q`", response.GoIdent.GoName, imp.mi.rpc.ParsedResource.Desc.Type)
	}
	imp.response = response
	imp.names = fields[0]
	return nil
}

// generateImport emits the sink and Run{Import} of an import method.
func (mc *methodCtx) generateImport(imp *importMethod) error {
	if mc.singleton {
		return fmt.Errorf("%s: a singleton cannot be imported", imp.lro.method.GoName)
	}
	createRequest, err := createRequestMessage(mc.si, mc.pr)
	if err != nil {
		return err
	}
	if createRequest == nil {
		return fmt.Errorf("%s: importing %s requires a Create%s or BatchCreate%s to prepare resources with",
			imp.lro.method.GoName, mc.pr.Desc.Type, mc.resourceGoName, mc.pr.PluralGoName())
	}
	mc.generateImportSink(imp, createRequest)
	mc.generateRunImport(imp)
	return nil
}

func (mc *methodCtx) generateImportSink(imp *importMethod, createRequest *protogen.Message) {
	g := mc.g
	pr := mc.pr
	sink := imp.sinkGoName()
	resourceVar := xstrings.ToCamelCase(mc.resourceGoName)
	pluralVar := xstrings.ToCamelCase(pr.PluralGoName())
	protoType := mc.protoType()
	serviceServer := mc.si.service.GoName + "Server"
	models := mc.createModelVars()
	modelTypes := []string{mc.goTypeQgi}
	for _, child := range mc.singletonChildren {
		modelTypes = append(modelTypes, mc.gen.modelIdent(child.Message.GoIdent.GoName))
	}
	hasLabels := mc.mi.rpc.Message.Desc.Fields().ByName("labels") != nil
	createHasParent := createRequest.Desc.Fields().ByName("parent") != nil
	createHasID := createRequest.Desc.Fields().ByName(protoreflect.Name(xstrings.ToSnakeCase(mc.resourceGoName)+"_id")) != nil

	g.P(fmt.Sprintf("// %s takes the %s of %s's sources into the store (AIP-153): each is", sink, pr.Desc.Plural, imp.lro.method.GoName))
	g.P("// stamped, inserted under the request's parent and counted in the operation's metadata.")
	g.P(fmt.Sprintf("type %s struct {", sink))
	g.P(fmt.Sprintf("  server *%s", mc.serverGoName))
	g.P(fmt.Sprintf("  request *%s", mc.inputType()))
	g.P(fmt.Sprintf("  requestID %s", mc.gen.ident(uuidPkg, "UUID")))
	g.P("  // The import-source and import-time label values of this run.")
	g.P("  source, date string")
	g.P(fmt.Sprintf("  progress *%s", mc.gen.ident(longrunningPkg, "ImportProgress")))
	g.P("  // The names imported so far, in order.")
	g.P("  names []string")
	g.P("  // Items taken so far, which keys the request id of an unnamed item.")
	g.P("  taken int")
	g.P("}")
	g.P()

	g.P(fmt.Sprintf("func (s *%s) new%s(request *%s, source string) (*%s, error) {", serviceServer, sink, mc.inputType(), sink))
	g.P(fmt.Sprintf("  requestID, err := %s(request.GetRequestId())", mc.gen.ident(uuidPkg, "Parse")))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"parsing request_id: %%v\", err).Err()", mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P(fmt.Sprintf("  return &%s{", sink))
	g.P(fmt.Sprintf("    server: s.%s,", mc.serverGoName))
	g.P("    request: request,")
	g.P("    requestID: requestID,")
	g.P("    source: source,")
	g.P(fmt.Sprintf("    date: %s().UTC().Format(%s),", mc.gen.ident(timePkg, "Now"), mc.gen.ident(aipPkg, "LabelDateFormat")))
	g.P(fmt.Sprintf("    progress: %s(s.schedulerServiceClient),", mc.gen.ident(longrunningPkg, "NewImportProgress")))
	g.P("  }, nil")
	g.P("}")
	g.P()

	g.P("// SetTotal records how many items the source holds, when known ahead of time.")
	g.P(fmt.Sprintf("func (s *%s) SetTotal(ctx %s, total int32) error {", sink, mc.gen.ident(contextPkg, "Context")))
	g.P("  return s.progress.SetTotal(ctx, total)")
	g.P("}")
	g.P()

	g.P(fmt.Sprintf("// Fail records an item the source could not turn into a %s (AIP-193). The error", pr.Desc.Singular))
	g.P("// returned means the operation was cancelled: stop importing.")
	g.P(fmt.Sprintf("func (s *%s) Fail(ctx %s, err error) error {", sink, mc.gen.ident(contextPkg, "Context")))
	g.P("  return s.progress.Failed(ctx, err)")
	g.P("}")
	g.P()

	// prepare: one item to its models.
	returns := make([]string, len(modelTypes))
	for i, modelType := range modelTypes {
		returns[i] = "*" + modelType
	}
	g.P(fmt.Sprintf("// prepare stamps one %s and resolves it to its database models: a name it carries", pr.Desc.Singular))
	g.P("// must be under the request's parent; timestamps and the import labels are kept when set.")
	g.P(fmt.Sprintf("func (s *%s) prepare(ctx %s, %s *%s) (string, %s, error) {", sink, mc.gen.ident(contextPkg, "Context"), resourceVar, protoType, strings.Join(returns, ", ")))
	errReturn := "return \"\", " + strings.Repeat("nil, ", len(modelTypes))
	g.P(fmt.Sprintf("  name := %s.GetName()", resourceVar))
	g.P("  requestIDKey := name")
	g.P("  if requestIDKey == \"\" {")
	g.P(fmt.Sprintf("    requestIDKey = %s(s.taken)", mc.gen.ident(strconvPkg, "Itoa")))
	g.P("  }")
	g.P("  s.taken++")
	g.P(fmt.Sprintf("  createRequest := &%s{", mc.gen.qgi(createRequest.GoIdent)))
	g.P(fmt.Sprintf("    RequestId: %s(s.requestID, requestIDKey).String(),", mc.gen.ident(uuidPkg, "NewV5")))
	if createHasParent {
		g.P("    Parent: s.request.GetParent(),")
	}
	g.P(fmt.Sprintf("    %s: %s,", mc.resourceGoName, resourceVar))
	g.P("  }")
	if createHasID {
		g.P("  if name != \"\" {")
		g.P(fmt.Sprintf("    createRequest.%sId = name[%s(name, \"/\")+1:]", mc.resourceGoName, mc.gen.ident(stringsPkg, "LastIndex")))
		g.P("  }")
	} else {
		g.P("  if name != \"\" {")
		g.P(fmt.Sprintf("    %s%s(%s, \"%s names are assigned by the server: %%q must be empty\", name).Err()", errReturn, mc.statusErrorf(), mc.codes("InvalidArgument"), pr.Desc.Singular))
		g.P("  }")
	}
	if hasLabels {
		g.P(fmt.Sprintf("  if !%s(%s, %s) {", mc.gen.ident(aipPkg, "HasLabel"), resourceVar, mc.gen.ident(aipPkg, "LabelKeyImportSource")))
		g.P(fmt.Sprintf("    %s(%s, %s, s.source)", mc.gen.ident(aipPkg, "SetLabel"), resourceVar, mc.gen.ident(aipPkg, "LabelKeyImportSource")))
		g.P("  }")
		g.P(fmt.Sprintf("  if !%s(%s, %s) {", mc.gen.ident(aipPkg, "HasLabel"), resourceVar, mc.gen.ident(aipPkg, "LabelKeyImportTime")))
		g.P(fmt.Sprintf("    %s(%s, %s, s.date)", mc.gen.ident(aipPkg, "SetLabel"), resourceVar, mc.gen.ident(aipPkg, "LabelKeyImportTime")))
		g.P("  }")
	}
	g.P(fmt.Sprintf("  %s, err := s.server.%s(ctx, createRequest, true)", strings.Join(models, ", "), mc.prepareGoName()))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    %serr", errReturn))
	g.P("  }")
	g.P(fmt.Sprintf("  if name != \"\" && %s.GetName() != name {", resourceVar))
	g.P(fmt.Sprintf("    %s%s(%s, \"%s %%q is not under parent %%q\", name, s.request.GetParent()).Err()", errReturn, mc.statusErrorf(), mc.codes("InvalidArgument"), pr.Desc.Singular))
	g.P("  }")
	g.P(fmt.Sprintf("  return createRequest.RequestId, %s, nil", strings.Join(models, ", ")))
	g.P("}")
	g.P()

	// insert: one batch, falling back to one row at a time so a bad row fails alone.
	modelSlices := make([]string, len(models))
	sliceParams := make([]string, len(models))
	singleSlices := make([]string, len(models))
	for i, model := range models {
		modelSlices[i] = model + "s"
		sliceParams[i] = fmt.Sprintf("%s []*%s", modelSlices[i], modelTypes[i])
		singleSlices[i] = modelSlices[i] + "[i:i+1]"
	}
	storeInsert := "s.server." + strings.TrimPrefix(mc.batchInsertCall("requestIDs", modelSlices), "s.")
	storeInsertOne := "s.server." + strings.TrimPrefix(mc.batchInsertCall("requestIDs[i:i+1]", singleSlices), "s.")
	g.P("// insert inserts the batch atomically or, when that fails, one row at a time so that")
	g.P("// only the rows at fault are recorded as failures.")
	g.P(fmt.Sprintf("func (s *%s) insert(ctx %s, requestIDs []string, %s) ([]*%s, error) {", sink, mc.gen.ident(contextPkg, "Context"), strings.Join(sliceParams, ", "), mc.goTypeQgi))
	g.P(fmt.Sprintf("  if db%s, err := %s; err == nil {", pr.PluralGoName(), storeInsert))
	g.P(fmt.Sprintf("    return db%s, nil", pr.PluralGoName()))
	g.P("  }")
	g.P(fmt.Sprintf("  db%s := make([]*%s, 0, len(requestIDs))", pr.PluralGoName(), mc.goTypeQgi))
	g.P("  for i := range requestIDs {")
	g.P(fmt.Sprintf("    inserted, err := %s", storeInsertOne))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      if %s(err, %s) {", mc.errorsIs(), mc.errAlreadyExists))
	g.P(fmt.Sprintf("        err = %s(%s, \"%s already exists\").Err()", mc.statusErrorf(), mc.codes("AlreadyExists"), pr.Desc.Singular))
	g.P("      }")
	g.P("      if err := s.Fail(ctx, err); err != nil {")
	g.P("        return nil, err")
	g.P("      }")
	g.P("      continue")
	g.P("    }")
	g.P(fmt.Sprintf("    db%s = append(db%s, inserted...)", pr.PluralGoName(), pr.PluralGoName()))
	g.P("  }")
	g.P(fmt.Sprintf("  return db%s, nil", pr.PluralGoName()))
	g.P("}")
	g.P()

	// Import: the public entry point.
	g.P(fmt.Sprintf("// Import imports a batch of %s and returns them as stored, in order. One that cannot", pr.Desc.Plural))
	g.P("// be imported is recorded as a partial failure and left out; the error returned means")
	g.P("// the operation was cancelled: stop importing.")
	g.P(fmt.Sprintf("func (s *%s) Import(ctx %s, %s []*%s) ([]*%s, error) {", sink, mc.gen.ident(contextPkg, "Context"), pluralVar, protoType, protoType))
	g.P(fmt.Sprintf("  requestIDs := make([]string, 0, len(%s))", pluralVar))
	for i, slice := range modelSlices {
		g.P(fmt.Sprintf("  %s := make([]*%s, 0, len(%s))", slice, modelTypes[i], pluralVar))
	}
	g.P(fmt.Sprintf("  for _, %s := range %s {", resourceVar, pluralVar))
	g.P(fmt.Sprintf("    requestID, %s, err := s.prepare(ctx, %s)", strings.Join(models, ", "), resourceVar))
	g.P("    if err != nil {")
	g.P("      if err := s.Fail(ctx, err); err != nil {")
	g.P("        return nil, err")
	g.P("      }")
	g.P("      continue")
	g.P("    }")
	g.P("    requestIDs = append(requestIDs, requestID)")
	for i, slice := range modelSlices {
		g.P(fmt.Sprintf("    %s = append(%s, %s)", slice, slice, models[i]))
	}
	g.P("  }")
	g.P("  if len(requestIDs) == 0 {")
	g.P("    return nil, nil")
	g.P("  }")
	g.P(fmt.Sprintf("  db%s, err := s.insert(ctx, requestIDs, %s)", pr.PluralGoName(), strings.Join(modelSlices, ", ")))
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P(fmt.Sprintf("  imported := make([]*%s, 0, len(db%s))", protoType, pr.PluralGoName()))
	g.P(fmt.Sprintf("  for _, db%s := range db%s {", mc.modelGoName, pr.PluralGoName()))
	g.P(fmt.Sprintf("    %s, err := db%s.ToPb()", resourceVar, mc.modelGoName))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()", mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    imported = append(imported, %s)", resourceVar))
	g.P(fmt.Sprintf("    s.names = append(s.names, %s.GetName())", resourceVar))
	g.P("  }")
	g.P("  if err := s.progress.Succeeded(ctx, int32(len(imported))); err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  return imported, nil")
	g.P("}")
	g.P()
}

// generateRunImport emits Run{Import}: it dispatches the request's source to the
// generated inline import or to the runner, and answers with the names imported.
func (mc *methodCtx) generateRunImport(imp *importMethod) {
	g := mc.g
	method := imp.lro.method
	serviceServer := mc.si.service.GoName + "Server"

	g.P(fmt.Sprintf("// Run%s imports %s from the request's source (AIP-153).", method.GoName, mc.pr.Desc.Plural))
	g.P(fmt.Sprintf("func (s *%s) Run%s(ctx %s, request *%s) (*%s, error) {",
		serviceServer, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.gen.qgi(imp.response.GoIdent)))
	g.P(fmt.Sprintf("  if %s(request.GetParent()) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard")))
	g.P(fmt.Sprintf("    return nil, %s(%s, \"parent cannot contain wildcard\").Err()", mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P("  var sourceLabel string")
	g.P("  switch request.GetSource().(type) {")
	for _, field := range append([]*protogen.Field{imp.inline}, imp.sources...) {
		g.P(fmt.Sprintf("  case *%s:", mc.gen.qgi(field.GoIdent)))
		g.P(fmt.Sprintf("    sourceLabel = %q", sourceLabel(field)))
	}
	g.P("  default:")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"source is required\").Err()", mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P(fmt.Sprintf("  sink, err := s.new%s(request, sourceLabel)", imp.sinkGoName()))
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P("  switch source := request.GetSource().(type) {")
	g.P(fmt.Sprintf("  case *%s:", mc.gen.qgi(imp.inline.GoIdent)))
	g.P(fmt.Sprintf("    err = sink.importInline(ctx, source.%s.Get%s())", imp.inline.GoName, imp.items.GoName))
	for _, field := range imp.sources {
		g.P(fmt.Sprintf("  case *%s:", mc.gen.qgi(field.GoIdent)))
		g.P(fmt.Sprintf("    err = s.runner.%s(ctx, request, sink)", imp.runnerMethodGoName(field)))
	}
	g.P("  }")
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P(fmt.Sprintf("  return &%s{%s: sink.names}, nil", mc.gen.qgi(imp.response.GoIdent), imp.names.GoName))
	g.P("}")
	g.P()

	pluralVar := xstrings.ToCamelCase(mc.pr.PluralGoName())
	g.P(fmt.Sprintf("// importInline imports the %s the request carries, %d at a time.", mc.pr.Desc.Plural, importBatchSize))
	g.P(fmt.Sprintf("func (s *%s) importInline(ctx %s, %s []*%s) error {", imp.sinkGoName(), mc.gen.ident(contextPkg, "Context"), pluralVar, mc.protoType()))
	g.P(fmt.Sprintf("  if err := s.SetTotal(ctx, int32(len(%s))); err != nil {", pluralVar))
	g.P("    return err")
	g.P("  }")
	g.P(fmt.Sprintf("  for start := 0; start < len(%s); start += %d {", pluralVar, importBatchSize))
	g.P(fmt.Sprintf("    end := min(start+%d, len(%s))", importBatchSize, pluralVar))
	g.P(fmt.Sprintf("    if _, err := s.Import(ctx, %s[start:end]); err != nil {", pluralVar))
	g.P("      return err")
	g.P("    }")
	g.P("  }")
	g.P("  return nil")
	g.P("}")
	g.P()
}

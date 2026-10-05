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
	exportMetadataType = "malonaz.aip.v1.ExportMetadata"
	// Rows per page of an export; also the granularity of its progress.
	exportPageSize = 500
)

var (
	slicesPkg   = protogen.GoImportPath("slices")
	postgresPkg = protogen.GoImportPath("github.com/malonaz/core/go/postgres")
)

// exportMethod is Export{Plural} (AIP-153) on a resource the service owns: a
// long-running standard method. Generated code reads the resources out of the
// store; the runner writes them out.
type exportMethod struct {
	lro *longrunningMethod
	mi  *methodInfo
	// The request's optional `filter` and `show_deleted` fields.
	filter      *protogen.Field
	showDeleted *protogen.Field
	// The operation's response.
	response *protogen.Message
}

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
	exp.response = response
	return exp, nil
}

func (exp *exportMethod) parseRequest() error {
	request := exp.lro.method.Input
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

// generateExportRunnerMethod emits the export's method of the runner interface,
// which writes out what the reader reads.
func (gen *generator) generateExportRunnerMethod(exp *exportMethod) {
	gen.g.P(fmt.Sprintf("  Run%s(ctx %s, request *%s, reader *%s) (*%s, error)", exp.lro.method.GoName, gen.ident(contextPkg, "Context"),
		gen.qgi(exp.lro.method.Input.GoIdent), exp.readerGoName(), gen.qgi(exp.response.GoIdent)))
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

	g.P(fmt.Sprintf("// %s reads the %s of %s out of the store (AIP-153): those under", reader, pr.Desc.Plural, method.GoName))
	g.P("// the request's parent matching its filter, a page at a time in primary key order, each")
	g.P("// counted as exported in the operation's metadata.")
	g.P(fmt.Sprintf("type %s struct {", reader))
	g.P(fmt.Sprintf("  server *%s", mc.serverGoName))
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

	// Next: one page per store call.
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

	g.P(fmt.Sprintf("// Next returns the next %s, nil once the export is exhausted. They are counted", pr.Desc.Plural))
	g.P("// as exported; the error returned means the operation was cancelled or the read")
	g.P("// failed: stop exporting.")
	g.P(fmt.Sprintf("func (r *%s) Next(ctx %s) ([]*%s, error) {", reader, mc.gen.ident(contextPkg, "Context"), mc.protoType()))
	g.P("  if r.done {")
	g.P("    return nil, nil")
	g.P("  }")
	g.P(fmt.Sprintf("  whereClause, whereParams := r.whereClause, %s(r.whereParams)", mc.gen.ident(slicesPkg, "Clone")))
	g.P("  if r.after != nil {")
	g.P(fmt.Sprintf("    whereClause = %s(whereClause, %s(%q, %s))",
		mc.gen.ident(postgresPkg, "AddToWhereClause"), mc.fmtSprintf(), keyset, strings.Join(keyArgs, ", ")))
	g.P("    whereParams = append(whereParams, r.after...)")
	g.P("  }")
	g.P(fmt.Sprintf("  %s, err := r.server.store.List%s(%s)", dbPlural, pr.PluralGoName(), strings.Join(listArgs, ", ")))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(err, \"listing %s\").Err()", mc.statusFromError(), xstrings.ToSnakeCase(pr.PluralGoName())))
	g.P("  }")
	g.P(fmt.Sprintf("  r.done = len(%s) < %d", dbPlural, exportPageSize))
	g.P(fmt.Sprintf("  if len(%s) == 0 {", dbPlural))
	g.P("    return nil, nil")
	g.P("  }")
	g.P(fmt.Sprintf("  last := %s[len(%s)-1]", dbPlural, dbPlural))
	afterValues := make([]string, len(bindings))
	for i, binding := range bindings {
		afterValues[i] = "last." + binding.GoFieldName()
		if binding.Shared {
			continue
		}
		// NULL never compares: key the row on the COALESCE the keyset compares.
		keyVar := xstrings.ToCamelCase(binding.Variable) + "Key"
		g.P(fmt.Sprintf("  %s := \"\"", keyVar))
		g.P(fmt.Sprintf("  if last.%s != nil {", binding.GoFieldName()))
		g.P(fmt.Sprintf("    %s = *last.%s", keyVar, binding.GoFieldName()))
		g.P("  }")
		afterValues[i] = keyVar
	}
	g.P(fmt.Sprintf("  r.after = []any{%s}", strings.Join(afterValues, ", ")))
	g.P(fmt.Sprintf("  %s := make([]*%s, 0, len(%s))", pluralVar, mc.protoType(), dbPlural))
	g.P(fmt.Sprintf("  for _, db%s := range %s {", mc.modelGoName, dbPlural))
	g.P(fmt.Sprintf("    %s, err := db%s.ToPb()", resourceVar, mc.modelGoName))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()", mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    %s = append(%s, %s)", pluralVar, pluralVar, resourceVar))
	g.P("  }")
	g.P(fmt.Sprintf("  if err := r.progress.Succeeded(ctx, int32(len(%s))); err != nil {", pluralVar))
	g.P("    return nil, err")
	g.P("  }")
	g.P(fmt.Sprintf("  return %s, nil", pluralVar))
	g.P("}")
	g.P()

	g.P(fmt.Sprintf("// SetTotal records how many %s the export holds, when the destination knows.", pr.Desc.Plural))
	g.P(fmt.Sprintf("func (r *%s) SetTotal(ctx %s, total int32) error {", reader, mc.gen.ident(contextPkg, "Context")))
	g.P("  return r.progress.SetTotal(ctx, total)")
	g.P("}")
	g.P()

	g.P(fmt.Sprintf("// Fail records a %s Next returned that the destination could not write (AIP-193).", pr.Desc.Singular))
	g.P("// The error returned means the operation was cancelled: stop exporting.")
	g.P(fmt.Sprintf("func (r *%s) Fail(ctx %s, err error) error {", reader, mc.gen.ident(contextPkg, "Context")))
	g.P("  return r.progress.Failed(ctx, err)")
	g.P("}")
	g.P()
}

// generateRunExport emits run{Export}: it hands the reader to the runner.
func (mc *methodCtx) generateRunExport(exp *exportMethod) {
	g := mc.g
	method := exp.lro.method
	serviceServer := mc.si.service.GoName + "Server"

	g.P(fmt.Sprintf("// run%s exports %s through the runner (AIP-153).", method.GoName, mc.pr.Desc.Plural))
	g.P(fmt.Sprintf("func (s *%s) run%s(ctx %s, request *%s) (*%s, error) {",
		serviceServer, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.gen.qgi(exp.response.GoIdent)))
	g.P(fmt.Sprintf("  reader, err := s.new%s(request)", exp.readerGoName()))
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P(fmt.Sprintf("  return s.runner.Run%s(ctx, request, reader)", method.GoName))
	g.P("}")
	g.P()
}

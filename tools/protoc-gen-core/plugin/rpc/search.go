package rpc

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"

	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

func (mc *methodCtx) generateSearch() error {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName

	searchDoc, err := schema.SearchDocument(mc.mi.rpc.Message)
	if err != nil {
		return err
	}
	if searchDoc == nil {
		return fmt.Errorf("resource %s must declare (malonaz.codegen.aip.v1.search) options for method %s", pr.Desc.Type, method.GoName)
	}
	hasSnippets := len(searchDoc.SnippetFields) > 0
	if hasSnippets && method.Output.Desc.Fields().ByName("snippets") == nil {
		return fmt.Errorf("response %s must declare a `repeated malonaz.aip.v1.SearchSnippet snippets` field (resource %s has snippet search fields)", method.Output.GoIdent.GoName, pr.Desc.Type)
	}

	// Search request parser.
	// Mirror the List parser: qualify column references with join aliases so
	// joined fields sharing a source column name don't collide.
	g.P(fmt.Sprintf("var %sParser = %s[*%s, *%s](%s())",
		xstrings.ToCamelCase(method.Input.GoIdent.GoName),
		mc.gen.ident(aipPkg, "MustNewSearchRequestParser"), mc.inputType(), mc.protoType(),
		mc.gen.ident(aipPkg, "WithFQN"),
	))
	g.P()

	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	if mc.multiPattern {
		// The parent can follow any of the resource's parent patterns; the
		// identifiers of the unmatched patterns stay empty and are not filtered on.
		parentIDNames := mc.parentIDNames()
		g.P("// Parse parent names")
		g.P(fmt.Sprintf("  var %s string", strings.Join(parentIDNames, ", ")))
		g.P("  switch {")
		for _, parent := range mc.uniqueParentPatterns() {
			g.P(fmt.Sprintf("  case %s(\"%s\", request.Parent):", mc.gen.ident(resourcenamePkg, "Match"), parent.Value))
			g.P(fmt.Sprintf("    if err := %s(request.Parent, \"%s\", %s); err != nil {",
				mc.gen.ident(resourcenamePkg, "Sscan"), parent.Value, parent.VariableIDPtrs()))
			g.P(fmt.Sprintf("      return nil, %s(%s, \"invalid parent name: %%v\", err).Err()",
				mc.statusErrorf(), mc.codes("InvalidArgument")))
			g.P("    }")
		}
		g.P("  default:")
		g.P(fmt.Sprintf("    return nil, %s(%s, \"invalid parent name %%q\", request.Parent).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("  }")
		g.P()
	} else if mc.pattern.Parent != nil {
		parent := mc.pattern.Parent
		g.P("// Parse parent names")
		g.P(fmt.Sprintf("  var %s string", parent.VariableIDs(true)))
		g.P(fmt.Sprintf("  if err := %s(request.Parent, \"%s\", %s); err != nil {",
			mc.gen.ident(resourcenamePkg, "Sscan"), parent.Value, parent.VariableIDPtrs()))
		g.P(fmt.Sprintf("    return nil, %s(%s, \"invalid parent name: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("  }")
		g.P()
	}

	g.P("  // Parse request")
	g.P(fmt.Sprintf("  parsedRequest, err := %sParser.Parse(request)",
		xstrings.ToCamelCase(method.Input.GoIdent.GoName)))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, err.Error()).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P("  whereClause, whereParams := parsedRequest.GetSQLWhereClause()")
	g.P("  var dbColumns []string")
	g.P()

	// Retrieve from the database.
	g.P("  // Retrieve from the database.")
	dbName := "db" + resourceGoName
	searchArgs := "ctx, "
	if parentIDNames := mc.parentIDNames(); len(parentIDNames) > 0 {
		searchArgs += strings.Join(parentIDNames, ", ") + ", "
	}
	if mc.softDeletable {
		searchArgs += "request.ShowDeleted, "
	}
	searchArgs += "parsedRequest.GetTSQuery(), whereClause, parsedRequest.GetSQLPaginationClause(), dbColumns, whereParams..."
	if hasSnippets {
		g.P(fmt.Sprintf("  %ss, dbSnippets, err := s.store.%s(%s)", dbName, method.GoName, searchArgs))
	} else {
		g.P(fmt.Sprintf("  %ss, err := s.store.%s(%s)", dbName, method.GoName, searchArgs))
	}
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(err, \"searching %s\").Err()",
		mc.statusFromError(), xstrings.ToSnakeCase(pr.PluralGoName())))
	g.P("  }")
	g.P(fmt.Sprintf("  nextPageToken := parsedRequest.GetNextPageToken(len(%ss))", dbName))
	g.P("  if nextPageToken != \"\" {")
	g.P(fmt.Sprintf("    %ss = %ss[:len(%ss)-1]", dbName, dbName, dbName))
	if hasSnippets {
		g.P("    dbSnippets = dbSnippets[:len(dbSnippets)-1]")
	}
	g.P("  }")
	g.P()

	if hasSnippets {
		// Snippets are index-aligned with the resource list; empty when the
		// query was empty (list semantics).
		g.P(fmt.Sprintf("  snippets := make([]*%s, len(%ss))", mc.gen.ident(aipGenPkg, "SearchSnippet"), dbName))
		g.P("  for i := range snippets {")
		g.P("    var fields map[string]string")
		g.P("    if dbSnippets != nil {")
		g.P("      fields = dbSnippets[i]")
		g.P("    }")
		g.P(fmt.Sprintf("    snippets[i] = &%s{Fields: fields}", mc.gen.ident(aipGenPkg, "SearchSnippet")))
		g.P("  }")
		g.P()
	}

	// Convert back to proto.
	g.P("  // Convert back to proto.")
	g.P(fmt.Sprintf("  %s := make([]*%s, 0, len(%ss))",
		xstrings.ToCamelCase(pr.PluralGoName()), mc.protoType(), dbName))
	g.P(fmt.Sprintf("  for _, %s := range %ss {", dbName, dbName))
	g.P(fmt.Sprintf("    %s, err := %s.ToPb()", xstrings.ToCamelCase(resourceGoName), dbName))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"converting model.%s to %s: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), resourceGoName, resourceGoName))
	g.P("    }")
	g.P(fmt.Sprintf("    %s = append(%s, %s)",
		xstrings.ToCamelCase(pr.PluralGoName()), xstrings.ToCamelCase(pr.PluralGoName()), xstrings.ToCamelCase(resourceGoName)))
	g.P("  }")
	g.P()

	// Return response.
	g.P("  // Create and return response.")
	g.P(fmt.Sprintf("  return &%s{", mc.gen.qgi(method.Output.GoIdent)))
	g.P(fmt.Sprintf("    %s: %s,", pr.PluralGoName(), xstrings.ToCamelCase(pr.PluralGoName())))
	if hasSnippets {
		g.P("    Snippets: snippets,")
	}
	g.P("    NextPageToken: nextPageToken,")
	g.P("  }, nil")
	g.P("}")
	g.P()
	return nil
}

package rpc

import (
	"fmt"

	"github.com/huandu/xstrings"
)

func (mc *methodCtx) generateList() {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName

	// List request parser.
	g.P(fmt.Sprintf("var %sParser = %s[*%s, *%s](%s(%s()), %s(%s()))",
		xstrings.ToCamelCase(method.Input.GoIdent.GoName),
		mc.gen.ident(aipPkg, "MustNewListRequestParser"), mc.inputType(), mc.protoType(),
		mc.gen.ident(aipPkg, "WithFilteringOpts"),
		mc.gen.ident(aipPkg, "WithFQN"),
		mc.gen.ident(aipPkg, "WithOrderingOpts"),
		mc.gen.ident(aipPkg, "WithOrderingFQN"),
	))
	g.P()

	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	if mc.pattern.Parent != nil {
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
	listArgs := "ctx, "
	if mc.pattern.Parent != nil {
		listArgs += mc.pattern.Parent.VariableIDs(true) + ", "
	}
	if mc.softDeletable {
		listArgs += "request.ShowDeleted, "
	}
	listArgs += "whereClause, parsedRequest.GetSQLOrderByClause(), parsedRequest.GetSQLPaginationClause(), dbColumns, whereParams..."
	g.P(fmt.Sprintf("  %ss, err := s.store.%s(%s)", dbName, method.GoName, listArgs))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(err, \"listing %s\").Err()",
		mc.statusFromError(), xstrings.ToSnakeCase(pr.PluralGoName())))
	g.P("  }")
	g.P(fmt.Sprintf("  nextPageToken := parsedRequest.GetNextPageToken(len(%ss))", dbName))
	g.P("  if nextPageToken != \"\" {")
	g.P(fmt.Sprintf("    %ss = %ss[:len(%ss)-1]", dbName, dbName, dbName))
	g.P("  }")
	g.P()

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
	g.P("    NextPageToken: nextPageToken,")
	g.P("  }, nil")
	g.P("}")
	g.P()
}

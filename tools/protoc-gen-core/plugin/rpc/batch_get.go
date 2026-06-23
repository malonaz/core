package rpc

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
)

func (mc *methodCtx) generateBatchGet() error {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName

	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	if pr.Parent != nil {
		g.P(fmt.Sprintf("  var %s string", pr.Parent.PatternVariableIDs(true)))
		g.P("  if request.Parent != \"\" {")
		g.P(fmt.Sprintf("    if err := %s(request.Parent, \"%s\", %s); err != nil {",
			mc.gen.ident(resourcenamePkg, "Sscan"), pr.Parent.Pattern, pr.Parent.PatternVariableIDPtrs()))
		g.P(fmt.Sprintf("      return nil, %s(%s, \"invalid parent name: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("    }")
		g.P("  }")
		g.P()
	}

	for _, v := range pr.PatternVariables {
		camel := xstrings.ToCamelCase(v)
		g.P(fmt.Sprintf("  %sIds := make([]string, len(request.GetNames()))", camel))
	}
	g.P()

	g.P("  for i, name := range request.Names {")
	g.P(fmt.Sprintf("    if %s(name) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard")))
	g.P(fmt.Sprintf("      return nil, %s(%s, \"name cannot contain wildcard\").Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("    }")

	if pr.Parent != nil {
		g.P("    if request.Parent != \"\" && !" + mc.gen.ident(resourcenamePkg, "HasParent") + "(name, request.Parent) {")
		g.P(fmt.Sprintf("      return nil, %s(%s, \"name %%q does not have parent %%q\", name, request.Parent).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("    }")
	}

	g.P(fmt.Sprintf("    %s, err := %s(name)", pr.PatternVariableIDs(true), mc.parseName))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"parsing name %%s: %%v\", name, err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("    }")

	for _, v := range pr.PatternVariables {
		camel := xstrings.ToCamelCase(v)
		g.P(fmt.Sprintf("    %sIds[i] = %sId", camel, camel))
	}

	g.P("  }")
	g.P()

	var storeArgs []string
	storeArgs = append(storeArgs, "ctx")
	for _, v := range pr.PatternVariables {
		camel := xstrings.ToCamelCase(v)
		storeArgs = append(storeArgs, camel+"Ids")
	}
	g.P(fmt.Sprintf("  db%s, err := s.store.BatchGet%s(%s)",
		pr.PluralGoName(), pr.PluralGoName(), strings.Join(storeArgs, ", ")))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(err, \"batch getting %s\").Err()",
		mc.statusFromError(), xstrings.ToSnakeCase(resourceGoName)))
	g.P("  }")
	g.P(fmt.Sprintf("  if len(db%s) != len(request.Names) {", pr.PluralGoName()))
	g.P(fmt.Sprintf("    return nil, %s(%s, \"expected %%d %s, found %%d\", len(request.Names), len(db%s)).Err()",
		mc.statusErrorf(), mc.codes("NotFound"), pr.Desc.Plural, pr.PluralGoName()))
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  %sNameTo%s := make(map[string]*%s, len(request.Names))",
		xstrings.ToCamelCase(resourceGoName), resourceGoName, mc.protoType()))
	g.P(fmt.Sprintf("  for _, db%s := range db%s {", mc.modelGoName, pr.PluralGoName()))
	g.P(fmt.Sprintf("    %s, err := db%s.ToPb()", xstrings.ToCamelCase(resourceGoName), mc.modelGoName))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    %sNameTo%s[%s.Name] = %s",
		xstrings.ToCamelCase(resourceGoName), resourceGoName, xstrings.ToCamelCase(resourceGoName), xstrings.ToCamelCase(resourceGoName)))
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  %s := make([]*%s, 0, len(db%s))",
		xstrings.ToCamelCase(pr.PluralGoName()), mc.protoType(), pr.PluralGoName()))
	g.P("  for _, name := range request.Names {")
	g.P(fmt.Sprintf("    %s, ok := %sNameTo%s[name]",
		xstrings.ToCamelCase(resourceGoName), xstrings.ToCamelCase(resourceGoName), resourceGoName))
	g.P("    if !ok {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"could not find %%q\", name).Err()",
		mc.statusErrorf(), mc.codes("NotFound")))
	g.P("    }")
	g.P(fmt.Sprintf("    %s = append(%s, %s)",
		xstrings.ToCamelCase(pr.PluralGoName()), xstrings.ToCamelCase(pr.PluralGoName()), xstrings.ToCamelCase(resourceGoName)))
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  return &%s{", mc.gen.qgi(method.Output.GoIdent)))
	g.P(fmt.Sprintf("    %s: %s,", pr.PluralGoName(), xstrings.ToCamelCase(pr.PluralGoName())))
	g.P("  }, nil")
	g.P("}")
	g.P()
	return nil
}

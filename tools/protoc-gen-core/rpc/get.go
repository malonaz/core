package rpc

import (
	"fmt"

	"github.com/huandu/xstrings"
)

func (mc *methodCtx) generateGet() {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName

	g.P(fmt.Sprintf("func (s *%s) Get%s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, resourceGoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	g.P(fmt.Sprintf("  if %s(request.Name) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard")))
	g.P(fmt.Sprintf("    return nil, %s(%s, \"cannot use wildcard\").Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  %s, err := %s(request.Name)", pr.PatternVariableIDs(true), mc.parseName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"parsing name: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P()

	g.P("  // Retrieve from the database.")
	g.P(fmt.Sprintf("  db%s, err := s.store.%s(ctx, %s)",
		mc.modelGoName, method.GoName, pr.PatternVariableIDs(true)))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errNotExist))
	g.P(fmt.Sprintf("      return nil, %s(%s, \"%s does not exist\").Err()",
		mc.statusErrorf(), mc.codes("NotFound"), pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    return nil, %s(err, \"getting %s\").Err()",
		mc.statusFromError(), pr.Desc.Singular))
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  %s, err := db%s.ToPb()", xstrings.ToCamelCase(resourceGoName), mc.modelGoName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("  }")
	g.P(fmt.Sprintf("  return %s, nil", xstrings.ToCamelCase(resourceGoName)))
	g.P("}")
	g.P()
}

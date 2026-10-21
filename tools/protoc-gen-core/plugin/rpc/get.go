package rpc

import (
	"fmt"

	"github.com/malonaz/core/tools/protoc-gen-core/goident"
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

	g.P(fmt.Sprintf("  %s, err := %s(request.Name)", mc.idParams(), mc.parseName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"parsing name: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P()

	g.P("  // Retrieve from the database.")
	g.P(fmt.Sprintf("  db%s, err := s.store.%s(ctx, %s)",
		mc.modelGoName, method.GoName, mc.idParams()))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errNotExist))
	if mc.lazy {
		g.P(fmt.Sprintf("      return s.default%s(request.Name)", resourceGoName))
	} else {
		g.P(fmt.Sprintf("      return nil, %s(%s, \"%s does not exist\").Err()",
			mc.statusErrorf(), mc.codes("NotFound"), pr.Desc.Singular))
	}
	g.P("    }")
	g.P(fmt.Sprintf("    return nil, %s(err, \"getting %s\").Err()",
		mc.statusFromError(), pr.Desc.Singular))
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  %s, err := db%s.ToPb()", goident.Var(resourceGoName), mc.modelGoName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("  }")
	g.P(fmt.Sprintf("  return %s, nil", goident.Var(resourceGoName)))
	g.P("}")
	g.P()
}

// generateDefault emits default{R}: a lazy singleton as it reads until its
// first update, with the etag that update must match.
func (mc *methodCtx) generateDefault() {
	g := mc.g
	resourceVar := goident.Var(mc.resourceGoName)

	g.P(fmt.Sprintf("func (s *%s) default%s(name string) (*%s, error) {", mc.serverGoName, mc.resourceGoName, mc.protoType()))
	g.P(fmt.Sprintf("  %s := &%s{Name: name}", resourceVar, mc.protoType()))
	if mc.hasEtag {
		g.P("  var err error")
		g.P(fmt.Sprintf("  if %s.Etag, err = %s(%s); err != nil {", resourceVar, mc.gen.ident(aipPkg, "ComputeETag"), resourceVar))
		g.P(fmt.Sprintf("    return nil, %s(%s, \"computing default %s etag: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), mc.pr.Desc.Singular))
		g.P("  }")
	}
	g.P(fmt.Sprintf("  return %s, nil", resourceVar))
	g.P("}")
	g.P()
}

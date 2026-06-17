package rpc

import (
	"fmt"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"
)

func (mc *methodCtx) generateDelete() error {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName

	// Publish helper for deleted events.
	if mc.mi.natsEventOpts != nil && len(mc.mi.natsEventOpts.GetDeleted()) > 0 {
		mc.generateDeletedEventPublisher()
	}

	g.P(fmt.Sprintf("func (s *%s) Delete%s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, resourceGoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	// STEP 1: Parse resource name.
	g.P("  // STEP 1: Parse resource name.")
	g.P(fmt.Sprintf("  %s, err := %s(request.Name)", pr.PatternVariableIDs(true), mc.parseName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"parsing name: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P()

	if mc.softDeletable {
		mc.generateSoftDeleteBody(method)
	} else {
		mc.generateHardDeleteBody(method)
	}

	g.P("}")
	g.P()
	return nil
}

func (mc *methodCtx) generateSoftDeleteBody(method *protogen.Method) {
	g := mc.g
	pr := mc.pr
	resourceGoName := mc.resourceGoName
	hasDeletedEvents := mc.mi.natsEventOpts != nil && len(mc.mi.natsEventOpts.GetDeleted()) > 0

	g.P(fmt.Sprintf("  deleteTime := %s().UTC()", mc.gen.ident(timePkg, "Now")))

	if mc.hasEtag {
		g.P("// Compute the new Etag.")
		g.P(fmt.Sprintf("  get%sRequest := &%s{ Name: request.Name }",
			resourceGoName, mc.gen.fileIdent("Get"+resourceGoName+"Request")))
		g.P(fmt.Sprintf("  %s, err := s.Get%s(ctx, get%sRequest)",
			resourceGoName, resourceGoName, resourceGoName))
		g.P("  if err != nil {")
		g.P("    return nil, err")
		g.P("  }")
		g.P(fmt.Sprintf("  %s.DeleteTime = %s(deleteTime)",
			resourceGoName, mc.gen.ident(timestamppbPkg, "New")))
		g.P(fmt.Sprintf("  newEtag, err := %s(%s)",
			mc.gen.ident(aipPkg, "ComputeETag"), resourceGoName))
		g.P("  if err != nil {")
		g.P(fmt.Sprintf("    return nil, %s(%s, \"computing etag: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal")))
		g.P("  }")
		g.P()
	}

	// STEP 2: Soft delete.
	g.P("  // STEP 2: Soft delete the resource.")
	deleteArgs := fmt.Sprintf("ctx, %s", pr.PatternVariableIDs(true))
	if mc.hasEtag {
		deleteArgs += ", request.GetEtag(), newEtag"
	}
	deleteArgs += ", deleteTime"
	g.P(fmt.Sprintf("  db%s, err := s.store.SoftDelete%s(%s)", mc.modelGoName, resourceGoName, deleteArgs))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errNotExist))
	g.P(fmt.Sprintf("      return nil, %s(%s, \"%s does not exist\").Err()",
		mc.statusErrorf(), mc.codes("NotFound"), pr.Desc.Singular))
	g.P("    }")
	if mc.hasEtag {
		g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errEtagChanged))
		g.P(fmt.Sprintf("      return nil, %s(%s, \"ETag changed\").Err()",
			mc.statusErrorf(), mc.codes("Aborted")))
		g.P("    }")
	}
	g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errAlreadyDeleted))
	g.P("      if request.AllowMissing {")
	g.P(fmt.Sprintf("        get%sRequest := &%s{ Name: request.Name }",
		resourceGoName, mc.gen.fileIdent("Get"+resourceGoName+"Request")))
	g.P(fmt.Sprintf("        %s, err := s.Get%s(ctx, get%sRequest)",
		xstrings.ToCamelCase(resourceGoName), resourceGoName, resourceGoName))
	g.P("        if err != nil {")
	g.P("          return nil, err")
	g.P("        }")
	if hasDeletedEvents {
		g.P(fmt.Sprintf("        if err := s.publishResourceDeletedEvent(ctx, %s); err != nil {", xstrings.ToCamelCase(resourceGoName)))
		g.P("          return nil, err")
		g.P("        }")
	}
	g.P(fmt.Sprintf("        return %s, nil", xstrings.ToCamelCase(resourceGoName)))
	g.P("      }")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"%s already deleted\").Err()",
		mc.statusErrorf(), mc.codes("NotFound"), pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    return nil, %s(err, \"soft deleting %s\").Err()",
		mc.statusFromError(), pr.Desc.Singular))
	g.P("  }")
	g.P()

	// STEP 3: Convert to protobuf.
	g.P("  // STEP 3: Convert to protobuf and return.")
	g.P(fmt.Sprintf("  %s, err := db%s.ToPb()", xstrings.ToCamelCase(resourceGoName), mc.modelGoName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("  }")
	g.P()

	if hasDeletedEvents {
		g.P("  // STEP 4: Publish event.")
		g.P(fmt.Sprintf("  if err := s.publishResourceDeletedEvent(ctx, %s); err != nil {", xstrings.ToCamelCase(resourceGoName)))
		g.P("    return nil, err")
		g.P("  }")
		g.P()
	}

	g.P(fmt.Sprintf("  return %s, nil", xstrings.ToCamelCase(resourceGoName)))
}

func (mc *methodCtx) generateHardDeleteBody(method *protogen.Method) {
	g := mc.g
	pr := mc.pr
	resourceGoName := mc.resourceGoName
	hasDeletedEvents := mc.mi.natsEventOpts != nil && len(mc.mi.natsEventOpts.GetDeleted()) > 0

	// STEP 2: Hard delete.
	g.P("  // STEP 2: Hard delete the resource.")
	deleteArgs := "ctx"
	if len(pr.PatternVariables) > 0 {
		deleteArgs += ", " + pr.PatternVariableIDs(true)
	}
	if mc.hasEtag {
		deleteArgs += ", request.GetEtag()"
	}

	if hasDeletedEvents {
		g.P(fmt.Sprintf("  db%s, err := s.store.Delete%s(%s)", mc.modelGoName, resourceGoName, deleteArgs))
	} else {
		g.P(fmt.Sprintf("  _, err = s.store.Delete%s(%s)", resourceGoName, deleteArgs))
	}
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errNotExist))
	g.P("      if request.AllowMissing {")
	g.P(fmt.Sprintf("        return &%s{}, nil", mc.gen.ident(emptypbPkg, "Empty")))
	g.P("      }")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"%s does not exist\").Err()",
		mc.statusErrorf(), mc.codes("NotFound"), pr.Desc.Singular))
	g.P("    }")
	if mc.hasEtag {
		g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errEtagChanged))
		g.P(fmt.Sprintf("      return nil, %s(%s, \"ETag changed\").Err()",
			mc.statusErrorf(), mc.codes("Aborted")))
		g.P("    }")
	}
	g.P(fmt.Sprintf("    return nil, %s(err, \"deleting %s\").Err()",
		mc.statusFromError(), pr.Desc.Singular))
	g.P("  }")
	g.P()

	if hasDeletedEvents {
		g.P("  // STEP 5: Publish event.")
		g.P(fmt.Sprintf("  %s, err := db%s.ToPb()", xstrings.ToCamelCase(resourceGoName), mc.modelGoName))
		g.P("  if err != nil {")
		g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
		g.P("  }")
		g.P(fmt.Sprintf("  if err := s.publishResourceDeletedEvent(ctx, %s); err != nil {", xstrings.ToCamelCase(resourceGoName)))
		g.P("    return nil, err")
		g.P("  }")
		g.P()
	}

	g.P(fmt.Sprintf("  return &%s{}, nil", mc.gen.ident(emptypbPkg, "Empty")))
}

func (mc *methodCtx) generateDeletedEventPublisher() {
	g := mc.g
	resourceGoName := mc.resourceGoName

	g.P(fmt.Sprintf("func (s *%s) publishResourceDeletedEvent(ctx %s, %s *%s) error {",
		mc.serverGoName, mc.gen.ident(contextPkg, "Context"), xstrings.ToCamelCase(resourceGoName), mc.protoType()))
	for _, eventOpt := range mc.mi.natsEventOpts.GetDeleted() {
		subject := eventOpt.GetSubject()
		g.P("  {")
		g.P(fmt.Sprintf("    subject := %s().Get%sSubject()",
			mc.gen.resourcePkgIdent(mc.mi.rpc.Message, "Get"+mc.natsStreamGoName),
			xstrings.ToPascalCase(subject)))
		g.P(fmt.Sprintf("    if err := subject.Publish(ctx, s.natsClient, %s); err != nil {", xstrings.ToCamelCase(resourceGoName)))
		g.P(fmt.Sprintf("      return %s(%s, \"publishing %s event: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), subject))
		g.P("    }")
		g.P("  }")
	}
	g.P("  return nil")
	g.P("}")
	g.P()
}

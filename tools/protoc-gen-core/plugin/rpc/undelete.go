package rpc

import (
	"fmt"

	"github.com/huandu/xstrings"
)

// generateUndelete emits Undelete{R} (AIP-164): clears the tombstone and
// returns the restored resource; a live resource is ALREADY_EXISTS.
func (mc *methodCtx) generateUndelete() error {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName
	resourceVar := xstrings.ToCamelCase(resourceGoName)

	if method.Input.Desc.Fields().ByName("name") == nil {
		return fmt.Errorf("%s must declare `string name` (AIP-164)", method.Input.Desc.FullName())
	}
	if mc.hasEtag && method.Input.Desc.Fields().ByName("etag") == nil {
		return fmt.Errorf("%s has an etag: %s must declare `string etag` (AIP-154)", pr.Desc.Type, method.Input.Desc.FullName())
	}

	hasUndeletedEvents := mc.mi.natsEventOpts != nil && len(mc.mi.natsEventOpts.GetUndeleted()) > 0
	if hasUndeletedEvents {
		mc.generateUndeletedEventPublisher()
	}

	g.P(fmt.Sprintf("func (s *%s) Undelete%s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, resourceGoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	g.P(fmt.Sprintf("  if %s(request.Name) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard")))
	g.P(fmt.Sprintf("    return nil, %s(%s, \"cannot use wildcard\").Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P()

	g.P("  // STEP 1: Parse resource name.")
	g.P(fmt.Sprintf("  %s, err := %s(request.Name)", mc.idParams(), mc.parseName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"parsing name: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P()

	undeleteArgs := fmt.Sprintf("ctx, %s", mc.idParams())
	if mc.hasEtag {
		// The etag covers the whole resource, so it is recomputed over the
		// restored (tombstone-free) state.
		g.P("  // Compute the new etag.")
		g.P(fmt.Sprintf("  get%sRequest := &%s{ Name: request.Name }",
			resourceGoName, mc.gen.fileIdent("Get"+resourceGoName+"Request")))
		g.P(fmt.Sprintf("  %s, err := s.Get%s(ctx, get%sRequest)", resourceVar, resourceGoName, resourceGoName))
		g.P("  if err != nil {")
		g.P("    return nil, err")
		g.P("  }")
		g.P(fmt.Sprintf("  %s.DeleteTime = nil", resourceVar))
		g.P(fmt.Sprintf("  newEtag, err := %s(%s)", mc.gen.ident(aipPkg, "ComputeETag"), resourceVar))
		g.P("  if err != nil {")
		g.P(fmt.Sprintf("    return nil, %s(%s, \"computing etag: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal")))
		g.P("  }")
		g.P()
		undeleteArgs += ", request.GetEtag(), newEtag"
	}

	g.P("  // STEP 2: Undelete the resource.")
	g.P(fmt.Sprintf("  db%s, err := s.store.Undelete%s(%s)", mc.modelGoName, resourceGoName, undeleteArgs))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errNotExist))
	g.P(fmt.Sprintf("      return nil, %s(%s, \"%s does not exist\").Err()",
		mc.statusErrorf(), mc.codes("NotFound"), pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errNotDeleted))
	g.P(fmt.Sprintf("      return nil, %s(%s, \"%s is not deleted\").Err()",
		mc.statusErrorf(), mc.codes("AlreadyExists"), pr.Desc.Singular))
	g.P("    }")
	if mc.hasEtag {
		g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errEtagChanged))
		g.P(fmt.Sprintf("      return nil, %s(%s, \"ETag changed\").Err()",
			mc.statusErrorf(), mc.codes("Aborted")))
		g.P("    }")
	}
	g.P(fmt.Sprintf("    return nil, %s(err, \"undeleting %s\").Err()",
		mc.statusFromError(), pr.Desc.Singular))
	g.P("  }")
	g.P()

	g.P("  // STEP 3: Convert to protobuf and return.")
	if mc.hasEtag {
		g.P(fmt.Sprintf("  %s, err = db%s.ToPb()", resourceVar, mc.modelGoName))
	} else {
		g.P(fmt.Sprintf("  %s, err := db%s.ToPb()", resourceVar, mc.modelGoName))
	}
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("  }")
	g.P()

	if hasUndeletedEvents {
		g.P("  // STEP 4: Publish event.")
		g.P(fmt.Sprintf("  if err := s.publishResourceUndeletedEvent(ctx, %s); err != nil {", resourceVar))
		g.P("    return nil, err")
		g.P("  }")
		g.P()
	}

	g.P(fmt.Sprintf("  return %s, nil", resourceVar))
	g.P("}")
	g.P()
	return nil
}

func (mc *methodCtx) generateUndeletedEventPublisher() {
	g := mc.g
	resourceVar := xstrings.ToCamelCase(mc.resourceGoName)

	g.P(fmt.Sprintf("func (s *%s) publishResourceUndeletedEvent(ctx %s, %s *%s) error {",
		mc.serverGoName, mc.gen.ident(contextPkg, "Context"), resourceVar, mc.protoType()))
	for _, eventOpt := range mc.mi.natsEventOpts.GetUndeleted() {
		subject := eventOpt.GetSubject()
		g.P("  {")
		g.P(fmt.Sprintf("    subject := %s().Get%sSubject()",
			mc.gen.resourcePkgIdent(mc.mi.rpc.Message, "Get"+mc.natsStreamGoName),
			xstrings.ToPascalCase(subject)))
		g.P(fmt.Sprintf("    if err := subject.Publish(ctx, s.natsClient, %s); err != nil {", resourceVar))
		g.P(fmt.Sprintf("      return %s(%s, \"publishing %s event: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), subject))
		g.P("    }")
		g.P("  }")
	}
	g.P("  return nil")
	g.P("}")
	g.P()
}

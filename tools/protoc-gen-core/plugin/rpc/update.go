package rpc

import (
	"fmt"

	"github.com/huandu/xstrings"
)

func (mc *methodCtx) generateUpdate() error {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName

	hasPrecondition := method.Input.Desc.Fields().ByName("precondition") != nil

	// Update request parser variable.
	g.P(fmt.Sprintf("var update%sRequestParser = %s[*%s, *%s]()",
		resourceGoName, mc.gen.ident(aipPkg, "MustNewUpdateRequestParser"), mc.inputType(), mc.protoType()))
	g.P()

	// CEL environment for preconditions.
	if hasPrecondition {
		g.P(fmt.Sprintf("var update%sCELEnv = func() *%s {", resourceGoName, mc.gen.ident(celPkg, "Env")))
		g.P(fmt.Sprintf("  env, err := %s(", mc.gen.ident(celPkg, "NewEnv")))
		g.P(fmt.Sprintf("    %s(),", mc.gen.ident(celExtPkg, "Protos")))
		g.P(fmt.Sprintf("    %s(&%s{}),", mc.gen.ident(celPkg, "Types"), mc.protoType()))
		g.P(fmt.Sprintf("    %s(\"previous_%s\", %s(\"%s\")),",
			mc.gen.ident(celPkg, "Variable"), xstrings.ToSnakeCase(mc.mi.rpc.Message.GoIdent.GoName),
			mc.gen.ident(celPkg, "ObjectType"), mc.mi.rpc.Message.Desc.FullName()))
		g.P(fmt.Sprintf("    %s(\"%s\", %s(\"%s\")),",
			mc.gen.ident(celPkg, "Variable"), xstrings.ToSnakeCase(mc.mi.rpc.Message.GoIdent.GoName),
			mc.gen.ident(celPkg, "ObjectType"), mc.mi.rpc.Message.Desc.FullName()))
		g.P("  )")
		g.P("  if err != nil {")
		g.P(fmt.Sprintf("    panic(%s(\"creating update %s CEL environment: %%v\", err))",
			mc.fmtSprintf(), resourceGoName))
		g.P("  }")
		g.P("  return env")
		g.P("}()")
		g.P()
	}

	// Retry wrapper when etag is present.
	if mc.hasEtag {
		mc.generateUpdateRetryWrapper()
	}

	// Main update implementation.
	funcName := method.GoName
	if mc.hasEtag {
		funcName = xstrings.ToCamelCase(method.GoName)
	}
	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, funcName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	g.P("  if len(request.GetUpdateMask().GetPaths()) == 0 {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"missing update_mask.paths\").Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P(fmt.Sprintf("  if %s(request.%s.Name) {",
		mc.gen.ident(resourcenamePkg, "ContainsWildcard"), resourceGoName))
	g.P(fmt.Sprintf("    return nil, %s(%s, \"cannot use wildcard\").Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P()

	// STEP 1: Parse request.
	g.P("  // STEP 1: Parse request.")
	g.P(fmt.Sprintf("  parsedRequest, err := update%sRequestParser.Parse(request)", resourceGoName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"parsing request: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P()

	// STEP 2: Retrieve existing resource.
	g.P("  // STEP 2: retrieve existing resource.")
	g.P(fmt.Sprintf("  get%sRequest := &%s{ Name: request.%s.Name }",
		resourceGoName, mc.gen.fileIdent("Get"+resourceGoName+"Request"), resourceGoName))
	g.P(fmt.Sprintf("  existing%s, err := s.Get%s(ctx, get%sRequest)",
		resourceGoName, resourceGoName, resourceGoName))
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")

	if mc.softDeletable {
		g.P("// Verify that the resource is not soft deleted")
		g.P(fmt.Sprintf("  if existing%s.DeleteTime != nil {", resourceGoName))
		g.P(fmt.Sprintf("    return nil, %s(%s, \"%s does not exist\").Err()",
			mc.statusErrorf(), mc.codes("NotFound"), pr.Desc.Singular))
		g.P("  }")
	}

	if mc.hasEtag {
		g.P("// Capture the Etag. If it is not set, use the latest available Etag.")
		g.P(fmt.Sprintf("  etag := request.Get%s().GetEtag()", resourceGoName))
		g.P("  if etag == \"\" {")
		g.P(fmt.Sprintf("    etag = existing%s.GetEtag()", resourceGoName))
		g.P("  }")
	}
	g.P()

	// STEP 3: Patch.
	g.P("  // STEP 3: Patch the existing resource.")
	g.P(fmt.Sprintf("  patched%s := %s(existing%s)",
		resourceGoName, mc.gen.ident(protoPkg, "CloneOf"), resourceGoName))
	g.P(fmt.Sprintf("  parsedRequest.ApplyFieldMask(patched%s, request.%s)", resourceGoName, resourceGoName))
	g.P(fmt.Sprintf("  if err := %s(patched%s); err != nil {",
		mc.gen.ident(protovalidate, "Validate"), resourceGoName))
	g.P(fmt.Sprintf("    return nil, %s(%s, \"validating patched resource: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P()

	// Precondition evaluation.
	if hasPrecondition {
		mc.generatePreconditionCheck(resourceGoName)
	}

	// Update time.
	if mc.mi.rpc.Message.Desc.Fields().ByName("update_time") != nil {
		g.P(" // Set the update time.")
		g.P(fmt.Sprintf("  patched%s.UpdateTime = %s()",
			resourceGoName, mc.gen.ident(timestamppbPkg, "Now")))
	}

	// New etag.
	if mc.hasEtag {
		g.P("  { // Compute the new Etag.")
		g.P("    var err error")
		g.P(fmt.Sprintf("    patched%s.Etag, err = %s(patched%s)",
			resourceGoName, mc.gen.ident(aipPkg, "ComputeETag"), resourceGoName))
		g.P("    if err != nil {")
		g.P(fmt.Sprintf("      return nil, %s(%s, \"computing new etag: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal")))
		g.P("    }")
		g.P("  }")
		g.P()
	}

	// STEP 4: Persist.
	g.P("  // STEP 4: Insert patched resource.")
	g.P(fmt.Sprintf("  %s, err := %s(patched%s)",
		xstrings.ToCamelCase(mc.modelGoName), mc.parseFromPb, resourceGoName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from pb to model: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("  }")

	updateArgs := fmt.Sprintf("ctx, %s, parsedRequest.GetSQLUpdateClause(), parsedRequest.GetSQLColumns()", xstrings.ToCamelCase(mc.modelGoName))
	if mc.hasEtag {
		updateArgs += ", etag"
	}
	g.P(fmt.Sprintf("  db%s, err := s.store.Update%s(%s)", mc.modelGoName, resourceGoName, updateArgs))
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
	g.P(fmt.Sprintf("    return nil, %s(err, \"updating %s\").Err()",
		mc.statusFromError(), pr.Desc.Singular))
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  %s, err := db%s.ToPb()", xstrings.ToCamelCase(resourceGoName), mc.modelGoName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("  }")
	g.P()

	// STEP 5: Publish events.
	mc.generateUpdatedEvents(xstrings.ToCamelCase(resourceGoName), "existing"+resourceGoName)

	g.P(fmt.Sprintf("  return %s, nil", xstrings.ToCamelCase(resourceGoName)))
	g.P("}")
	g.P()
	return nil
}

func (mc *methodCtx) generateUpdateRetryWrapper() {
	g := mc.g
	method := mc.mi.method
	resourceGoName := mc.resourceGoName

	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))
	g.P("  for {")
	g.P(fmt.Sprintf("    response, err := s.%s(ctx, request)", xstrings.ToCamelCase(method.GoName)))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      if request.Get%s().GetEtag() == \"\" && %s(err, %s) {",
		resourceGoName, mc.statusHasCode(), mc.codes("Aborted")))
	g.P("// Request did not specify an ETag => we retry.")
	g.P("// In order to understand why we still use ETag in the db layer, consider the following situation:")
	g.P("//  > `resource.metadata` is stored as JSONB in the store.")
	g.P("//  > Request A wants to update `resource.metadata.field1` and does not care about ETag.")
	g.P("//  > Request B wants to update `resource.metadata.field2` and does not care about ETag.")
	g.P("//  > Request A reads the resource and patches it.")
	g.P("//  > Request B reads the resource and patches it.")
	g.P("//  > Request A persists the patched resource, followed by Request B.")
	g.P("//  > Request A's changes are lost.")
	g.P("        select {")
	g.P("        case <-ctx.Done():")
	g.P(fmt.Sprintf("          return nil, %s(%s, \"context canceled while retrying update\").Err()",
		mc.statusErrorf(), mc.codes("Canceled")))
	g.P("        default:")
	g.P("          continue")
	g.P("        }")
	g.P("      }")
	g.P("      return nil, err")
	g.P("    }")
	g.P("    return response, nil")
	g.P("  }")
	g.P("}")
	g.P()
}

func (mc *methodCtx) generatePreconditionCheck(resourceGoName string) {
	g := mc.g

	g.P("  if request.Precondition != \"\" {")
	g.P(fmt.Sprintf("    ast, issues := update%sCELEnv.Compile(request.Precondition)", resourceGoName))
	g.P("    if issues != nil && issues.Err() != nil {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"compiling precondition: %%v\", issues.Err()).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("    }")
	g.P(fmt.Sprintf("    if ast.OutputType() != %s {", mc.gen.ident(celPkg, "BoolType")))
	g.P(fmt.Sprintf("      return nil, %s(%s, \"precondition must return bool, got %%v\", ast.OutputType()).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("    }")
	g.P(fmt.Sprintf("    prg, err := update%sCELEnv.Program(ast)", resourceGoName))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"creating precondition program: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("    }")
	g.P("    out, _, err := prg.Eval(map[string]any{")
	g.P(fmt.Sprintf("      \"previous_%s\": existing%s,",
		xstrings.ToSnakeCase(mc.mi.rpc.Message.GoIdent.GoName), resourceGoName))
	g.P(fmt.Sprintf("      \"%s\": patched%s,",
		xstrings.ToSnakeCase(mc.mi.rpc.Message.GoIdent.GoName), resourceGoName))
	g.P("    })")
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"evaluating precondition: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("    }")
	g.P("    result, ok := out.Value().(bool)")
	g.P("    if !ok {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"precondition returned non-bool type %%T\", out.Value()).Err()",
		mc.statusErrorf(), mc.codes("Internal")))
	g.P("    }")
	g.P("    if !result {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"precondition not met\").Err()",
		mc.statusErrorf(), mc.codes("FailedPrecondition")))
	g.P("    }")
	g.P("  }")
	g.P()
}

func (mc *methodCtx) generateUpdatedEvents(resourceVar, existingVar string) {
	if mc.mi.natsEventOpts == nil || len(mc.mi.natsEventOpts.GetUpdated()) == 0 {
		return
	}
	mc.g.P("  // STEP 5: Publish events.")
	for _, eventOpt := range mc.mi.natsEventOpts.GetUpdated() {
		subject := eventOpt.GetSubject()
		mc.g.P("  {")
		mc.g.P(fmt.Sprintf("    subject := %s().Get%sSubject()",
			mc.gen.resourcePkgIdent(mc.mi.rpc.Message, "Get"+mc.natsStreamGoName),
			xstrings.ToPascalCase(subject)))
		mc.g.P(fmt.Sprintf("    if err := subject.Publish(ctx, s.natsClient, %s, %s, request.GetUpdateMask()); err != nil {",
			resourceVar, existingVar))
		mc.g.P(fmt.Sprintf("      return nil, %s(%s, \"publishing %s event: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), subject))
		mc.g.P("    }")
		mc.g.P("  }")
	}
}

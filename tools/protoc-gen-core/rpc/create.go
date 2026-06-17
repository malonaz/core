package rpc

import (
	"fmt"

	"github.com/huandu/xstrings"

	"github.com/malonaz/core/tools/protoc-gen-core/resource"
)

func (mc *methodCtx) generateCreate() error {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName

	hasRequestID := method.Input.Desc.Fields().ByName("request_id") != nil

	singletonChildren, err := getSingletonChildren(pr)
	if err != nil {
		return err
	}

	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	// STEP 1: Set identifiers.
	g.P("  // STEP 1: Set identifiers.")
	if hasRequestID {
		g.P("  if request.RequestId == \"\" { // We always set a request id")
		g.P(fmt.Sprintf("    request.RequestId = %s().String()", mc.gen.ident(uuidPkg, "MustNewV7")))
		g.P("  }")
	}

	patternVarID, err := pr.PatternVariableID(true)
	if err != nil {
		return err
	}
	g.P(fmt.Sprintf("  %s := request.%sId", patternVarID, resourceGoName))
	g.P(fmt.Sprintf("  if %s == \"\" {", patternVarID))
	g.P(fmt.Sprintf("    %s = %s()", patternVarID, mc.gen.ident(aipPkg, "NewSystemGeneratedBase32ResourceID")))
	g.P("  }")
	g.P()

	if pr.Parent != nil {
		g.P(fmt.Sprintf("  var %s string", pr.Parent.PatternVariableIDs(true)))
		g.P(fmt.Sprintf("  if err := %s(request.Parent, \"%s\", %s); err != nil {",
			mc.gen.ident(resourcenamePkg, "Sscan"), pr.Parent.Pattern, pr.Parent.PatternVariableIDPtrs()))
		g.P(fmt.Sprintf("    return nil, %s(%s, \"invalid parent name: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("  }")
		g.P()
	}

	g.P(fmt.Sprintf("  request.%s.Name = %s(\"%s\", %s)",
		resourceGoName, mc.gen.ident(resourcenamePkg, "Sprint"), pr.Pattern, pr.PatternVariableIDs(true)))
	g.P()

	// STEP 2: Instantiate timestamps.
	g.P("  // STEP 2: Instantiate timestamps.")
	g.P("  // Check for x-migration-request header")
	if mc.mi.rpc.Message.Desc.Fields().ByName("create_time") != nil {
		g.P(fmt.Sprintf("  if values := %s(ctx, \"x-migration-request\"); len(values) > 0 {",
			mc.gen.ident(metadataPkg, "ValueFromIncomingContext")))
		g.P(fmt.Sprintf("    if request.%s.CreateTime == nil {", resourceGoName))
		g.P(fmt.Sprintf("      return nil, %s(%s, \"x-migration-request used without setting a create_time\").Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("    }")
		g.P("  } else {")
		g.P(fmt.Sprintf("    request.%s.CreateTime = %s()",
			resourceGoName, mc.gen.ident(timestamppbPkg, "Now")))
		g.P("  }")
	}
	if mc.mi.rpc.Message.Desc.Fields().ByName("update_time") != nil {
		g.P(fmt.Sprintf("  request.%s.UpdateTime = request.%s.CreateTime", resourceGoName, resourceGoName))
	}
	g.P()

	if mc.hasEtag {
		g.P("  { // Capture the Etag.")
		g.P("    var err error")
		g.P(fmt.Sprintf("    request.%s.Etag, err = %s(request.%s)",
			resourceGoName, mc.gen.ident(aipPkg, "ComputeETag"), resourceGoName))
		g.P("    if err != nil {")
		g.P(fmt.Sprintf("      return nil, %s(%s, \"computing etag: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal")))
		g.P("    }")
		g.P("  }")
		g.P()
	}

	// STEP 3: Convert to model.
	g.P("  // STEP 3: Convert the resource to the database representation.")
	g.P(fmt.Sprintf("  %s, err := %s(request.%s)",
		xstrings.ToCamelCase(mc.modelGoName), mc.parseFromPb, resourceGoName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from pb to model: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("  }")
	g.P()

	// Singleton children.
	for _, childPr := range singletonChildren {
		childMessage, err := resource.GetMessageByResourceType(childPr.Desc.Type)
		if err != nil {
			return fmt.Errorf("getting message for child resource %s: %w", childPr.Desc.Type, err)
		}
		childGoName := childPr.SingularGoName()

		g.P(fmt.Sprintf("  %s := &%s{", xstrings.ToCamelCase(childGoName), mc.gen.qgi(childMessage.GoIdent)))
		g.P(fmt.Sprintf("    Name: %s(\"%s\", %s),",
			mc.gen.ident(resourcenamePkg, "Sprint"), childPr.Pattern, childPr.PatternVariableIDs(true)))
		g.P(fmt.Sprintf("    CreateTime: request.%s.CreateTime,", resourceGoName))
		g.P(fmt.Sprintf("    UpdateTime: request.%s.UpdateTime,", resourceGoName))
		g.P("  }")

		if childMessage.Desc.Fields().ByName("etag") != nil {
			g.P("  {")
			g.P("    var err error")
			g.P(fmt.Sprintf("    %s.Etag, err = %s(%s)",
				xstrings.ToCamelCase(childGoName), mc.gen.ident(aipPkg, "ComputeETag"), xstrings.ToCamelCase(childGoName)))
			g.P("    if err != nil {")
			g.P(fmt.Sprintf("      return nil, %s(%s, \"computing %%s etag: %%v\", \"%s\", err).Err()",
				mc.statusErrorf(), mc.codes("Internal"), childPr.Desc.Singular))
			g.P("    }")
			g.P("  }")
		}

		childParseFromPb := mc.gen.modelIdent(childMessage.GoIdent.GoName + "FromPb")
		g.P(fmt.Sprintf("  %sModel, err := %s(%s)",
			xstrings.ToCamelCase(childGoName), childParseFromPb, xstrings.ToCamelCase(childGoName)))
		g.P("  if err != nil {")
		g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from pb to model: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), childPr.Desc.Singular))
		g.P("  }")
		g.P()
	}

	// Validate only.
	if method.Input.Desc.Fields().ByName("validate_only") != nil {
		g.P("  if request.ValidateOnly {")
		g.P(fmt.Sprintf("    return request.%s, nil", resourceGoName))
		g.P("  }")
		g.P()
	}

	// STEP 4: Insert.
	g.P("  // STEP 4: Insert the resource idempotently.")
	insertCall := fmt.Sprintf("  db%s, err := s.store.Insert%s", mc.modelGoName, resourceGoName)
	if hasRequestID {
		insertCall += "Idempotently"
	}
	insertCall += "(ctx, "
	if hasRequestID {
		insertCall += "request.RequestId, "
	}
	insertCall += xstrings.ToCamelCase(mc.modelGoName)
	for _, childPr := range singletonChildren {
		insertCall += ", " + xstrings.ToCamelCase(childPr.SingularGoName()) + "Model"
	}
	insertCall += ")"
	g.P(insertCall)
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errAlreadyExists))
	g.P(fmt.Sprintf("      return nil, %s(%s, \"%s already exists\").Err()",
		mc.statusErrorf(), mc.codes("AlreadyExists"), pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    return nil, %s(err, \"inserting %s\").Err()",
		mc.statusFromError(), xstrings.ToSnakeCase(resourceGoName)))
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  %s, err := db%s.ToPb()", xstrings.ToCamelCase(resourceGoName), mc.modelGoName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("  }")
	g.P()

	// STEP 5: Publish events.
	mc.generateCreatedEvents(xstrings.ToCamelCase(resourceGoName))

	g.P(fmt.Sprintf("  return %s, nil", xstrings.ToCamelCase(resourceGoName)))
	g.P("}")
	g.P()
	return nil
}

func (mc *methodCtx) generateCreatedEvents(resourceVar string) {
	if mc.mi.natsEventOpts == nil || len(mc.mi.natsEventOpts.GetCreated()) == 0 {
		return
	}
	mc.g.P("  // STEP 5: Publish events.")
	for _, eventOpt := range mc.mi.natsEventOpts.GetCreated() {
		subject := eventOpt.GetSubject()
		mc.g.P(fmt.Sprintf("  {"))
		mc.g.P(fmt.Sprintf("    subject := %s().Get%sSubject()",
			mc.gen.resourcePkgIdent(mc.mi.rpc.Message, "Get"+mc.natsStreamGoName),
			xstrings.ToPascalCase(subject)))
		mc.g.P(fmt.Sprintf("    if err := subject.Publish(ctx, s.natsClient, %s); err != nil {", resourceVar))
		mc.g.P(fmt.Sprintf("      return nil, %s(%s, \"publishing %s event: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), subject))
		mc.g.P("    }")
		mc.g.P("  }")
	}
}

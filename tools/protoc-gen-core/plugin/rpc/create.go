package rpc

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
)

func (mc *methodCtx) generateCreate() error {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName

	hasRequestID := method.Input.Desc.Fields().ByName("request_id") != nil

	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	// STEP 1: Set identifiers.
	g.P("  // STEP 1: Set identifiers.")
	if hasRequestID {
		g.P("  if request.RequestId == \"\" { // We always set a request id")
		g.P(fmt.Sprintf("    request.RequestId = %s().String()", mc.gen.ident(uuidPkg, "MustNewV7")))
		g.P("  }")
	}

	var patternVarID string
	if mc.multiPattern {
		idNames := mc.idNames()
		patternVarID = idNames[len(idNames)-1]
	} else {
		var err error
		patternVarID, err = mc.pattern.VariableID(true)
		if err != nil {
			return err
		}
	}
	g.P(fmt.Sprintf("  %s := request.%sId", patternVarID, resourceGoName))
	g.P(fmt.Sprintf("  if %s == \"\" {", patternVarID))
	g.P(fmt.Sprintf("    %s = %s()", patternVarID, mc.gen.ident(aipPkg, "NewSystemGeneratedBase32ResourceID")))
	g.P("  }")
	g.P()

	if mc.multiPattern {
		if err := mc.generateMultiPatternCreateName(); err != nil {
			return err
		}
	} else {
		if mc.pattern.Parent != nil {
			parent := mc.pattern.Parent
			g.P(fmt.Sprintf("  var %s string", parent.VariableIDs(true)))
			g.P(fmt.Sprintf("  if %s(request.Parent) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard")))
			g.P(fmt.Sprintf("    return nil, %s(%s, \"parent cannot contain wildcard\").Err()",
				mc.statusErrorf(), mc.codes("InvalidArgument")))
			g.P("  }")
			g.P(fmt.Sprintf("  if err := %s(request.Parent, \"%s\", %s); err != nil {",
				mc.gen.ident(resourcenamePkg, "Sscan"), parent.Value, parent.VariableIDPtrs()))
			g.P(fmt.Sprintf("    return nil, %s(%s, \"invalid parent name: %%v\", err).Err()",
				mc.statusErrorf(), mc.codes("InvalidArgument")))
			g.P("  }")
			g.P()
		}

		g.P(fmt.Sprintf("  request.%s.Name = %s(\"%s\", %s)",
			resourceGoName, mc.gen.ident(resourcenamePkg, "Sprint"), mc.pattern.Value, mc.pattern.VariableIDs(true)))
		g.P()
	}

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

	// Singleton children are created alongside their parent.
	for _, child := range mc.singletonChildren {
		childGoName := child.Resource.SingularGoName()

		g.P(fmt.Sprintf("  %s := &%s{", xstrings.ToCamelCase(childGoName), mc.gen.qgi(child.Message.GoIdent)))
		g.P(fmt.Sprintf("    Name: %s(\"%s\", %s),",
			mc.gen.ident(resourcenamePkg, "Sprint"), child.Pattern.Value, child.Pattern.VariableIDs(true)))
		g.P(fmt.Sprintf("    CreateTime: request.%s.CreateTime,", resourceGoName))
		g.P(fmt.Sprintf("    UpdateTime: request.%s.UpdateTime,", resourceGoName))
		g.P("  }")

		if child.Message.Desc.Fields().ByName("etag") != nil {
			g.P("  {")
			g.P("    var err error")
			g.P(fmt.Sprintf("    %s.Etag, err = %s(%s)",
				xstrings.ToCamelCase(childGoName), mc.gen.ident(aipPkg, "ComputeETag"), xstrings.ToCamelCase(childGoName)))
			g.P("    if err != nil {")
			g.P(fmt.Sprintf("      return nil, %s(%s, \"computing %%s etag: %%v\", \"%s\", err).Err()",
				mc.statusErrorf(), mc.codes("Internal"), child.Resource.Desc.Singular))
			g.P("    }")
			g.P("  }")
		}

		childParseFromPb := mc.gen.modelIdent(child.Message.GoIdent.GoName + "FromPb")
		g.P(fmt.Sprintf("  %sModel, err := %s(%s)",
			xstrings.ToCamelCase(childGoName), childParseFromPb, xstrings.ToCamelCase(childGoName)))
		g.P("  if err != nil {")
		g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from pb to model: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), child.Resource.Desc.Singular))
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
	for _, child := range mc.singletonChildren {
		insertCall += ", " + xstrings.ToCamelCase(child.Resource.SingularGoName()) + "Model"
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

// generateMultiPatternCreateName parses the parent against each pattern's
// parent pattern and builds the resource name directly in the matching case.
func (mc *methodCtx) generateMultiPatternCreateName() error {
	g := mc.g
	resourceGoName := mc.resourceGoName
	parentIDNames := mc.parentIDNames()

	// Each parent pattern must map to exactly one resource pattern, otherwise
	// the parent alone cannot determine the resource name.
	parentValueSet := map[string]bool{}
	for _, pattern := range mc.patterns {
		if parentValueSet[pattern.Parent.Value] {
			return fmt.Errorf("patterns of resource %s share parent pattern %q; cannot determine the resource name from the parent", mc.pr.Desc.Type, pattern.Parent.Value)
		}
		parentValueSet[pattern.Parent.Value] = true
	}

	g.P(fmt.Sprintf("  var %s string", strings.Join(parentIDNames, ", ")))
	g.P(fmt.Sprintf("  if %s(request.Parent) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard")))
	g.P(fmt.Sprintf("    return nil, %s(%s, \"parent cannot contain wildcard\").Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P("  switch {")
	for _, pattern := range mc.patterns {
		parent := pattern.Parent
		g.P(fmt.Sprintf("  case %s(\"%s\", request.Parent):", mc.gen.ident(resourcenamePkg, "Match"), parent.Value))
		g.P(fmt.Sprintf("    if err := %s(request.Parent, \"%s\", %s); err != nil {",
			mc.gen.ident(resourcenamePkg, "Sscan"), parent.Value, parent.VariableIDPtrs()))
		g.P(fmt.Sprintf("      return nil, %s(%s, \"invalid parent name: %%v\", err).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("    }")
		g.P(fmt.Sprintf("    request.%s.Name = %s(\"%s\", %s)",
			resourceGoName, mc.gen.ident(resourcenamePkg, "Sprint"), pattern.Value, mc.patternIDArgs(pattern)))
	}
	g.P("  default:")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"invalid parent name %%q\", request.Parent).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
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

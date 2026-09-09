package rpc

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
)

// prepareGoName returns the name of the per-resource helper that resolves a
// Create request into its database representation.
func (mc *methodCtx) prepareGoName() string {
	return "prepareCreate" + mc.resourceGoName
}

// createModelVars returns the Go variables holding a prepared resource's
// models: the resource's, then one per singleton child.
func (mc *methodCtx) createModelVars() []string {
	models := []string{xstrings.ToCamelCase(mc.modelGoName)}
	for _, child := range mc.singletonChildren {
		models = append(models, xstrings.ToCamelCase(child.Resource.SingularGoName())+"Model")
	}
	return models
}

// prepareErrReturn returns the error-return prefix of the prepare helper, which
// yields the parent model and one model per singleton child.
func (mc *methodCtx) prepareErrReturn() string {
	return "return " + strings.Repeat("nil, ", 1+len(mc.singletonChildren))
}

// generatePrepareCreate emits prepareCreate{X}: it resolves identifiers, the
// resource name, timestamps and etag onto request.{X} (and any singleton
// children) and returns the database models. Create and BatchCreate share it.
func (mc *methodCtx) generatePrepareCreate(createRequest string) error {
	g := mc.g
	pr := mc.pr
	resourceGoName := mc.resourceGoName
	errReturn := mc.prepareErrReturn()

	returns := []string{"*" + mc.goTypeQgi}
	for _, child := range mc.singletonChildren {
		returns = append(returns, "*"+mc.gen.modelIdent(child.Message.GoIdent.GoName))
	}
	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (%s, error) {",
		mc.serverGoName, mc.prepareGoName(), mc.gen.ident(contextPkg, "Context"), createRequest, strings.Join(returns, ", ")))

	// STEP 1: Set identifiers.
	g.P("  // STEP 1: Set identifiers.")
	g.P("  if request.RequestId == \"\" { // We always set a request id")
	g.P(fmt.Sprintf("    request.RequestId = %s().String()", mc.gen.ident(uuidPkg, "MustNewV7")))
	g.P("  }")

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
		if err := mc.generateMultiPatternCreateName(errReturn); err != nil {
			return err
		}
	} else {
		if mc.pattern.Parent != nil {
			parent := mc.pattern.Parent
			g.P(fmt.Sprintf("  var %s string", parent.VariableIDs(true)))
			g.P(fmt.Sprintf("  if %s(request.Parent) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard")))
			g.P(fmt.Sprintf("    %s%s(%s, \"parent cannot contain wildcard\").Err()",
				errReturn, mc.statusErrorf(), mc.codes("InvalidArgument")))
			g.P("  }")
			g.P(fmt.Sprintf("  if err := %s(request.Parent, \"%s\", %s); err != nil {",
				mc.gen.ident(resourcenamePkg, "Sscan"), parent.Value, parent.VariableIDPtrs()))
			g.P(fmt.Sprintf("    %s%s(%s, \"invalid parent name: %%v\", err).Err()",
				errReturn, mc.statusErrorf(), mc.codes("InvalidArgument")))
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
		g.P(fmt.Sprintf("      %s%s(%s, \"x-migration-request used without setting a create_time\").Err()",
			errReturn, mc.statusErrorf(), mc.codes("InvalidArgument")))
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
		g.P(fmt.Sprintf("      %s%s(%s, \"computing etag: %%v\", err).Err()",
			errReturn, mc.statusErrorf(), mc.codes("Internal")))
		g.P("    }")
		g.P("  }")
		g.P()
	}

	// STEP 3: Convert to model.
	g.P("  // STEP 3: Convert the resource to the database representation.")
	g.P(fmt.Sprintf("  %s, err := %s(request.%s)",
		xstrings.ToCamelCase(mc.modelGoName), mc.parseFromPb, resourceGoName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    %s%s(%s, \"converting %s from pb to model: %%v\", err).Err()",
		errReturn, mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("  }")
	g.P()

	// Singleton children are created alongside their parent.
	for _, child := range mc.singletonChildren {
		childVar := xstrings.ToCamelCase(child.Resource.SingularGoName())

		g.P(fmt.Sprintf("  %s := &%s{", childVar, mc.gen.qgi(child.Message.GoIdent)))
		g.P(fmt.Sprintf("    Name: %s(\"%s\", %s),",
			mc.gen.ident(resourcenamePkg, "Sprint"), child.Pattern.Value, child.Pattern.VariableIDs(true)))
		g.P(fmt.Sprintf("    CreateTime: request.%s.CreateTime,", resourceGoName))
		g.P(fmt.Sprintf("    UpdateTime: request.%s.UpdateTime,", resourceGoName))
		g.P("  }")

		if child.Message.Desc.Fields().ByName("etag") != nil {
			g.P("  {")
			g.P("    var err error")
			g.P(fmt.Sprintf("    %s.Etag, err = %s(%s)",
				childVar, mc.gen.ident(aipPkg, "ComputeETag"), childVar))
			g.P("    if err != nil {")
			g.P(fmt.Sprintf("      %s%s(%s, \"computing %%s etag: %%v\", \"%s\", err).Err()",
				errReturn, mc.statusErrorf(), mc.codes("Internal"), child.Resource.Desc.Singular))
			g.P("    }")
			g.P("  }")
		}

		childParseFromPb := mc.gen.modelIdent(child.Message.GoIdent.GoName + "FromPb")
		g.P(fmt.Sprintf("  %sModel, err := %s(%s)", childVar, childParseFromPb, childVar))
		g.P("  if err != nil {")
		g.P(fmt.Sprintf("    %s%s(%s, \"converting %s from pb to model: %%v\", err).Err()",
			errReturn, mc.statusErrorf(), mc.codes("Internal"), child.Resource.Desc.Singular))
		g.P("  }")
		g.P()
	}

	g.P(fmt.Sprintf("  return %s, nil", strings.Join(mc.createModelVars(), ", ")))
	g.P("}")
	g.P()
	return nil
}

// batchInsertCall renders the store insert for the given model slices, e.g.
// `s.store.BatchInsertAuthors(ctx, requestIDs, authorModels, authorProfileModels)`.
func (mc *methodCtx) batchInsertCall(requestIDs string, modelSlices []string) string {
	args := append([]string{"ctx", requestIDs}, modelSlices...)
	return fmt.Sprintf("s.store.BatchInsert%s(%s)", mc.pr.PluralGoName(), strings.Join(args, ", "))
}

// generateInsertErrorMapping emits the status mapping of a store insert error.
func (mc *methodCtx) generateInsertErrorMapping() {
	g := mc.g
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errAlreadyExists))
	g.P(fmt.Sprintf("      return nil, %s(%s, \"%s already exists\").Err()",
		mc.statusErrorf(), mc.codes("AlreadyExists"), mc.pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    return nil, %s(err, \"inserting %s\").Err()",
		mc.statusFromError(), xstrings.ToSnakeCase(mc.pr.PluralGoName())))
	g.P("  }")
}

// generateCreate emits Create{X} as a single-element batch insert.
func (mc *methodCtx) generateCreate() error {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName
	resourceVar := xstrings.ToCamelCase(resourceGoName)

	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	models := mc.createModelVars()
	g.P(fmt.Sprintf("  %s, err := s.%s(ctx, request)", strings.Join(models, ", "), mc.prepareGoName()))
	g.P("  if err != nil {")
	g.P("    return nil, err")
	g.P("  }")
	g.P()

	if method.Input.Desc.Fields().ByName("validate_only") != nil {
		g.P("  if request.ValidateOnly {")
		g.P(fmt.Sprintf("    return request.%s, nil", resourceGoName))
		g.P("  }")
		g.P()
	}

	// STEP 4: Insert.
	g.P("  // STEP 4: Insert the resource.")
	modelSlices := []string{fmt.Sprintf("[]*%s{%s}", mc.goTypeQgi, models[0])}
	for i, child := range mc.singletonChildren {
		modelSlices = append(modelSlices, fmt.Sprintf("[]*%s{%s}", mc.gen.modelIdent(child.Message.GoIdent.GoName), models[i+1]))
	}
	g.P(fmt.Sprintf("  db%s, err := %s", pr.PluralGoName(), mc.batchInsertCall("[]string{request.RequestId}", modelSlices)))
	mc.generateInsertErrorMapping()
	g.P(fmt.Sprintf("  if len(db%s) != 1 {", pr.PluralGoName()))
	g.P(fmt.Sprintf("    return nil, %s(%s, \"expected 1 inserted %s, got %%d\", len(db%s)).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular, pr.PluralGoName()))
	g.P("  }")
	g.P()

	g.P(fmt.Sprintf("  %s, err := db%s[0].ToPb()", resourceVar, pr.PluralGoName()))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("  }")
	g.P()

	// STEP 5: Publish events.
	mc.generateCreatedEvents(resourceVar)

	g.P(fmt.Sprintf("  return %s, nil", resourceVar))
	g.P("}")
	g.P()
	return nil
}

// generateMultiPatternCreateName parses the parent against each pattern's
// parent pattern and builds the resource name directly in the matching case.
// Parents are matched most-specific first, so a literal segment aliasing a
// variable segment resolves to the literal pattern.
func (mc *methodCtx) generateMultiPatternCreateName(errReturn string) error {
	g := mc.g
	resourceGoName := mc.resourceGoName
	parentIDNames := mc.parentIDNames()

	// Each parent pattern must map to exactly one resource pattern, otherwise
	// the parent alone cannot determine the resource name.
	parentValueSet := map[string]bool{}
	for _, pattern := range mc.parentedPatterns() {
		if parentValueSet[pattern.Parent.Value] {
			return fmt.Errorf("patterns of resource %s share parent pattern %q; cannot determine the resource name from the parent", mc.pr.Desc.Type, pattern.Parent.Value)
		}
		parentValueSet[pattern.Parent.Value] = true
	}

	g.P(fmt.Sprintf("  var %s string", strings.Join(parentIDNames, ", ")))
	g.P(fmt.Sprintf("  if %s(request.Parent) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard")))
	g.P(fmt.Sprintf("    %s%s(%s, \"parent cannot contain wildcard\").Err()",
		errReturn, mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P("  switch {")
	if rootPattern := mc.rootPattern(); rootPattern != nil {
		g.P("  case request.Parent == \"\":")
		g.P(fmt.Sprintf("    request.%s.Name = %s(\"%s\", %s)",
			resourceGoName, mc.gen.ident(resourcenamePkg, "Sprint"), rootPattern.Value, mc.patternIDArgs(rootPattern)))
	}
	for _, pattern := range mc.parentedPatterns() {
		parent := pattern.Parent
		g.P(fmt.Sprintf("  case %s(\"%s\", request.Parent):", mc.gen.ident(resourcenamePkg, "Match"), parent.Value))
		g.P(fmt.Sprintf("    if err := %s(request.Parent, \"%s\", %s); err != nil {",
			mc.gen.ident(resourcenamePkg, "Sscan"), parent.Value, parent.VariableIDPtrs()))
		g.P(fmt.Sprintf("      %s%s(%s, \"invalid parent name: %%v\", err).Err()",
			errReturn, mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("    }")
		g.P(fmt.Sprintf("    request.%s.Name = %s(\"%s\", %s)",
			resourceGoName, mc.gen.ident(resourcenamePkg, "Sprint"), pattern.Value, mc.patternIDArgs(pattern)))
	}
	g.P("  default:")
	g.P(fmt.Sprintf("    %s%s(%s, \"invalid parent name %%q\", request.Parent).Err()",
		errReturn, mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("  }")
	g.P()
	return nil
}

// generateCreatedEvents publishes the created events of the resource held in
// resourceVar.
func (mc *methodCtx) generateCreatedEvents(resourceVar string) {
	if mc.mi.natsEventOpts == nil || len(mc.mi.natsEventOpts.GetCreated()) == 0 {
		return
	}
	mc.g.P("  // STEP 5: Publish events.")
	for _, eventOpt := range mc.mi.natsEventOpts.GetCreated() {
		subject := eventOpt.GetSubject()
		mc.g.P("  {")
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

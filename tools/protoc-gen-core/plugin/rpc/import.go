package rpc

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"

	"github.com/malonaz/core/tools/protoc-gen-core/resource"
)

// inlineSourceFieldName is the field of an Import request holding the
// resources to import. AIP-153 wraps every import source in a `oneof source`
// so that a service can grow new sources without breaking the wire format;
// inline is the only source this codegen knows how to load.
const inlineSourceFieldName = "inline_source"

// importIO resolves the request and response wiring of an Import RPC.
type importIO struct {
	// resourcesGoName is the Go name of the repeated resource field carried by
	// the request's inline source, e.g. "Books".
	resourcesGoName string
	// responseGoName is the Go name of the repeated resource field of the
	// response message.
	responseGoName string
}

// resolveImportIO locates the repeated resource field on both sides of the RPC.
func (mc *methodCtx) resolveImportIO() (*importIO, error) {
	method := mc.mi.method
	resourceIdent := mc.mi.rpc.Message.GoIdent

	var inlineSource *protogen.Field
	for _, field := range method.Input.Fields {
		if string(field.Desc.Name()) == inlineSourceFieldName {
			inlineSource = field
			break
		}
	}
	if inlineSource == nil || inlineSource.Message == nil {
		return nil, fmt.Errorf("request %s must define an %q message field holding the resources to import",
			method.Input.GoIdent.GoName, inlineSourceFieldName)
	}

	resourcesField := repeatedFieldOf(inlineSource.Message, resourceIdent)
	if resourcesField == nil {
		return nil, fmt.Errorf("message %s must define a repeated %s field",
			inlineSource.Message.GoIdent.GoName, resourceIdent.GoName)
	}
	responseField := repeatedFieldOf(method.Output, resourceIdent)
	if responseField == nil {
		return nil, fmt.Errorf("response %s must define a repeated %s field",
			method.Output.GoIdent.GoName, resourceIdent.GoName)
	}
	return &importIO{
		resourcesGoName: resourcesField.GoName,
		responseGoName:  responseField.GoName,
	}, nil
}

// repeatedFieldOf returns the repeated field of message whose element type is
// the given message type.
func repeatedFieldOf(message *protogen.Message, ident protogen.GoIdent) *protogen.Field {
	for _, field := range message.Fields {
		if field.Desc.IsList() && field.Message != nil && field.Message.GoIdent == ident {
			return field
		}
	}
	return nil
}

// generateImport emits an Import{Plural} method: it names, stamps and converts
// every resource of the request's inline source, then hands the whole batch to
// the store, which bulk-loads it with postgres COPY.
//
// See https://google.aip.dev/153. The method is synchronous rather than a long
// running operation, which AIP-153 allows for imports that complete in seconds.
func (mc *methodCtx) generateImport() error {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName
	plural := pr.PluralGoName()
	// A singleton has no identifier of its own, so a batch of them is not
	// addressable.
	if mc.singleton {
		return fmt.Errorf("resource %s is a singleton; Import is not supported", pr.Desc.Type)
	}

	io, err := mc.resolveImportIO()
	if err != nil {
		return err
	}

	hasRequestID := mc.createHasRequestID()
	hasParent := mc.multiPattern || mc.pattern.Parent != nil
	resourcesVar := xstrings.ToCamelCase(plural)
	resourceVar := xstrings.ToCamelCase(resourceGoName)
	modelsVar := resourceVar + "Models"

	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	// STEP 1: Resolve the parent into the pattern the imported resources are named under.
	g.P("  // STEP 1: Resolve the parent into the pattern the imported resources are named under.")
	if err := mc.generateImportPattern(); err != nil {
		return err
	}

	g.P(fmt.Sprintf("  %s := request.GetInlineSource().Get%s()", resourcesVar, io.resourcesGoName))
	g.P()

	// STEP 2: Name, stamp and convert every resource.
	g.P("  // STEP 2: Name, stamp and convert every resource.")
	hasCreateTime := mc.mi.rpc.Message.Desc.Fields().ByName("create_time") != nil
	if hasCreateTime {
		// The response echoes the resources rather than reading them back, so
		// the stamp is truncated to the microsecond precision of a postgres
		// timestamp: what the caller gets back is what was stored.
		g.P(fmt.Sprintf("  createTime := %s(%s().UTC().Truncate(%s))",
			mc.gen.ident(timestamppbPkg, "New"), mc.gen.ident(timePkg, "Now"), mc.gen.ident(timePkg, "Microsecond")))
		g.P(fmt.Sprintf("  migrationRequest := len(%s(ctx, \"x-migration-request\")) > 0",
			mc.gen.ident(metadataPkg, "ValueFromIncomingContext")))
	}
	if hasRequestID {
		g.P(fmt.Sprintf("  requestIds := make([]string, len(%s))", resourcesVar))
	}
	g.P(fmt.Sprintf("  %s := make([]*%s, len(%s))", modelsVar, mc.goTypeQgi, resourcesVar))
	for _, child := range mc.singletonChildren {
		g.P(fmt.Sprintf("  %sModels := make([]*%s, len(%s))",
			xstrings.ToCamelCase(child.Resource.SingularGoName()),
			mc.gen.modelIdent(child.Message.GoIdent.GoName), resourcesVar))
	}
	g.P(fmt.Sprintf("  for i, %s := range %s {", resourceVar, resourcesVar))

	// Identifier: a caller may preserve an existing identifier by setting the
	// resource name; otherwise the server generates one.
	idVar := resourceVar + "Id"
	g.P(fmt.Sprintf("    var %s string", idVar))
	g.P(fmt.Sprintf("    if %s.Name != \"\" {", resourceVar))
	g.P(fmt.Sprintf("      if %s(%s.Name) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard"), resourceVar))
	g.P(fmt.Sprintf("        return nil, %s(%s, \"%s[%%d]: name cannot contain wildcard\", i).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument"), pr.Desc.Plural))
	g.P("      }")
	g.P(fmt.Sprintf("      if !%s(patternValue, %s.Name) {", mc.gen.ident(resourcenamePkg, "Match"), resourceVar))
	g.P(fmt.Sprintf("        return nil, %s(%s, \"%s[%%d]: name %%q does not match the pattern of a %s\", i, %s.Name).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument"), pr.Desc.Plural, pr.Desc.Singular, resourceVar))
	g.P("      }")
	if hasParent {
		g.P(fmt.Sprintf("      if !%s(%s.Name, request.Parent) {", mc.gen.ident(resourcenamePkg, "HasParent"), resourceVar))
		g.P(fmt.Sprintf("        return nil, %s(%s, \"%s[%%d]: name %%q does not have parent %%q\", i, %s.Name, request.Parent).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument"), pr.Desc.Plural, resourceVar))
		g.P("      }")
	}
	// Parse{Resource}Name returns every union identifier; only the resource's
	// own identifier is needed, the rest are pinned by the parent check above.
	discards := make([]string, len(mc.idNames())-1)
	for i := range discards {
		discards[i] = "_"
	}
	parsedIDs := append(discards, "parsedId")
	g.P(fmt.Sprintf("      %s, err := %s(%s.Name)", strings.Join(parsedIDs, ", "), mc.parseName, resourceVar))
	g.P("      if err != nil {")
	g.P(fmt.Sprintf("        return nil, %s(%s, \"%s[%%d]: parsing name %%q: %%v\", i, %s.Name, err).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument"), pr.Desc.Plural, resourceVar))
	g.P("      }")
	g.P(fmt.Sprintf("      %s = parsedId", idVar))
	g.P("    } else {")
	g.P(fmt.Sprintf("      %s = %s()", idVar, mc.gen.ident(aipPkg, "NewSystemGeneratedBase32ResourceID")))
	g.P("    }")
	g.P("    nameVariables := make([]string, 0, len(parentIds)+1)")
	g.P("    nameVariables = append(nameVariables, parentIds...)")
	g.P(fmt.Sprintf("    nameVariables = append(nameVariables, %s)", idVar))
	g.P(fmt.Sprintf("    %s.Name = %s(patternValue, nameVariables...)",
		resourceVar, mc.gen.ident(resourcenamePkg, "Sprint")))
	g.P()

	// Timestamps. An import migrating existing data preserves create_time,
	// gated on the same header the Create RPC honors.
	if hasCreateTime {
		g.P("    if migrationRequest {")
		g.P(fmt.Sprintf("      if %s.CreateTime == nil {", resourceVar))
		g.P(fmt.Sprintf("        return nil, %s(%s, \"%s[%%d]: x-migration-request used without setting a create_time\", i).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument"), pr.Desc.Plural))
		g.P("      }")
		g.P("    } else {")
		g.P(fmt.Sprintf("      %s.CreateTime = createTime", resourceVar))
		g.P("    }")
	}
	if mc.mi.rpc.Message.Desc.Fields().ByName("update_time") != nil {
		g.P(fmt.Sprintf("    %s.UpdateTime = %s.CreateTime", resourceVar, resourceVar))
	}

	if mc.hasEtag {
		g.P("    { // Capture the Etag.")
		g.P("      var err error")
		g.P(fmt.Sprintf("      %s.Etag, err = %s(%s)",
			resourceVar, mc.gen.ident(aipPkg, "ComputeETag"), resourceVar))
		g.P("      if err != nil {")
		g.P(fmt.Sprintf("        return nil, %s(%s, \"%s[%%d]: computing etag: %%v\", i, err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Plural))
		g.P("      }")
		g.P("    }")
	}
	g.P()

	g.P(fmt.Sprintf("    %sModel, err := %s(%s)", resourceVar, mc.parseFromPb, resourceVar))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"%s[%%d]: converting %s from pb to model: %%v\", i, err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Plural, pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    %s[i] = %sModel", modelsVar, resourceVar))
	if hasRequestID {
		g.P(fmt.Sprintf("    requestIds[i] = %s().String()", mc.gen.ident(uuidPkg, "MustNewV7")))
	}

	// Singleton children are imported alongside their parent, exactly as the
	// Create RPC inserts them alongside theirs.
	for _, child := range mc.singletonChildren {
		childVar := xstrings.ToCamelCase(child.Resource.SingularGoName())
		g.P()
		g.P(fmt.Sprintf("    %s := &%s{", childVar, mc.gen.qgi(child.Message.GoIdent)))
		g.P(fmt.Sprintf("      Name: %s(\"%s\", %s),",
			mc.gen.ident(resourcenamePkg, "Sprint"), child.Pattern.Value, child.Pattern.VariableIDs(true)))
		g.P(fmt.Sprintf("      CreateTime: %s.CreateTime,", resourceVar))
		g.P(fmt.Sprintf("      UpdateTime: %s.UpdateTime,", resourceVar))
		g.P("    }")
		if child.Message.Desc.Fields().ByName("etag") != nil {
			g.P("    {")
			g.P("      var err error")
			g.P(fmt.Sprintf("      %s.Etag, err = %s(%s)",
				childVar, mc.gen.ident(aipPkg, "ComputeETag"), childVar))
			g.P("      if err != nil {")
			g.P(fmt.Sprintf("        return nil, %s(%s, \"%s[%%d]: computing %s etag: %%v\", i, err).Err()",
				mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Plural, child.Resource.Desc.Singular))
			g.P("      }")
			g.P("    }")
		}
		childParseFromPb := mc.gen.modelIdent(child.Message.GoIdent.GoName + "FromPb")
		g.P(fmt.Sprintf("    %sModel, err := %s(%s)", childVar, childParseFromPb, childVar))
		g.P("    if err != nil {")
		g.P(fmt.Sprintf("      return nil, %s(%s, \"%s[%%d]: converting %s from pb to model: %%v\", i, err).Err()",
			mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Plural, child.Resource.Desc.Singular))
		g.P("    }")
		g.P(fmt.Sprintf("    %sModels[i] = %sModel", childVar, childVar))
	}

	g.P("  }")
	g.P()

	// The response carries the resources as written; join-backed output-only
	// fields are not resolved, since COPY returns no rows.
	response := fmt.Sprintf("&%s{%s: %s}", mc.gen.qgi(method.Output.GoIdent), io.responseGoName, resourcesVar)

	if method.Input.Desc.Fields().ByName("validate_only") != nil {
		g.P("  if request.ValidateOnly {")
		g.P(fmt.Sprintf("    return %s, nil", response))
		g.P("  }")
		g.P()
	}

	// STEP 3: Bulk load.
	g.P("  // STEP 3: Bulk load the resources.")
	storeCall := fmt.Sprintf("  if _, err := s.store.%s(ctx, ", mc.importStoreMethod())
	if hasRequestID {
		storeCall += "requestIds, "
	}
	storeCall += modelsVar
	for _, child := range mc.singletonChildren {
		storeCall += ", " + xstrings.ToCamelCase(child.Resource.SingularGoName()) + "Models"
	}
	storeCall += "); err != nil {"
	g.P(storeCall)
	g.P(fmt.Sprintf("    if %s(err, %s) {", mc.errorsIs(), mc.errAlreadyExists))
	g.P(fmt.Sprintf("      return nil, %s(%s, \"%s already exists\").Err()",
		mc.statusErrorf(), mc.codes("AlreadyExists"), pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    return nil, %s(err, \"importing %s\").Err()",
		mc.statusFromError(), xstrings.ToSnakeCase(plural)))
	g.P("  }")
	g.P()

	// STEP 4: Publish events. Import has its own event category: a bulk load
	// can fan out one event per row, which a service may not want even when it
	// publishes created events for interactive creates.
	if importedEvents := mc.mi.natsEventOpts.GetImported(); len(importedEvents) > 0 {
		g.P("  // STEP 4: Publish events.")
		g.P(fmt.Sprintf("  for _, %s := range %s {", resourceVar, resourcesVar))
		mc.emitEvents(importedEvents, resourceVar)
		g.P("  }")
		g.P()
	}

	g.P(fmt.Sprintf("  return %s, nil", response))
	g.P("}")
	g.P()
	return nil
}

// generateImportPattern emits `patternValue` and `parentIds`: the resource
// pattern the batch is named under, and the parent identifiers every name in
// it shares. Multi-pattern resources resolve both from the parent.
func (mc *methodCtx) generateImportPattern() error {
	g := mc.g

	if mc.multiPattern {
		parentIDNames := mc.parentIDNames()
		g.P(fmt.Sprintf("  var %s string", strings.Join(parentIDNames, ", ")))
		g.P("  var patternValue string")
		g.P("  var parentIds []string")
		g.P(fmt.Sprintf("  if %s(request.Parent) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard")))
		g.P(fmt.Sprintf("    return nil, %s(%s, \"parent cannot contain wildcard\").Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("  }")
		g.P("  switch {")
		for _, pattern := range resource.SortPatternsBySpecificity(mc.patterns) {
			parent := pattern.Parent
			g.P(fmt.Sprintf("  case %s(\"%s\", request.Parent):", mc.gen.ident(resourcenamePkg, "Match"), parent.Value))
			g.P(fmt.Sprintf("    if err := %s(request.Parent, \"%s\", %s); err != nil {",
				mc.gen.ident(resourcenamePkg, "Sscan"), parent.Value, parent.VariableIDPtrs()))
			g.P(fmt.Sprintf("      return nil, %s(%s, \"invalid parent name: %%v\", err).Err()",
				mc.statusErrorf(), mc.codes("InvalidArgument")))
			g.P("    }")
			g.P(fmt.Sprintf("    patternValue = \"%s\"", pattern.Value))
			g.P(fmt.Sprintf("    parentIds = []string{%s}", parent.VariableIDs(true)))
		}
		g.P("  default:")
		g.P(fmt.Sprintf("    return nil, %s(%s, \"invalid parent name %%q\", request.Parent).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("  }")
		g.P()
		return nil
	}

	g.P(fmt.Sprintf("  patternValue := \"%s\"", mc.pattern.Value))
	if mc.pattern.Parent == nil {
		g.P("  var parentIds []string")
		g.P()
		return nil
	}

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
	g.P(fmt.Sprintf("  parentIds := []string{%s}", parent.VariableIDs(true)))
	g.P()
	return nil
}

// importStoreMethod returns the store method an Import RPC binds to. Resources
// whose Create RPC carries a request_id are stored with one per row, mirroring
// InsertIdempotently.
func (mc *methodCtx) importStoreMethod() string {
	name := "Import" + mc.pr.PluralGoName()
	if mc.createHasRequestID() {
		name += "WithRequestIDs"
	}
	return name
}

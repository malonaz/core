package rpc

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// batchCreateRequestsField validates and returns the `requests` field of a
// BatchCreate request: a repeated message whose type is the resource's Create
// request (AIP-233).
func (mc *methodCtx) batchCreateRequestsField(createRequest *protogen.Message) (*protogen.Field, error) {
	method := mc.mi.method
	for _, field := range method.Input.Fields {
		if field.Desc.Name() != "requests" {
			continue
		}
		if field.Desc.Cardinality() != protoreflect.Repeated || field.Message == nil {
			return nil, fmt.Errorf("%s.requests must be a repeated message", method.Input.GoIdent.GoName)
		}
		if field.Message.Desc.FullName() != createRequest.Desc.FullName() {
			return nil, fmt.Errorf("%s.requests must be repeated %s, got %s",
				method.Input.GoIdent.GoName, createRequest.Desc.FullName(), field.Message.Desc.FullName())
		}
		return field, nil
	}
	return nil, fmt.Errorf("%s must declare a `requests` field", method.Input.GoIdent.GoName)
}

// batchCreateResponseField validates and returns the response's repeated
// resource field, named after the resource's plural.
func (mc *methodCtx) batchCreateResponseField() (*protogen.Field, error) {
	method := mc.mi.method
	// Desc.Plural is lowerCamel ("modelRevisions"); proto field names are snake_case.
	fieldName := xstrings.ToSnakeCase(mc.pr.Desc.Plural)
	for _, field := range method.Output.Fields {
		if string(field.Desc.Name()) != fieldName {
			continue
		}
		if field.Desc.Cardinality() != protoreflect.Repeated || field.Message == nil ||
			field.Message.Desc.FullName() != mc.mi.rpc.Message.Desc.FullName() {
			return nil, fmt.Errorf("%s.%s must be repeated %s", method.Output.GoIdent.GoName, fieldName, mc.mi.rpc.Message.Desc.FullName())
		}
		return field, nil
	}
	return nil, fmt.Errorf("%s must declare a repeated `%s` field", method.Output.GoIdent.GoName, fieldName)
}

// generateBatchCreate emits BatchCreate{Plural}: every sub-request is prepared
// like a Create, then the whole batch is inserted atomically; the store returns
// rows in request order.
func (mc *methodCtx) generateBatchCreate(createRequest *protogen.Message) error {
	g := mc.g
	method := mc.mi.method
	pr := mc.pr
	resourceGoName := mc.resourceGoName
	resourceVar := xstrings.ToCamelCase(resourceGoName)
	pluralGoName := pr.PluralGoName()
	pluralVar := xstrings.ToCamelCase(pluralGoName)

	requestsField, err := mc.batchCreateRequestsField(createRequest)
	if err != nil {
		return err
	}
	responseField, err := mc.batchCreateResponseField()
	if err != nil {
		return err
	}

	createHasParent := createRequest.Desc.Fields().ByName("parent") != nil
	createHasValidateOnly := createRequest.Desc.Fields().ByName("validate_only") != nil
	hasParent := method.Input.Desc.Fields().ByName("parent") != nil
	hasValidateOnly := method.Input.Desc.Fields().ByName("validate_only") != nil
	if hasParent && !createHasParent {
		return fmt.Errorf("%s declares a parent but %s does not", method.Input.GoIdent.GoName, createRequest.GoIdent.GoName)
	}

	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		mc.serverGoName, method.GoName, mc.gen.ident(contextPkg, "Context"), mc.inputType(), mc.outputType()))

	if hasParent {
		g.P(fmt.Sprintf("  if %s(request.Parent) {", mc.gen.ident(resourcenamePkg, "ContainsWildcard")))
		g.P(fmt.Sprintf("    return nil, %s(%s, \"parent cannot contain wildcard\").Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("  }")
	}
	g.P(fmt.Sprintf("  n := len(request.%s)", requestsField.GoName))
	g.P("  requestIDs := make([]string, 0, n)")
	g.P("  requestIDSet := make(map[string]struct{}, n)")
	models := mc.createModelVars()
	modelTypes := []string{mc.goTypeQgi}
	for _, child := range mc.singletonChildren {
		modelTypes = append(modelTypes, mc.gen.modelIdent(child.Message.GoIdent.GoName))
	}
	modelSlices := make([]string, len(models))
	for i, model := range models {
		modelSlices[i] = model + "s"
		g.P(fmt.Sprintf("  %s := make([]*%s, 0, n)", modelSlices[i], modelTypes[i]))
	}
	// Duplicate names within one multi-row upsert are a cardinality violation
	// in postgres, not a unique violation; reject them upfront.
	g.P("  names := make(map[string]struct{}, n)")
	g.P(fmt.Sprintf("  for i, createRequest := range request.%s {", requestsField.GoName))

	if hasParent {
		g.P("    // A sub-request inherits the batch parent; an explicit one must agree.")
		g.P("    if request.Parent != \"\" {")
		g.P("      if createRequest.Parent == \"\" {")
		g.P("        createRequest.Parent = request.Parent")
		g.P("      } else if createRequest.Parent != request.Parent {")
		g.P(fmt.Sprintf("        return nil, %s(%s, \"requests[%%d].parent %%q does not match parent %%q\", i, createRequest.Parent, request.Parent).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument")))
		g.P("      }")
		g.P("    }")
	}
	if createHasValidateOnly {
		hint := ""
		if hasValidateOnly {
			hint = "; set validate_only on the batch request"
		}
		g.P("    if createRequest.ValidateOnly {")
		g.P(fmt.Sprintf("      return nil, %s(%s, \"requests[%%d].validate_only is not supported%s\", i).Err()",
			mc.statusErrorf(), mc.codes("InvalidArgument"), hint))
		g.P("    }")
	}

	g.P(fmt.Sprintf("    %s, err := s.%s(ctx, createRequest)", strings.Join(models, ", "), mc.prepareGoName()))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(err, \"requests[%%d]\", i).Err()", mc.statusFromError()))
	g.P("    }")
	g.P(fmt.Sprintf("    if _, ok := names[createRequest.%s.Name]; ok {", resourceGoName))
	g.P(fmt.Sprintf("      return nil, %s(%s, \"requests[%%d]: duplicate %s name %%q\", i, createRequest.%s.Name).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument"), pr.Desc.Singular, resourceGoName))
	g.P("    }")
	g.P(fmt.Sprintf("    names[createRequest.%s.Name] = struct{}{}", resourceGoName))
	g.P("    if _, ok := requestIDSet[createRequest.RequestId]; ok {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"requests[%%d]: duplicate request_id %%q\", i, createRequest.RequestId).Err()",
		mc.statusErrorf(), mc.codes("InvalidArgument")))
	g.P("    }")
	g.P("    requestIDSet[createRequest.RequestId] = struct{}{}")
	g.P("    requestIDs = append(requestIDs, createRequest.RequestId)")
	for i, slice := range modelSlices {
		g.P(fmt.Sprintf("    %s = append(%s, %s)", slice, slice, models[i]))
	}
	g.P("  }")
	g.P()

	if hasValidateOnly {
		g.P("  if request.ValidateOnly {")
		g.P(fmt.Sprintf("    %s := make([]*%s, n)", pluralVar, mc.protoType()))
		g.P(fmt.Sprintf("    for i, createRequest := range request.%s {", requestsField.GoName))
		g.P(fmt.Sprintf("      %s[i] = createRequest.%s", pluralVar, resourceGoName))
		g.P("    }")
		g.P(fmt.Sprintf("    return &%s{%s: %s}, nil", mc.outputType(), responseField.GoName, pluralVar))
		g.P("  }")
		g.P()
	}

	g.P("  // Insert the whole batch atomically.")
	g.P(fmt.Sprintf("  db%s, err := %s", pluralGoName, mc.batchInsertCall("requestIDs", modelSlices)))
	mc.generateInsertErrorMapping()
	g.P(fmt.Sprintf("  if len(db%s) != n {", pluralGoName))
	g.P(fmt.Sprintf("    return nil, %s(%s, \"expected %%d inserted %s, got %%d\", n, len(db%s)).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Plural, pluralGoName))
	g.P("  }")
	g.P()

	// The store returns rows in request order, so a replay yields the
	// originally created resources rather than the names computed above.
	g.P(fmt.Sprintf("  %s := make([]*%s, n)", pluralVar, mc.protoType()))
	g.P(fmt.Sprintf("  for i, db%s := range db%s {", mc.modelGoName, pluralGoName))
	g.P(fmt.Sprintf("    %s, err := db%s.ToPb()", resourceVar, mc.modelGoName))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return nil, %s(%s, \"converting %s from model to pb: %%v\", err).Err()",
		mc.statusErrorf(), mc.codes("Internal"), pr.Desc.Singular))
	g.P("    }")
	g.P(fmt.Sprintf("    %s[i] = %s", pluralVar, resourceVar))
	g.P("  }")
	g.P()

	if mc.mi.natsEventOpts != nil && len(mc.mi.natsEventOpts.GetCreated()) > 0 {
		g.P(fmt.Sprintf("  for _, %s := range %s {", resourceVar, pluralVar))
		mc.generateCreatedEvents(resourceVar)
		g.P("  }")
		g.P()
	}

	g.P(fmt.Sprintf("  return &%s{%s: %s}, nil", mc.outputType(), responseField.GoName, pluralVar))
	g.P("}")
	g.P()
	return nil
}

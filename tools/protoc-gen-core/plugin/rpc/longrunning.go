package rpc

import (
	"errors"
	"fmt"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	codegenschedulerpb "github.com/malonaz/core/genproto/codegen/scheduler/v1"
	"github.com/malonaz/core/go/pbutil"
)

var (
	longrunningPkg    = protogen.GoImportPath("github.com/malonaz/core/go/scheduler/longrunning")
	longrunningpbPkg  = protogen.GoImportPath("cloud.google.com/go/longrunning/autogen/longrunningpb")
	schedulerGenPkg   = protogen.GoImportPath("github.com/malonaz/core/genproto/scheduler/scheduler_service/v1")
	operationFullName = (&longrunningpb.Operation{}).ProtoReflect().Descriptor().FullName()
)

// longrunningMethod is an RPC returning google.longrunning.Operation (AIP-151),
// generated in two roles behind one handler: the producer that hands the
// request to the scheduler, and the runner the scheduler calls back.
type longrunningMethod struct {
	method *protogen.Method
	// The resource the operation acts on, deriving its job's parent: the request's `parent` or `name`.
	resourceField *protogen.Field
	// Set when the request carries a `request_id`, which makes the start idempotent.
	requestIDField *protogen.Field
	responseType   protoreflect.FullName
}

// parseLongrunningMethod returns nil when the method does not return an
// Operation. One that does must carry `google.longrunning.operation_info`
// (AIP-151) and the scheduler's `malonaz.scheduler.v1.method` annotation, which
// is what makes the scheduler run it.
func parseLongrunningMethod(method *protogen.Method) (*longrunningMethod, error) {
	if method.Output.Desc.FullName() != operationFullName {
		return nil, nil
	}
	operationInfo, err := pbutil.GetExtension[*longrunningpb.OperationInfo](method.Desc.Options(), longrunningpb.E_OperationInfo)
	if err != nil {
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			return nil, fmt.Errorf("%s returns google.longrunning.Operation but declares no google.longrunning.operation_info (AIP-151)", method.GoName)
		}
		return nil, fmt.Errorf("getting operation_info of %s: %w", method.GoName, err)
	}
	if operationInfo.GetResponseType() == "" || operationInfo.GetMetadataType() == "" {
		return nil, fmt.Errorf("%s: operation_info must set response_type and metadata_type", method.GoName)
	}
	if !proto.HasExtension(method.Desc.Options(), codegenschedulerpb.E_Method) {
		return nil, fmt.Errorf("%s returns google.longrunning.Operation but declares no (malonaz.scheduler.v1.method) annotation", method.GoName)
	}
	responseType, err := resolveResponseType(method, operationInfo.GetResponseType())
	if err != nil {
		return nil, err
	}
	parsed := &longrunningMethod{method: method, responseType: responseType}
	for _, field := range method.Input.Fields {
		switch field.Desc.Name() {
		case "parent", "name":
			if proto.HasExtension(field.Desc.Options(), annotations.E_ResourceReference) {
				parsed.resourceField = field
			}
		case "request_id":
			parsed.requestIDField = field
		}
	}
	if parsed.resourceField == nil {
		return nil, fmt.Errorf("%s must declare a `parent` or `name` field with a google.api.resource_reference: its job's parent derives from it", method.GoName)
	}
	return parsed, nil
}

// resolveResponseType resolves operation_info.response_type, which AIP-151
// allows to be relative to the method's package.
func resolveResponseType(method *protogen.Method, responseType string) (protoreflect.FullName, error) {
	fullName := protoreflect.FullName(responseType)
	if !fullName.IsValid() {
		return "", fmt.Errorf("%s: invalid operation_info.response_type %q", method.GoName, responseType)
	}
	if fullName.Parent() == "" {
		fullName = method.Desc.ParentFile().Package().Append(fullName.Name())
	}
	return fullName, nil
}

// runnerGoName is the interface the embedding service implements.
func runnerGoName(si *serviceInfo) string { return si.service.GoName + "Runner" }

// generateLongrunningServiceLevel emits the runner interface.
func (gen *generator) generateLongrunningServiceLevel(si *serviceInfo) error {
	g := gen.g
	g.P("// ", runnerGoName(si), " does the work of ", si.service.GoName, "'s long-running operations. The")
	g.P("// scheduler calls each Run method back with the request the operation was started")
	g.P("// with; the result is recorded as the operation's response or error.")
	g.P("type ", runnerGoName(si), " interface {")
	for _, lro := range si.lroMethods {
		responseType, err := gen.responseTypeIdent(lro)
		if err != nil {
			return err
		}
		g.P(fmt.Sprintf("  Run%s(ctx %s, request *%s) (*%s, error)",
			lro.method.GoName, gen.ident(contextPkg, "Context"), gen.qgi(lro.method.Input.GoIdent), responseType))
	}
	g.P("}")
	g.P()
	return nil
}

// responseTypeIdent resolves the operation's response message to its Go type.
func (gen *generator) responseTypeIdent(lro *longrunningMethod) (string, error) {
	for _, file := range gen.files {
		for _, message := range file.Messages {
			if message.Desc.FullName() == lro.responseType {
				return gen.qgi(message.GoIdent), nil
			}
		}
	}
	return "", fmt.Errorf("%s: operation_info.response_type %s is not a top-level message of the compilation unit", lro.method.GoName, lro.responseType)
}

// generateLongrunning emits the handler: a producer when the call comes from a
// client, a runner when it comes from the scheduler.
func (gen *generator) generateLongrunning(si *serviceInfo, lro *longrunningMethod) error {
	g := gen.g
	serverGoName := si.service.GoName + "Server"
	method := lro.method
	g.P(fmt.Sprintf("// %s starts the operation, or runs it when called by the scheduler (AIP-151).", method.GoName))
	g.P(fmt.Sprintf("func (s *%s) %s(ctx %s, request *%s) (*%s, error) {",
		serverGoName, method.GoName, gen.ident(contextPkg, "Context"), gen.qgi(method.Input.GoIdent), gen.ident(longrunningpbPkg, "Operation")))
	g.P(fmt.Sprintf("  if !%s(ctx) {", gen.ident(longrunningPkg, "IsRun")))
	g.P(fmt.Sprintf("    startRequest := &%s{", gen.ident(longrunningPkg, "StartRequest")))
	g.P(fmt.Sprintf("      Resource: request.Get%s(),", lro.resourceField.GoName))
	g.P("      Request:  request,")
	if lro.requestIDField != nil {
		g.P(fmt.Sprintf("      RequestID: request.Get%s(),", lro.requestIDField.GoName))
	}
	g.P("    }")
	g.P(fmt.Sprintf("    return %s(ctx, s.schedulerServiceClient, startRequest)", gen.ident(longrunningPkg, "Start")))
	g.P("  }")
	g.P(fmt.Sprintf("  response, err := s.runner.Run%s(ctx, request)", method.GoName))
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return %s(ctx, err)", gen.ident(longrunningPkg, "Failed")))
	g.P("  }")
	g.P(fmt.Sprintf("  return %s(ctx, response)", gen.ident(longrunningPkg, "Done")))
	g.P("}")
	g.P()
	return nil
}

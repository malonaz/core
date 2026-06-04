package authentication

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
)

// ErrMethodNotConfigured indicates no CEL rule exists for the requested method.
var ErrMethodNotConfigured = errors.New("method not configured")

// jwtAuthorizer holds compiled CEL programs per method for a single issuer.
type jwtAuthorizer struct {
	methodToProgram map[string]cel.Program
}

// resolveMethodInputDescriptor resolves the request message descriptor for a full RPC method name
// (e.g. "/package.Service/Method").
func resolveMethodInputDescriptor(fullMethod string) (protoreflect.MessageDescriptor, error) {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	lastSlash := strings.LastIndex(trimmed, "/")
	if lastSlash < 0 {
		return nil, fmt.Errorf("invalid full method format %q: missing slash separator", fullMethod)
	}
	serviceName := trimmed[:lastSlash]
	methodName := trimmed[lastSlash+1:]

	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return nil, fmt.Errorf("finding service descriptor for %q: %w", serviceName, err)
	}
	serviceDescriptor, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor %q is %T, not a service descriptor", serviceName, descriptor)
	}
	methodDescriptor := serviceDescriptor.Methods().ByName(protoreflect.Name(methodName))
	if methodDescriptor == nil {
		return nil, fmt.Errorf("method %q not found in service %q", methodName, serviceName)
	}
	return methodDescriptor.Input(), nil
}

func newJwtAuthorizer(jwtIssuer *authenticationpb.JwtIssuer) (*jwtAuthorizer, error) {
	// Resolve every referenced method's request descriptor up front so we can
	// register only those concrete types with CEL, rather than dumping the entire
	// global registry into a flat FileDescriptorSet (which breaks on ordering/dupes).
	methodToInputDescriptor := make(map[string]protoreflect.MessageDescriptor, len(jwtIssuer.MethodToAuthorizationCel))
	requestTypes := make([]proto.Message, 0, len(jwtIssuer.MethodToAuthorizationCel))
	for method := range jwtIssuer.MethodToAuthorizationCel {
		inputDescriptor, err := resolveMethodInputDescriptor(method)
		if err != nil {
			return nil, fmt.Errorf("resolving request type for method %q on issuer %q: %w", method, jwtIssuer.Id, err)
		}
		methodToInputDescriptor[method] = inputDescriptor
		requestTypes = append(requestTypes, dynamicpb.NewMessage(inputDescriptor))
	}

	// Build a single environment registering all referenced request types.
	// cel.Types resolves transitive field types through each message's own
	// descriptor, so we never touch the global registry's ordering.
	environmentOptions := []cel.EnvOption{
		cel.Variable("claims", cel.MapType(cel.StringType, cel.DynType)),
		ext.Protos(),
		ext.Strings(),
	}
	for _, requestType := range requestTypes {
		environmentOptions = append(environmentOptions, cel.Types(requestType))
	}
	environment, err := cel.NewEnv(environmentOptions...)
	if err != nil {
		return nil, fmt.Errorf("creating CEL environment for issuer %q: %w", jwtIssuer.Id, err)
	}

	methodToProgram := make(map[string]cel.Program, len(jwtIssuer.MethodToAuthorizationCel))
	for method, expression := range jwtIssuer.MethodToAuthorizationCel {
		inputDescriptor := methodToInputDescriptor[method]
		// Declare `request` as this method's concrete input type for compile-time field checks.
		methodEnvironment, err := environment.Extend(
			cel.Variable("request", cel.ObjectType(string(inputDescriptor.FullName()))),
		)
		if err != nil {
			return nil, fmt.Errorf("extending CEL environment for method %q on issuer %q: %w", method, jwtIssuer.Id, err)
		}

		ast, issues := methodEnvironment.Compile(expression)
		if issues != nil && issues.Err() != nil {
			return nil, fmt.Errorf("compiling CEL for method %q on issuer %q: %w", method, jwtIssuer.Id, issues.Err())
		}
		if ast.OutputType() != cel.BoolType {
			return nil, fmt.Errorf("CEL for method %q on issuer %q must return bool, got %s", method, jwtIssuer.Id, ast.OutputType())
		}
		program, err := methodEnvironment.Program(ast)
		if err != nil {
			return nil, fmt.Errorf("building CEL program for method %q on issuer %q: %w", method, jwtIssuer.Id, err)
		}
		methodToProgram[method] = program
	}
	return &jwtAuthorizer{methodToProgram: methodToProgram}, nil
}

// authorize evaluates the method's CEL expression against the claims and request.
// Returns ErrMethodNotConfigured if no rule exists for the method.
func (a *jwtAuthorizer) authorize(method string, claims *structpb.Struct, request any) (bool, error) {
	program, ok := a.methodToProgram[method]
	if !ok {
		return false, ErrMethodNotConfigured
	}

	output, _, err := program.Eval(map[string]any{
		"claims":  claims.AsMap(),
		"request": request,
	})
	if err != nil {
		return false, fmt.Errorf("evaluating CEL for method %q: %w", method, err)
	}
	allowed, ok := output.Value().(bool)
	if !ok {
		return false, fmt.Errorf("CEL for method %q did not return bool", method)
	}
	return allowed, nil
}

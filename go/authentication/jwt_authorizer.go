package authentication

import (
	"errors"
	"fmt"

	"github.com/google/cel-go/cel"
	"google.golang.org/protobuf/types/known/structpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
)

// ErrMethodNotConfigured indicates no CEL rule exists for the requested method.
var ErrMethodNotConfigured = errors.New("method not configured")

// jwtAuthorizer holds compiled CEL programs per method for a single issuer.
type jwtAuthorizer struct {
	methodToProgram map[string]cel.Program
}

func newJwtAuthorizer(jwtIssuer *authenticationpb.JwtIssuer) (*jwtAuthorizer, error) {
	environment, err := cel.NewEnv(
		cel.Variable("claims", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("request", cel.DynType),
	)
	if err != nil {
		return nil, fmt.Errorf("creating CEL environment: %w", err)
	}

	methodToProgram := make(map[string]cel.Program, len(jwtIssuer.MethodToAuthorizationCel))
	for method, expression := range jwtIssuer.MethodToAuthorizationCel {
		ast, issues := environment.Compile(expression)
		if issues != nil && issues.Err() != nil {
			return nil, fmt.Errorf("compiling CEL for method %q on issuer %q: %w", method, jwtIssuer.Id, issues.Err())
		}
		if ast.OutputType() != cel.BoolType {
			return nil, fmt.Errorf("CEL for method %q on issuer %q must return bool, got %s", method, jwtIssuer.Id, ast.OutputType())
		}
		program, err := environment.Program(ast)
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

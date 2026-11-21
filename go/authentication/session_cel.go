package authentication

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
)

// sessionCelOptions declare the variables every authorization CEL expression reads a session through: `session`,
// and `actor`, its delegation's actor, empty unless delegated.
var sessionCelOptions = []cel.EnvOption{
	cel.Types(&authenticationpb.Session{}),
	cel.Variable("session", cel.ObjectType(string((&authenticationpb.Session{}).ProtoReflect().Descriptor().FullName()))),
	cel.Variable("actor", cel.StringType),
	ext.Protos(),
	ext.Strings(),
}

// sessionCelVariables returns the values of the variables sessionCelOptions declare.
func sessionCelVariables(session *authenticationpb.Session) map[string]any {
	return map[string]any{
		"session": session,
		"actor":   session.GetDelegation().GetActor(),
	}
}

// compileBoolCel compiles a CEL expression that must evaluate to a bool.
func compileBoolCel(environment *cel.Env, expression string) (cel.Program, error) {
	ast, issues := environment.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	if ast.OutputType() != cel.BoolType {
		return nil, fmt.Errorf("must return bool, got %s", ast.OutputType())
	}
	return environment.Program(ast)
}

// evalBool evaluates a program compiled by compileBoolCel.
func evalBool(program cel.Program, variables map[string]any) (bool, error) {
	output, _, err := program.Eval(variables)
	if err != nil {
		return false, err
	}
	allowed, ok := output.Value().(bool)
	if !ok {
		return false, fmt.Errorf("did not return bool")
	}
	return allowed, nil
}

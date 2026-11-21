package authentication

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/ext"
	"google.golang.org/protobuf/types/known/structpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
)

// mappingContainer resolves short names in mapping expressions, so that `Grant{...}` names
// malonaz.authentication.v2.Grant. Conditions have none: names in v1's rules resolve as they did.
const mappingContainer = "malonaz.authentication.v2"

var (
	principalType = cel.ObjectType(string((&authenticationpb.Principal{}).ProtoReflect().Descriptor().FullName()))
	claimsType    = cel.MapType(cel.StringType, cel.DynType)
)

// Expressions read when an issuer leaves one unset.
const (
	defaultSubjectExpression = `claims.sub`
	defaultActorExpression   = `has(claims.act) ? claims.act.sub : ""`
)

// newEnvironment is what every expression may use: v1's extensions, the rn.* functions and this package's types.
func newEnvironment(options ...cel.EnvOption) (*cel.Env, error) {
	environmentOptions := []cel.EnvOption{
		cel.Types(&authenticationpb.Grant{}, &authenticationpb.Principal{}),
		ext.Protos(),
		ext.Strings(),
	}
	environmentOptions = append(environmentOptions, resourceNameCelOptions...)
	return cel.NewEnv(append(environmentOptions, options...)...)
}

// compileMapping compiles an expression mapping claims, whose result must be of the given kind or dynamic,
// checked when it runs.
func compileMapping(environment *cel.Env, expression string, outputKind types.Kind) (cel.Program, error) {
	ast, issues := environment.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	if kind := ast.OutputType().Kind(); kind != outputKind && kind != types.DynKind {
		return nil, fmt.Errorf("must evaluate to a %s, got %s", kindName[outputKind], ast.OutputType())
	}
	return environment.Program(ast)
}

// compileCondition compiles a rule's condition, which must be a bool when checked, as in v1.
func compileCondition(environment *cel.Env, expression string) (cel.Program, error) {
	ast, issues := environment.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	if ast.OutputType() != cel.BoolType {
		return nil, fmt.Errorf("must evaluate to a bool, got %s", ast.OutputType())
	}
	return environment.Program(ast)
}

var kindName = map[types.Kind]string{
	types.StringKind: "string",
	types.MapKind:    "map",
	types.ListKind:   "list",
}

// evaluateString runs a program that must yield a string.
func evaluateString(program cel.Program, activation map[string]any) (string, error) {
	value, _, err := program.Eval(activation)
	if err != nil {
		return "", err
	}
	result, ok := value.Value().(string)
	if !ok {
		return "", fmt.Errorf("evaluated to %s, not a string", value.Type())
	}
	return result, nil
}

// evaluateStruct runs a program that must yield a map, converted the way v1 rewrote claims so attributes read the same.
func evaluateStruct(program cel.Program, activation map[string]any) (*structpb.Struct, error) {
	value, _, err := program.Eval(activation)
	if err != nil {
		return nil, err
	}
	native, err := value.ConvertToNative(reflect.TypeOf(map[string]any{}))
	if err != nil {
		return nil, fmt.Errorf("converting to a map: %w", err)
	}
	bytes, err := json.Marshal(native)
	if err != nil {
		return nil, fmt.Errorf("marshaling: %w", err)
	}
	result := &structpb.Struct{}
	if err := result.UnmarshalJSON(bytes); err != nil {
		return nil, fmt.Errorf("unmarshaling: %w", err)
	}
	return result, nil
}

// evaluateGrants runs a program that must yield a list of grants.
func evaluateGrants(program cel.Program, activation map[string]any) ([]*authenticationpb.Grant, error) {
	value, _, err := program.Eval(activation)
	if err != nil {
		return nil, err
	}
	native, err := value.ConvertToNative(reflect.TypeOf([]*authenticationpb.Grant{}))
	if err != nil {
		return nil, fmt.Errorf("converting to grants: %w", err)
	}
	return native.([]*authenticationpb.Grant), nil
}

// evaluateCondition runs a condition, which yields a bool.
func evaluateCondition(program cel.Program, activation map[string]any) (bool, error) {
	value, _, err := program.Eval(activation)
	if err != nil {
		return false, err
	}
	allowed, ok := value.Value().(bool)
	if !ok {
		return false, fmt.Errorf("evaluated to %s, not a bool", value.Type())
	}
	return allowed, nil
}

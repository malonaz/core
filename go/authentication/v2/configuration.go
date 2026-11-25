package authentication

import (
	"fmt"
	"regexp"
	"strings"

	"buf.build/go/protovalidate"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/jsonnet"
	"github.com/malonaz/core/go/pbutil"
)

// Opts configures the interceptor.
type Opts struct {
	Config       string `long:"config" env:"CONFIG" description:"Path to the authentication configuration (jsonnet)" required:"true"`
	APIKeys      string `long:"api-keys" env:"API_KEYS" description:"Comma-separated service_account_id:api_key pairs; an account may hold several keys"`
	APIKeyHeader string `long:"api-key-header" env:"API_KEY_HEADER" description:"Header carrying an API key" default:"x-api-key"`
	CookieName   string `long:"cookie-name" env:"COOKIE_NAME" description:"Cookie read for a token when the call carries no other credential" default:"access_token"`
	ServerSecret string `long:"server-secret" env:"SERVER_SECRET" description:"HS256 secret the platform's servers sign and verify their tokens with" required:"true"`
}

// LoadConfiguration evaluates a jsonnet configuration, strictly, and validates it.
func LoadConfiguration(path string) (*authenticationpb.Configuration, error) {
	bytes, err := jsonnet.EvaluateFile(path, jsonnet.WithEnvVariables())
	if err != nil {
		return nil, fmt.Errorf("evaluating configuration %s: %w", path, err)
	}
	configuration := &authenticationpb.Configuration{}
	if err := pbutil.JSONUnmarshalStrict(bytes, configuration); err != nil {
		return nil, fmt.Errorf("parsing configuration %s: %w", path, err)
	}
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("instantiating validator: %w", err)
	}
	if err := validator.Validate(configuration); err != nil {
		return nil, fmt.Errorf("validating configuration %s: %w", path, err)
	}
	return configuration, nil
}

// methodToConditions is what a role allows: per method, its rules' conditions, nil for a rule without one.
type methodToConditions map[string][]*condition

// condition is a rule's compiled condition.
type condition struct {
	program cel.Program
	// readsRequest is whether it reads `request`, which only a call can decide.
	readsRequest bool
}

// roleCompiler compiles roles into the methods they allow, resolving includes and method patterns against the
// registered methods.
type roleCompiler struct {
	roleIDToRole               map[string]*authenticationpb.Role
	roleIDToMethodToConditions map[string]methodToConditions
	resolving                  map[string]bool
	methodToDescriptor         map[string]protoreflect.MethodDescriptor
	environment                *cel.Env
	keyToCondition             map[conditionKey]*condition
}

// conditionKey is a condition compiled against a request type, or against none when it does not read `request`.
type conditionKey struct {
	expression string
	input      protoreflect.FullName
}

// compileRoles returns, per role, the methods it allows with their conditions, the role's includes folded in.
func compileRoles(roles []*authenticationpb.Role, methodToDescriptor map[string]protoreflect.MethodDescriptor) (map[string]methodToConditions, error) {
	environment, err := newEnvironment(
		// cel.bind lets a host keep rule texts written over `claims` and `subject`, binding those names around them.
		ext.Bindings(),
		cel.Variable("principal", principalType),
		cel.Variable("actor", principalType),
		cel.Variable("caller", principalType),
	)
	if err != nil {
		return nil, fmt.Errorf("creating condition environment: %w", err)
	}
	compiler := &roleCompiler{
		roleIDToRole:               make(map[string]*authenticationpb.Role, len(roles)),
		roleIDToMethodToConditions: make(map[string]methodToConditions, len(roles)),
		resolving:                  map[string]bool{},
		methodToDescriptor:         methodToDescriptor,
		environment:                environment,
		keyToCondition:             map[conditionKey]*condition{},
	}
	for _, role := range roles {
		compiler.roleIDToRole[role.GetId()] = role
	}
	for _, role := range roles {
		if _, err := compiler.resolve(role.GetId()); err != nil {
			return nil, err
		}
	}
	return compiler.roleIDToMethodToConditions, nil
}

func (c *roleCompiler) resolve(roleID string) (methodToConditions, error) {
	if resolved, ok := c.roleIDToMethodToConditions[roleID]; ok {
		return resolved, nil
	}
	role, ok := c.roleIDToRole[roleID]
	if !ok {
		return nil, fmt.Errorf("unknown role %q", roleID)
	}
	if c.resolving[roleID] {
		return nil, fmt.Errorf("role %q includes itself", roleID)
	}
	c.resolving[roleID] = true
	defer delete(c.resolving, roleID)

	resolved := methodToConditions{}
	for _, rule := range role.GetRules() {
		methods, err := c.expand(rule.GetMethods())
		if err != nil {
			return nil, fmt.Errorf("role %q: %w", roleID, err)
		}
		for _, method := range methods {
			condition, err := c.condition(rule.GetConditionCel(), c.methodToDescriptor[method])
			if err != nil {
				return nil, fmt.Errorf("role %q: condition on %s: %w", roleID, method, err)
			}
			resolved[method] = append(resolved[method], condition)
		}
	}
	for _, includedRoleID := range role.GetIncludes() {
		included, err := c.resolve(includedRoleID)
		if err != nil {
			return nil, fmt.Errorf("role %q includes %w", roleID, err)
		}
		for method, conditions := range included {
			resolved[method] = append(resolved[method], conditions...)
		}
	}
	c.roleIDToMethodToConditions[roleID] = resolved
	return resolved, nil
}

// expand turns method patterns into registered methods. "*" matches any run of characters, dots and slashes
// included; every pattern must match a registered method.
func (c *roleCompiler) expand(patterns []string) ([]string, error) {
	var methods []string
	for _, pattern := range patterns {
		if !strings.Contains(pattern, "*") {
			if _, ok := c.methodToDescriptor[pattern]; !ok {
				return nil, fmt.Errorf("method %q is not registered", pattern)
			}
			methods = append(methods, pattern)
			continue
		}
		expression, err := compileMethodPattern(pattern)
		if err != nil {
			return nil, err
		}
		matched := false
		for method := range c.methodToDescriptor {
			if expression.MatchString(method) {
				methods = append(methods, method)
				matched = true
			}
		}
		if !matched {
			return nil, fmt.Errorf("method pattern %q matches no registered method", pattern)
		}
	}
	return methods, nil
}

func compileMethodPattern(pattern string) (*regexp.Regexp, error) {
	expression := strings.ReplaceAll("^"+regexp.QuoteMeta(pattern)+"$", `\*`, `.*`)
	compiled, err := regexp.Compile(expression)
	if err != nil {
		return nil, fmt.Errorf("compiling method pattern %q: %w", pattern, err)
	}
	return compiled, nil
}

// condition compiles a condition for a method. Most conditions do not read `request`: compiled without it, they
// compile once for all methods instead of once per request type. One that compiles only with it reads it.
func (c *roleCompiler) condition(expression string, method protoreflect.MethodDescriptor) (*condition, error) {
	if expression == "" {
		return nil, nil
	}
	withoutRequest := conditionKey{expression: expression}
	withRequest := conditionKey{expression: expression, input: method.Input().FullName()}
	for _, key := range []conditionKey{withoutRequest, withRequest} {
		if condition, ok := c.keyToCondition[key]; ok {
			return condition, nil
		}
	}
	program, err := compileCondition(c.environment, expression)
	if err == nil {
		c.keyToCondition[withoutRequest] = &condition{program: program}
		return c.keyToCondition[withoutRequest], nil
	}
	if method.IsStreamingClient() && method.IsStreamingServer() {
		return nil, fmt.Errorf("%w (`request` is not declared for a bidirectional stream)", err)
	}
	environment, err := c.environment.Extend(
		cel.Types(dynamicpb.NewMessage(method.Input())),
		cel.Variable("request", cel.ObjectType(string(method.Input().FullName()))),
	)
	if err != nil {
		return nil, fmt.Errorf("declaring request %s: %w", method.Input().FullName(), err)
	}
	if program, err = compileCondition(environment, expression); err != nil {
		return nil, err
	}
	c.keyToCondition[withRequest] = &condition{program: program, readsRequest: true}
	return c.keyToCondition[withRequest], nil
}

// registeredMethods returns every method of every service registered in the process.
func registeredMethods() map[string]protoreflect.MethodDescriptor {
	methodToDescriptor := map[string]protoreflect.MethodDescriptor{}
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		services := file.Services()
		for i := range services.Len() {
			service := services.Get(i)
			methods := service.Methods()
			for j := range methods.Len() {
				method := methods.Get(j)
				methodToDescriptor["/"+string(service.FullName())+"/"+string(method.Name())] = method
			}
		}
		return true
	})
	return methodToDescriptor
}

package aip

import (
	"fmt"
	"strings"

	"go.einride.tech/aip/filtering"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

	canonicalizepb "github.com/malonaz/core/genproto/canonicalize/v1"
	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/aip/transpiler/postgres"
	"github.com/malonaz/core/go/canonicalize"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

type filteringRequest interface {
	proto.Message
	filtering.Request
	SetFilter(string)
}

type FilteringRequestOpt func(*filteringRequestOpts)

type filteringRequestOpts struct {
	withFQN bool
}

// WithFQN prepends table names to column references in generated SQL.
func WithFQN() FilteringRequestOpt {
	return func(o *filteringRequestOpts) {
		o.withFQN = true
	}
}

type FilteringRequestParser[T filteringRequest, R proto.Message] struct {
	declarations      *filtering.Declarations
	macroDeclarations *filtering.Declarations
	macros            []filtering.Macro
	tree              *Tree
}

func MustNewFilteringRequestParser[T filteringRequest, R proto.Message](opts ...FilteringRequestOpt) *FilteringRequestParser[T, R] {
	parser, err := NewFilteringRequestParser[T, R](opts...)
	if err != nil {
		panic(err)
	}
	return parser
}

func NewFilteringRequestParser[T filteringRequest, R proto.Message](opts ...FilteringRequestOpt) (*FilteringRequestParser[T, R], error) {
	var options filteringRequestOpts
	for _, opt := range opts {
		opt(&options)
	}

	var zero T
	filteringOptions, err := pbutil.GetMessageOption[*aippb.FilteringOptions](zero, aippb.E_Filtering)
	if err != nil {
		return nil, fmt.Errorf("getting filtering options: %v", err)
	}

	var zeroResource R
	if err := pbfieldmask.FromPaths(filteringOptions.GetPaths()...).Validate(zeroResource); err != nil {
		return nil, fmt.Errorf("validating paths: %w", err)
	}

	tree, err := BuildResourceTree[R](WithAllowedPaths(filteringOptions.GetPaths()))
	if err != nil {
		return nil, err
	}

	declarations, macroDeclarations, macros, err := NewFilterDeclarations(tree, options.withFQN)
	if err != nil {
		return nil, err
	}

	return &FilteringRequestParser[T, R]{
		declarations:      declarations,
		macroDeclarations: macroDeclarations,
		macros:            macros,
		tree:              tree,
	}, nil
}

func (p *FilteringRequestParser[T, R]) Parse(request T) (*FilteringRequest, error) {
	filter, err := filtering.ParseFilter(request, p.declarations)
	if err != nil {
		return nil, fmt.Errorf("parsing filter: %w", err)
	}
	if filter.CheckedExpr != nil {
		filter, err = filtering.ApplyMacros(filter, p.macroDeclarations, p.macros...)
		if err != nil {
			return nil, fmt.Errorf("applying macros: %w", err)
		}
	}
	whereClause, whereParams, err := postgres.TranspileFilter(filter)
	if err != nil {
		return nil, fmt.Errorf("transpiling filter to SQL: %w", err)
	}
	return &FilteringRequest{
		request:     request,
		filter:      filter,
		whereClause: whereClause,
		whereParams: whereParams,
	}, nil
}

type FilteringRequest struct {
	request     filtering.Request
	filter      filtering.Filter
	whereClause string
	whereParams []any
}

func (f *FilteringRequest) GetSQLWhereClause() (string, []any) {
	return f.whereClause, f.whereParams
}

func (f *FilteringRequest) GetFilter() filtering.Filter {
	return f.filter
}

// NewFilterDeclarations builds the filter declarations of a resource tree:
// idents matching proto field paths, macro idents matching database column
// references, and the macros rewriting the former into the latter. withFQN
// qualifies column references with their table (or join alias).
func NewFilterDeclarations(tree *Tree, withFQN bool) (declarations, macroDeclarations *filtering.Declarations, macros []filtering.Macro, err error) {
	sharedDeclarationOptions := []filtering.DeclarationOption{
		filtering.DeclareIdent("true", filtering.TypeBool),
		filtering.DeclareIdent("false", filtering.TypeBool),
		filtering.DeclareStandardFunctions(),
	}
	declarationOptions := []filtering.DeclarationOption{}      // ident declarations matching proto fields.
	macroDeclarationOptions := []filtering.DeclarationOption{} // ident declarations matching db column names.

	identNameToFQN := map[string]string{}
	// Ident name (proto path or FQN) -> canonicalization rule, used to
	// canonicalize filter values the same way stored values were canonicalized.
	identNameToCanonicalizeRule := map[string]*canonicalizepb.Field{}
	for node := range tree.FilterableNodes() {
		if node.ExprType == nil && node.EnumType == nil {
			continue
		}

		fqn := node.Path
		if node.ReplacementPath != "" {
			fqn = node.ReplacementPath
		}
		if withFQN {
			fqn = node.TableName + "." + fqn
		}
		identNameToFQN[node.Path] = fqn
		if node.Canonicalize != nil {
			identNameToCanonicalizeRule[node.Path] = node.Canonicalize
			identNameToCanonicalizeRule[fqn] = node.Canonicalize
		}

		if node.ExprType != nil {
			ident := filtering.DeclareIdent(node.Path, node.ExprType)
			function := filtering.DeclareFunction(filtering.FunctionHas,
				filtering.NewFunctionOverload(
					fmt.Sprintf("%s_%s_string", filtering.FunctionHas, node.Path),
					filtering.TypeBool, node.ExprType, filtering.TypeString,
				),
			)
			declarationOptions = append(declarationOptions, ident, function)
			{
				ident := filtering.DeclareIdent(fqn, node.ExprType)
				function := filtering.DeclareFunction(filtering.FunctionHas,
					filtering.NewFunctionOverload(
						fmt.Sprintf("%s_%s_string", filtering.FunctionHas, fqn),
						filtering.TypeBool, node.ExprType, filtering.TypeString,
					),
				)
				macroDeclarationOptions = append(macroDeclarationOptions, ident, function)
			}
		}

		if node.EnumType != nil {
			ident := filtering.DeclareEnumIdent(node.Path, node.EnumType)
			function := filtering.DeclareFunction(filtering.FunctionHas,
				filtering.NewFunctionOverload(
					fmt.Sprintf("%s_%s_string", filtering.FunctionHas, node.Path),
					filtering.TypeBool, filtering.TypeEnum(node.EnumType), filtering.TypeString,
				),
			)
			declarationOptions = append(declarationOptions, ident, function)
			{
				ident := filtering.DeclareEnumIdent(fqn, node.EnumType)
				function := filtering.DeclareFunction(filtering.FunctionHas,
					filtering.NewFunctionOverload(
						fmt.Sprintf("%s_%s_string", filtering.FunctionHas, fqn),
						filtering.TypeBool, filtering.TypeEnum(node.EnumType), filtering.TypeString,
					),
				)
				macroDeclarationOptions = append(macroDeclarationOptions, ident, function)
			}
		}
	}

	// Canonicalize string constants compared against canonicalized fields, so
	// filters match the canonicalized values stored in the database.
	macros = append(macros, func(cursor *filtering.Cursor) {
		callExpr := cursor.Expr().GetCallExpr()
		if callExpr == nil || len(callExpr.GetArgs()) != 2 {
			return
		}
		switch callExpr.GetFunction() {
		case filtering.FunctionEquals, filtering.FunctionNotEquals, filtering.FunctionHas:
		default:
			return
		}
		rule, ok := identNameToCanonicalizeRule[exprPath(callExpr.GetArgs()[0])]
		if !ok {
			return
		}
		constExpr := callExpr.GetArgs()[1].GetConstExpr()
		if constExpr == nil {
			return
		}
		value := constExpr.GetStringValue()
		if value == "" {
			return
		}
		// Full canonicalization needs the whole value, so wildcard patterns are
		// only best-effort: emails get lowercased/trimmed (matching the stored
		// casing), phones are left untouched (users must query in E.164 form).
		wildcard := strings.Contains(value, "*")
		switch rule.GetRule().(type) {
		case *canonicalizepb.Field_EmailAddress:
			if wildcard {
				constExpr.ConstantKind = &expr.Constant_StringValue{StringValue: strings.ToLower(strings.TrimSpace(value))}
				return
			}
			constExpr.ConstantKind = &expr.Constant_StringValue{StringValue: canonicalize.EmailAddress(value)}
		case *canonicalizepb.Field_PhoneNumber:
			if wildcard {
				return
			}
			// On parse failure keep the raw value; the filter simply won't match.
			if canonicalized, err := canonicalize.PhoneNumber(value, canonicalize.RegionCodeUS); err == nil {
				constExpr.ConstantKind = &expr.Constant_StringValue{StringValue: canonicalized}
			}
		}
	})

	macros = append(macros, func(cursor *filtering.Cursor) {
		identExpr := cursor.Expr().GetIdentExpr()
		if fqn, ok := identNameToFQN[identExpr.GetName()]; ok {
			cursor.Replace(filtering.Text(fqn))
		}
	})

	declarationOptions = append(declarationOptions, sharedDeclarationOptions...)
	macroDeclarationOptions = append(macroDeclarationOptions, sharedDeclarationOptions...)

	declarations, err = filtering.NewDeclarations(declarationOptions...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating filter declarations: %w", err)
	}
	macroDeclarations, err = filtering.NewDeclarations(macroDeclarationOptions...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating filter macro declarations: %w", err)
	}
	return declarations, macroDeclarations, macros, nil
}

// exprPath flattens an ident or select-expression chain into a dotted path
// (e.g. metadata.email_address); returns "" for other expression kinds.
func exprPath(e *expr.Expr) string {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		return kind.IdentExpr.GetName()
	case *expr.Expr_SelectExpr:
		operand := exprPath(kind.SelectExpr.GetOperand())
		if operand == "" {
			return ""
		}
		return operand + "." + kind.SelectExpr.GetField()
	default:
		return ""
	}
}

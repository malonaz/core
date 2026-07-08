package aip

import (
	"fmt"

	"go.einride.tech/aip/filtering"
	"google.golang.org/protobuf/proto"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/aip/transpiler/postgres"
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

	var (
		sharedDeclarationOptions []filtering.DeclarationOption // Shared declarations.
		declarationOptions       []filtering.DeclarationOption // ident declarations matching proto fields.
		macroDeclarationOptions  []filtering.DeclarationOption // ident declarations matching db column names.
		macros                   []filtering.Macro
	)

	// Declare boolean constants
	sharedDeclarationOptions = append(sharedDeclarationOptions,
		filtering.DeclareIdent("true", filtering.TypeBool),
		filtering.DeclareIdent("false", filtering.TypeBool),
		filtering.DeclareStandardFunctions(),
	)

	identNameToFQN := map[string]string{}
	for node := range tree.FilterableNodes() {
		if node.ExprType == nil && node.EnumType == nil {
			continue
		}

		fqn := node.Path
		if node.ReplacementPath != "" {
			fqn = node.ReplacementPath
		}
		if options.withFQN {
			fqn = node.TableName + "." + fqn
		}
		identNameToFQN[node.Path] = fqn

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

	macros = append(macros, func(cursor *filtering.Cursor) {
		identExpr := cursor.Expr().GetIdentExpr()
		if fqn, ok := identNameToFQN[identExpr.GetName()]; ok {
			cursor.Replace(filtering.Text(fqn))
		}
	})

	declarationOptions = append(declarationOptions, sharedDeclarationOptions...)
	macroDeclarationOptions = append(macroDeclarationOptions, sharedDeclarationOptions...)

	declarations, err := filtering.NewDeclarations(declarationOptions...)
	if err != nil {
		return nil, fmt.Errorf("creating filter declarations: %w", err)
	}

	macroDeclarations, err := filtering.NewDeclarations(macroDeclarationOptions...)
	if err != nil {
		return nil, fmt.Errorf("creating filter macro declarations: %w", err)
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

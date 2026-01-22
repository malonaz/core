// go/aip/filtering_request.go
package aip

import (
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	"go.einride.tech/aip/filtering"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/go/aip/transpiler/postgres"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

type filteringRequest interface {
	proto.Message
	filtering.Request
}

type FilteringRequestParser[T filteringRequest, R proto.Message] struct {
	validator    protovalidate.Validator
	declarations *filtering.Declarations
	tree         *Tree
}

func MustNewFilteringRequestParser[T filteringRequest, R proto.Message]() *FilteringRequestParser[T, R] {
	parser, err := NewFilteringRequestParser[T, R]()
	if err != nil {
		panic(err)
	}
	return parser
}

func NewFilteringRequestParser[T filteringRequest, R proto.Message]() (*FilteringRequestParser[T, R], error) {
	validator, err := protovalidate.New(
		protovalidate.WithDisableLazy(),
		protovalidate.WithMessages(&aippb.FilteringOptions{}, &modelpb.FieldOpts{}),
	)
	if err != nil {
		return nil, fmt.Errorf("instantiating validator for filtering request parser: %v", err)
	}

	var zero T
	filteringOptions, err := pbutil.GetMessageOption[*aippb.FilteringOptions](zero, aippb.E_Filtering)
	if err != nil {
		return nil, fmt.Errorf("getting filtering options: %v", err)
	}
	if err := validator.Validate(filteringOptions); err != nil {
		return nil, fmt.Errorf("validating options: %v", err)
	}

	var zeroResource R
	sanitizedPaths := make([]string, 0, len(filteringOptions.GetPaths()))
	for _, path := range filteringOptions.GetPaths() {
		sanitizedPaths = append(sanitizedPaths, strings.TrimSuffix(path, ".*"))
	}
	if err := pbfieldmask.FromPaths(sanitizedPaths...).Validate(zeroResource); err != nil {
		return nil, fmt.Errorf("validating paths: %w", err)
	}

	tree, err := BuildResourceTree[R](WithAllowedPaths(filteringOptions.GetPaths()))
	if err != nil {
		return nil, err
	}

	var declarationOptions []filtering.DeclarationOption

	// Declare boolean constants
	declarationOptions = append(declarationOptions,
		filtering.DeclareIdent("true", filtering.TypeBool),
		filtering.DeclareIdent("false", filtering.TypeBool),
	)

	for node := range tree.FilterableNodes() {
		replacementPath := node.Path
		if node.ReplacementPath != "" {
			replacementPath = node.ReplacementPath
		}

		if node.ExprType != nil {
			declarationOptions = append(declarationOptions, filtering.DeclareIdent(replacementPath, node.ExprType))
			// Add has overload for presence check (field:*)
			declarationOptions = append(declarationOptions,
				filtering.DeclareFunction(filtering.FunctionHas,
					filtering.NewFunctionOverload(
						fmt.Sprintf("%s_%s_string", filtering.FunctionHas, replacementPath),
						filtering.TypeBool,
						node.ExprType,
						filtering.TypeString,
					),
				),
			)
		}
		if node.EnumType != nil {
			declarationOptions = append(declarationOptions, filtering.DeclareEnumIdent(replacementPath, node.EnumType))
			// Add has overload for enum presence check
			declarationOptions = append(declarationOptions,
				filtering.DeclareFunction(filtering.FunctionHas,
					filtering.NewFunctionOverload(
						fmt.Sprintf("%s_%s_string", filtering.FunctionHas, replacementPath),
						filtering.TypeBool,
						filtering.TypeEnum(node.EnumType),
						filtering.TypeString,
					),
				),
			)
		}
	}

	declarationOptions = append(declarationOptions, filtering.DeclareStandardFunctions())

	declarations, err := filtering.NewDeclarations(declarationOptions...)
	if err != nil {
		return nil, fmt.Errorf("creating filter declarations: %w", err)
	}

	return &FilteringRequestParser[T, R]{
		validator:    validator,
		declarations: declarations,
		tree:         tree,
	}, nil
}

func (p *FilteringRequestParser[T, R]) Parse(request T) (*FilteringRequest, error) {
	filterClause := request.GetFilter()
	// Handle id column changes here.
	for node := range p.tree.FilterableNodes() {
		filterClause = node.ApplyReplacement(filterClause)
	}
	p.setFilter(request, filterClause)

	filter, err := filtering.ParseFilter(request, p.declarations)
	if err != nil {
		return nil, fmt.Errorf("parsing filter: %w", err)
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

func (p *FilteringRequestParser[T, R]) setFilter(request filteringRequest, filter string) {
	msgReflect := request.ProtoReflect()
	fields := msgReflect.Descriptor().Fields()
	filterField := fields.ByName("filter")
	msgReflect.Set(filterField, protoreflect.ValueOfString(filter))
}

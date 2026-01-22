package aip

import (
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	"go.einride.tech/aip/filtering"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
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
	isNullFunctionOverloads := getIsNullFunctionDefaultOverloads()

	for node := range tree.FilterableNodes() {
		replacementPath := node.Path
		if node.ReplacementPath != "" {
			replacementPath = node.ReplacementPath
		}

		if node.ExprType != nil {
			declarationOptions = append(declarationOptions, filtering.DeclareIdent(replacementPath, node.ExprType))
		}
		if node.EnumType != nil {
			declarationOptions = append(declarationOptions, filtering.DeclareEnumIdent(replacementPath, node.EnumType))
			if node.Nullable || node.Depth > 0 {
				enumIdentType := filtering.TypeEnum(node.EnumType)
				isNullOverload := filtering.NewFunctionOverload(
					postgres.FunctionIsNull+"_"+enumIdentType.GetMessageType(), filtering.TypeBool, enumIdentType,
				)
				isNullFunctionOverloads = append(isNullFunctionOverloads, isNullOverload)
			}
		}
	}

	declarationOptions = append(declarationOptions, filtering.DeclareStandardFunctions())
	isNullDeclarationOption := filtering.DeclareFunction(postgres.FunctionIsNull, isNullFunctionOverloads...)
	declarationOptions = append(declarationOptions, isNullDeclarationOption)

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

func getIsNullFunctionDefaultOverloads() []*v1alpha1.Decl_FunctionDecl_Overload {
	return []*v1alpha1.Decl_FunctionDecl_Overload{
		filtering.NewFunctionOverload(postgres.FunctionIsNull+"_string", filtering.TypeBool, filtering.TypeString),
		filtering.NewFunctionOverload(postgres.FunctionIsNull+"_enum", filtering.TypeBool, filtering.TypeString),
		filtering.NewFunctionOverload(postgres.FunctionIsNull+"_bool", filtering.TypeBool, filtering.TypeBool),
		filtering.NewFunctionOverload(postgres.FunctionIsNull+"_int", filtering.TypeBool, filtering.TypeInt),
		filtering.NewFunctionOverload(postgres.FunctionIsNull+"_float", filtering.TypeBool, filtering.TypeFloat),
	}
}

func (p *FilteringRequestParser[T, R]) setFilter(request filteringRequest, filter string) {
	// Get the protobuf message descriptor
	msgReflect := request.ProtoReflect()
	// Get the field descriptor for "filter"
	fields := msgReflect.Descriptor().Fields()
	filterField := fields.ByName("filter")
	// Set the filter value
	msgReflect.Set(filterField, protoreflect.ValueOfString(filter))
}

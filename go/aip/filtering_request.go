package aip

import (
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/spanner-aip/spanfiltering"
	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

// //////////////////////////// INTERFACE //////////////////////////
type filteringRequest interface {
	proto.Message
	filtering.Request
}

// //////////////////////////// PARSER //////////////////////////
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

	// Parse options from the generic type T
	var zero T
	filteringOptions, err := pbutil.GetMessageOption[*aippb.FilteringOptions](zero, aippb.E_Filtering)
	if err != nil {
		return nil, fmt.Errorf("getting filtering options: %v", err)
	}
	// Validate options
	if err := validator.Validate(filteringOptions); err != nil {
		return nil, fmt.Errorf("validating options: %v", err)
	}

	// Validate the paths.
	var zeroResource R
	sanitizedPaths := make([]string, 0, len(filteringOptions.GetPaths()))
	for _, path := range filteringOptions.GetPaths() {
		sanitizedPaths = append(sanitizedPaths, strings.TrimSuffix(path, ".*"))
	}
	if err := pbfieldmask.FromPaths(sanitizedPaths...).Validate(zeroResource); err != nil {
		return nil, fmt.Errorf("validating paths: %w", err)
	}

	// Create a tree and explore.
	tree, err := BuildResourceTree[R](WithAllowedPaths(filteringOptions.GetPaths()), WithTransformNestedPath())
	if err != nil {
		return nil, err
	}

	// Instantiate data we need to collect.
	var declarationOptions []filtering.DeclarationOption
	isNullFunctionOverloads := getIsNullFunctionDefaultOverloads()

	for node := range tree.AllowedNodes() {
		if node.HasFieldBehavior(annotationspb.FieldBehavior_IDENTIFIER) || node.HasFieldBehavior(annotationspb.FieldBehavior_INPUT_ONLY) {
			continue
		}

		replacementPath := node.Path
		if node.ReplacementPath != "" {
			replacementPath = node.ReplacementPath
		}

		if node.ExprType != nil {
			declarationOptions = append(declarationOptions, filtering.DeclareIdent(replacementPath, node.ExprType))
		}
		if node.EnumType != nil {
			declarationOptions = append(declarationOptions, filtering.DeclareEnumIdent(replacementPath, node.EnumType))
			if node.Nullable || node.Depth > 0 { // It's either a root nullable field or a nullable-by-default JSONB value.
				enumIdentType := filtering.TypeEnum(node.EnumType)
				isNullOverload := filtering.NewFunctionOverload(
					spanfiltering.FunctionIsNull+"_"+enumIdentType.GetMessageType(), filtering.TypeBool, enumIdentType,
				)
				isNullFunctionOverloads = append(isNullFunctionOverloads, isNullOverload)
			}
		}
	}

	// Standard functions.
	declarationOptions = append(declarationOptions, filtering.DeclareStandardFunctions())
	// Jsonb function.
	declarationOptions = append(declarationOptions, jsonbFunctionDeclarationOption)
	// Construct the isNull declaration option
	isNullDeclarationOption := filtering.DeclareFunction(spanfiltering.FunctionIsNull, isNullFunctionOverloads...)
	declarationOptions = append(declarationOptions, isNullDeclarationOption)

	// Build the declarations
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
	// A nested field is a path in a proto message like "hello.hi.how"
	// We need to replace those with `JSONB(hello@hi@now)`
	// We must be careful to ensure that if there's a `hello.hi` declared and a `hello.hi.now` declared.
	// We don't do `JSONB(hello.hi).now` / this would be problematic.
	for node := range p.tree.AllowedNodes() {
		filterClause = node.ApplyReplacement(filterClause)
	}
	p.setFilter(request, filterClause)

	// Parse filtering
	filter, err := filtering.ParseFilter(request, p.declarations)
	if err != nil {
		return nil, fmt.Errorf("parsing filter: %w", err)
	}

	// Transpile to SQL
	whereClause, whereParams, err := spanfiltering.TranspileFilter(filter)
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

// //////////////////////////// PARSED REQUEST //////////////////////////
type FilteringRequest struct {
	request     filtering.Request
	filter      filtering.Filter
	whereClause string
	whereParams []any
}

func (f *FilteringRequest) GetSQLWhereClause() (string, []any) {
	return f.whereClause, f.whereParams
}

// /////////////////////////// UTILS //////////////////////////////
func (p *FilteringRequestParser[T, R]) setFilter(request filteringRequest, filter string) {
	// Get the protobuf message descriptor
	msgReflect := request.ProtoReflect()
	// Get the field descriptor for "filter"
	fields := msgReflect.Descriptor().Fields()
	filterField := fields.ByName("filter")
	// Set the filter value
	msgReflect.Set(filterField, protoreflect.ValueOfString(filter))
}

var (
	jsonbFunctionDeclarationOption = filtering.DeclareFunction(
		spanfiltering.FunctionJSONB,
		filtering.NewFunctionOverload(
			spanfiltering.FunctionJSONB+"_string",
			filtering.TypeString,
			filtering.TypeString,
		),

		// Type casted as concrete type.
		filtering.NewFunctionOverload(
			spanfiltering.FunctionJSONB+"_bool",
			filtering.TypeBool,
			filtering.TypeBool,
		),
		filtering.NewFunctionOverload(
			spanfiltering.FunctionJSONB+"_int",
			filtering.TypeInt,
			filtering.TypeInt,
		),
	)
)

func getIsNullFunctionDefaultOverloads() []*v1alpha1.Decl_FunctionDecl_Overload {
	return []*v1alpha1.Decl_FunctionDecl_Overload{
		filtering.NewFunctionOverload(
			spanfiltering.FunctionIsNull+"_string",
			filtering.TypeBool,
			filtering.TypeString,
		),
		filtering.NewFunctionOverload(
			spanfiltering.FunctionIsNull+"_enum",
			filtering.TypeBool,
			filtering.TypeString,
		),
		filtering.NewFunctionOverload(
			spanfiltering.FunctionIsNull+"_bool",
			filtering.TypeBool,
			filtering.TypeBool,
		),
		filtering.NewFunctionOverload(
			spanfiltering.FunctionIsNull+"_int",
			filtering.TypeBool,
			filtering.TypeInt,
		),
		filtering.NewFunctionOverload(
			spanfiltering.FunctionIsNull+"_float",
			filtering.TypeBool,
			filtering.TypeFloat,
		),
	}
}

package aip

import (
	"fmt"
	"slices"
	"strings"

	"buf.build/go/protovalidate"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/spanner-aip/spanfiltering"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/pbutil"
)

// //////////////////////////// INTERFACE //////////////////////////
type filteringRequest interface {
	proto.Message
	filtering.Request
}

// //////////////////////////// PARSER //////////////////////////
type FilteringRequestParser[T filteringRequest, R proto.Message] struct {
	resourceMessage R
	validator       protovalidate.Validator
	declarations    *filtering.Declarations
	// All nested fields ordered in decreasing length.
	nestedFieldsSorted []string
	fieldToReplacement map[string]string
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
		protovalidate.WithMessages(&aippb.FilteringOptions{}),
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

	// Create a zero value of the resource type for validation
	var resourceZero R

	declarationOptions := make([]filtering.DeclarationOption, 0, len(filteringOptions.Filters)+3)
	declarationOptions = append(declarationOptions, filtering.DeclareStandardFunctions(), jsonbFunctionDeclarationOption)

	var nestedFieldsSorted []string
	fieldToReplacement := map[string]string{}
	isNullFunctionOverloads := getIsNullFunctionDefaultOverloads()
	for _, filter := range filteringOptions.Filters {
		if err := pbutil.ValidateMask(resourceZero, filter.Field); err != nil {
			return nil, fmt.Errorf("validating path %s: %v", filter.Field, err)
		}

		fieldName := filter.Field
		if strings.Contains(fieldName, ".") {
			// Capture nested field.
			nestedFieldsSorted = append(nestedFieldsSorted, fieldName)
			// Capture replacement.
			replacement := strings.ReplaceAll(fieldName, ".", "@")
			fieldToReplacement[fieldName] = replacement
			// Replace.
			fieldName = replacement
		}
		switch filter.Type.(type) {
		case *aippb.FilterIdent_WellKnown:
			exprType := &v1alpha1.Type{TypeKind: &v1alpha1.Type_WellKnown{WellKnown: filter.GetWellKnown()}}
			declarationOption := filtering.DeclareIdent(fieldName, exprType)
			declarationOptions = append(declarationOptions, declarationOption)

		case *aippb.FilterIdent_Primitive:
			exprType := &v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: filter.GetPrimitive()}}
			declarationOption := filtering.DeclareIdent(fieldName, exprType)
			declarationOptions = append(declarationOptions, declarationOption)

		case *aippb.FilterIdent_Enum:
			enumType, err := protoregistry.GlobalTypes.FindEnumByName(protoreflect.FullName(filter.GetEnum()))
			if err != nil {
				return nil, fmt.Errorf("finding enum type %s: %w", filter.GetEnum(), err)
			}
			declarationOption := filtering.DeclareEnumIdent(fieldName, enumType)
			declarationOptions = append(declarationOptions, declarationOption)

			// Overload is null function.
			enumIdentType := filtering.TypeEnum(enumType)
			isNullFunctionOverload := filtering.NewFunctionOverload(
				spanfiltering.FunctionIsNull+"_"+enumIdentType.GetMessageType(),
				filtering.TypeBool,
				enumIdentType,
			)
			isNullFunctionOverloads = append(isNullFunctionOverloads, isNullFunctionOverload)
		}
	}

	// Construct the isNull declaration option.
	isNullDeclarationOption := filtering.DeclareFunction(spanfiltering.FunctionIsNull, isNullFunctionOverloads...)
	declarationOptions = append(declarationOptions, isNullDeclarationOption)

	// Build the declarations.
	declarations, err := filtering.NewDeclarations(declarationOptions...)
	if err != nil {
		return nil, fmt.Errorf("creating filter declarations: %w", err)
	}

	// Sorting.
	slices.SortFunc(nestedFieldsSorted, func(a, b string) int { return len(b) - len(a) }) // b - a for descending (longest first) }

	// Return the parser.
	return &FilteringRequestParser[T, R]{
		resourceMessage:    resourceZero,
		validator:          validator,
		declarations:       declarations,
		nestedFieldsSorted: nestedFieldsSorted,
		fieldToReplacement: fieldToReplacement,
	}, nil
}

func (p *FilteringRequestParser[T, R]) Parse(request T) (*FilteringRequest, error) {
	filterClause := request.GetFilter()
	// A nested field is a path in a proto message like "hello.hi.how"
	// We need to replace those with `JSONB(hello@hi@now)`
	// We must be careful to ensure that if there's a `hello.hi` declared and a `hello.hi.now` declared.
	// We don't do `JSONB(hello.hi).now` / this would be problematic.
	// We sort by length to prevent this issue in the instantiation of the parser.
	for _, nestedField := range p.nestedFieldsSorted {
		replacement, ok := p.fieldToReplacement[nestedField]
		if !ok {
			return nil, fmt.Errorf("%s replacement not found", nestedField)
		}
		filterClause = strings.ReplaceAll(filterClause, nestedField, "JSONB("+replacement+")")
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

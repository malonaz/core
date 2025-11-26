package aip

import (
	"fmt"
	"reflect"
	"strings"

	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
	"go.einride.tech/aip/pagination"
	"go.einride.tech/spanner-aip/spanfiltering"
	"go.einride.tech/spanner-aip/spanordering"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/pbutil"
)

// ListRequest defines the interface of an AIP list request.
type ListRequest interface {
	proto.Message
	GetFilter() string
	GetPageSize() int32
	GetPageToken() string
	GetOrderBy() string
}

// GetNextPageToken computes the next page token of this request.
func GetNthPageToken(request ListRequest, n int) (string, error) {
	pageToken, err := pagination.ParsePageToken(request)
	if err != nil {
		return "", fmt.Errorf("parsing page token: %w", err)
	}
	for i := 0; i < n; i++ {
		pageToken = pageToken.Next(request)
	}
	return pageToken.String(), nil
}

// Parser implements aip parsing.
type Parser struct {
	declarations   *filtering.Declarations
	orderByOptions []string
	maxPageSize    int32
	aliases        map[string]string
}

// NewListRequestParser instantiates and returns a new parser.
func NewListRequestParser(request ListRequest) *Parser {
	options := pbutil.MustGetMessageOption(request, aippb.E_List).(*aippb.ListOptions)
	if options == nil {
		panic(fmt.Sprintf("%T must define ListOptions", request))
	}
	if options.MaxPageSize == 0 {
		panic(fmt.Sprintf("must set max page size"))
	}

	isNullFunctionEnumOverloads := []*v1alpha1.Decl_FunctionDecl_Overload{
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

	declarationOptions := make([]filtering.DeclarationOption, 0, len(options.Filters))
	for _, filter := range options.Filters {
		filterField := filter.Field
		if alias, ok := options.Aliases[filter.Field]; ok {
			filterField = alias
		}
		switch filter.Type.(type) {
		case *aippb.FilterIdent_WellKnown:
			exprType := &v1alpha1.Type{TypeKind: &v1alpha1.Type_WellKnown{WellKnown: filter.GetWellKnown()}}
			declarationOption := filtering.DeclareIdent(filterField, exprType)
			declarationOptions = append(declarationOptions, declarationOption)
		case *aippb.FilterIdent_Primitive:
			exprType := &v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: filter.GetPrimitive()}}
			declarationOption := filtering.DeclareIdent(filterField, exprType)
			declarationOptions = append(declarationOptions, declarationOption)
		case *aippb.FilterIdent_Enum:
			enumType, err := protoregistry.GlobalTypes.FindEnumByName(protoreflect.FullName(filter.GetEnum()))
			if err != nil {
				panic(fmt.Sprintf("could not find enum type %s", filter.GetEnum()))
			}
			declarationOption := filtering.DeclareEnumIdent(filterField, enumType)
			declarationOptions = append(declarationOptions, declarationOption)
			enumIdentType := filtering.TypeEnum(enumType)
			isNullFunctionEnumOverloads = append(isNullFunctionEnumOverloads,
				filtering.NewFunctionOverload(
					spanfiltering.FunctionIsNull+"_"+enumIdentType.GetMessageType(),
					filtering.TypeBool,
					enumIdentType,
				),
			)
		}
	}
	declarationOptions = append(
		declarationOptions,
		filtering.DeclareStandardFunctions(),
		jsonbFunctionDeclarationOption,
		filtering.DeclareFunction(
			spanfiltering.FunctionIsNull,
			append(isNullFunctionDefaultOverloads, isNullFunctionEnumOverloads...)...,
		),
	)
	declarations, err := filtering.NewDeclarations(declarationOptions...)
	if err != nil {
		panic(fmt.Sprintf("invalid declaration options: %v", err))
	}

	orderByOptions := make([]string, 0, len(options.OrderBy))
	for _, field := range options.OrderBy {
		if alias, ok := options.Aliases[field]; ok {
			orderByOptions = append(orderByOptions, alias)
		} else {
			orderByOptions = append(orderByOptions, field)
		}
	}
	aliases := map[string]string{}
	for k, v := range options.Aliases {
		aliases[k] = v
	}
	return &Parser{
		orderByOptions: orderByOptions,
		declarations:   declarations,
		maxPageSize:    options.MaxPageSize,
		aliases:        aliases,
	}
}

// An extra declaration option.
var (
	isNullFunctionDefaultOverloads = []*v1alpha1.Decl_FunctionDecl_Overload{
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

	jsonbFunctionDeclarationOption = filtering.DeclareFunction(
		spanfiltering.FunctionJSONB,
		filtering.NewFunctionOverload(
			spanfiltering.FunctionJSONB+"_string",
			filtering.TypeString,
			filtering.TypeString,
		),
	)
)

// ParsedRequest is a request that is parsed.
type ParsedRequest interface {
	// Returns an SQL limit/offset clause. The limit is 0 if the request's page size is 0, or pageSize + 1 otherwise. Offset is the page token's offset if it exists.
	GetSQLPaginationClause() string
	// Returns "" if the request's page size is 0 or if we found `GetLimit` objects, indicating there is no more pages.
	// Otherwise returns the next page token.
	GetNextPageToken(itemsFetchedWithLimit int) string
	// Returns an SQL where clause + any params.
	GetSQLWhereClause() (string, []any)
	// Returns an SQL where clause.
	GetSQLOrderByClause() string
}

type parsedRequest struct {
	request     ListRequest
	pageToken   pagination.PageToken
	orderBy     ordering.OrderBy
	whereClause string
	whereParams []any
}

// GetSQLLimitClause implements the ParsedRequest interface.
func (pr *parsedRequest) GetSQLPaginationClause() string {
	if pr.request.GetPageSize() == 0 {
		return ""
	}
	return fmt.Sprintf("OFFSET %d LIMIT %d", pr.pageToken.Offset, pr.request.GetPageSize()+1)
}

// GetNextPageToken implements the ParsedRequest interface.
func (pr *parsedRequest) GetNextPageToken(itemsFetched int) string {
	if pr.request.GetPageSize() == 0 || itemsFetched <= int(pr.request.GetPageSize()) {
		return ""
	}
	return pr.pageToken.Next(pr.request).String()
}

// GetSQLOrderByClause implements the ParsedRequest interface.
func (pr *parsedRequest) GetSQLOrderByClause() string {
	return spanordering.TranspileOrderBy(pr.orderBy)
}

// GetSQLWhereClause implements the ParsedRequest interface.
func (pr *parsedRequest) GetSQLWhereClause() (string, []any) {
	return pr.whereClause, pr.whereParams
}

// ParseRequest parses the given request. Any error should be returned as a InvalidArgument error.
func (p *Parser) ParseRequest(request ListRequest, macros ...filtering.Macro) (ParsedRequest, error) {
	// Apply replacements to the request.
	filterExpression := request.GetFilter()
	for k, v := range p.aliases {
		filterExpression = strings.ReplaceAll(filterExpression, k, v)
	}
	if err := setStringField(request, "Filter", filterExpression); err != nil {
		panic(err)
	}

	orderByExpression := request.GetOrderBy()
	for k, v := range p.aliases {
		orderByExpression = strings.ReplaceAll(orderByExpression, k, v)
	}
	if err := setStringField(request, "OrderBy", orderByExpression); err != nil {
		panic(err)
	}

	// Apply max page size if required.
	if request.GetPageSize() == 0 {
		if err := setPageSize(request, p.maxPageSize); err != nil {
			panic(err)
		}
	}
	if request.GetPageSize() > p.maxPageSize {
		return nil, fmt.Errorf("page_size (%d) exceeds max page_size (%d)", request.GetPageSize(), p.maxPageSize)
	}

	// Parse page token.
	pageToken, err := pagination.ParsePageToken(request)
	if err != nil {
		return nil, fmt.Errorf("parsing page token: %w", err)
	}

	// Parse Order by.
	orderBy, err := ordering.ParseOrderBy(request)
	if err != nil {
		return nil, fmt.Errorf("parsing order by: %w", err)
	}
	if err := orderBy.ValidateForPaths(p.orderByOptions...); err != nil {
		return nil, fmt.Errorf("validating order by paths: %w", err)
	}

	// Parse filtering.
	filter, err := filtering.ParseFilter(request, p.declarations)
	if err != nil {
		return nil, fmt.Errorf("parsing filter: %w", err)
	}

	if len(macros) > 0 && request.GetFilter() != "" {
		filter, err = filtering.ApplyMacros(filter, p.declarations, macros...)
		if err != nil {
			return nil, fmt.Errorf("applying macros to filter: %w", err)
		}
	}

	whereClause, whereParams, err := spanfiltering.TranspileFilter(filter)
	if err != nil {
		return nil, fmt.Errorf("transpiling filter to SQL: %w", err)
	}

	return &parsedRequest{
		request:     request,
		pageToken:   pageToken,
		orderBy:     orderBy,
		whereClause: whereClause,
		whereParams: whereParams,
	}, nil
}

func setStringField(msg proto.Message, field, newValue string) error {
	// Reflect on the message to obtain the value
	v := reflect.ValueOf(msg)

	// Check if the passed interface is a pointer
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("msg must be a pointer")
	}

	// Dereference the pointer to get the value
	v = v.Elem()

	// Find and set the Filter field
	structField := v.FieldByName(field)
	fieldType := structField.Kind()
	if fieldType != reflect.String {
		return fmt.Errorf("%s is not a string field", field)
	}
	structField.SetString(newValue)
	return nil
}

func setPageSize(msg proto.Message, pageSize int32) error {
	// Reflect on the message to obtain the value
	v := reflect.ValueOf(msg)

	// Check if the passed interface is a pointer
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("msg must be a pointer")
	}

	// Dereference the pointer to get the value
	v = v.Elem()

	// Find and set the Filter field
	structField := v.FieldByName("PageSize")
	fieldType := structField.Kind()
	if fieldType != reflect.Int32 {
		return fmt.Errorf("Filter is not an integer field")
	}
	structField.SetInt(int64(pageSize))
	return nil
}

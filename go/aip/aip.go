package aip

import (
	"fmt"

	"github.com/pkg/errors"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
	"go.einride.tech/aip/pagination"
	"go.einride.tech/spanner-aip/spanfiltering"
	"go.einride.tech/spanner-aip/spanordering"
	"google.golang.org/protobuf/proto"

	"go/logging"
)

var log = logging.NewLogger()

// Request defines the interface of a ListResourceRequest.
type Request interface {
	proto.Message
	GetFilter() string
	GetPageSize() int32
	GetPageToken() string
	GetOrderBy() string
}

// Parser implements aip parsing.
type Parser struct {
	declarations   *filtering.Declarations
	orderByOptions []string
}

// NewParser instantiates and returns a new parser.
func NewParser() *Parser {
	return &Parser{}
}

// An extra declaration option.
var nullFunctionDeclarationOption = filtering.DeclareFunction(
	"ISNULL",
	filtering.NewFunctionOverload("ISNULL"+"_string", filtering.TypeBool, filtering.TypeString),
)

// WithFilteringOptions sets filtering options. This method panics on error as this method should be declared as a topline variable.
func (p *Parser) WithFilteringOptions(declarationOptions ...filtering.DeclarationOption) *Parser {
	declarationOptions = append(declarationOptions, filtering.DeclareStandardFunctions(), nullFunctionDeclarationOption)
	declarations, err := filtering.NewDeclarations(declarationOptions...)
	if err != nil {
		log.Panicf("invalid declaration options: %v", err)
	}
	p.declarations = declarations
	return p
}

// WithOrderByOptions sets order by options.
func (p *Parser) WithOrderByOptions(orderByOptions ...string) *Parser {
	p.orderByOptions = orderByOptions
	return p
}

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
	request     Request
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
func (p *Parser) ParseRequest(request Request, macros ...filtering.Macro) (ParsedRequest, error) {
	// Parse page token.
	pageToken, err := pagination.ParsePageToken(request)
	if err != nil {
		return nil, errors.Wrap(err, "parsing page token")
	}

	// Parse Order by.
	orderBy, err := ordering.ParseOrderBy(request)
	if err != nil {
		return nil, errors.Wrap(err, "parsing order by")
	}
	if err := orderBy.ValidateForPaths(p.orderByOptions...); err != nil {
		return nil, errors.Wrap(err, "validating order by paths")
	}

	// Parse filtering.
	filter, err := filtering.ParseFilter(request, p.declarations)
	if err != nil {
		return nil, errors.Wrap(err, "parsing filter")
	}
	if len(macros) > 0 && request.GetFilter() != "" {
		filter, err = filtering.ApplyMacros(filter, p.declarations, macros...)
		if err != nil {
			return nil, errors.Wrap(err, "applying macros to filter")
		}
	}

	whereClause, whereParams, err := spanfiltering.TranspileFilter(filter)
	if err != nil {
		return nil, errors.Wrap(err, "transpiling filter to SQL")
	}

	return &parsedRequest{
		request:     request,
		pageToken:   pageToken,
		orderBy:     orderBy,
		whereClause: whereClause,
		whereParams: whereParams,
	}, nil
}

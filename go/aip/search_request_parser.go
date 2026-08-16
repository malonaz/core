package aip

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// searchRequest defines the interface of an AIP search request.
// The R type parameter represents the Resource type being searched.
type searchRequest[R proto.Message] interface {
	filteringRequest
	paginationRequest
	GetQuery() string
}

// SearchRequestParser implements aip parsing using composition of specialized parsers.
type SearchRequestParser[T searchRequest[R], R proto.Message] struct {
	filteringParser  *FilteringRequestParser[T, R]
	paginationParser *PaginationRequestParser[T]
}

func MustNewSearchRequestParser[T searchRequest[R], R proto.Message]() *SearchRequestParser[T, R] {
	parser, err := NewSearchRequestParser[T, R]()
	if err != nil {
		panic(err)
	}
	return parser
}

// NewSearchRequestParser instantiates and returns a new parser.
func NewSearchRequestParser[T searchRequest[R], R proto.Message]() (*SearchRequestParser[T, R], error) {
	filteringParser, err := NewFilteringRequestParser[T, R]()
	if err != nil {
		return nil, fmt.Errorf("creating filtering parser: %w", err)
	}

	paginationParser, err := NewPaginationRequestParser[T]()
	if err != nil {
		return nil, fmt.Errorf("creating pagination parser: %w", err)
	}

	return &SearchRequestParser[T, R]{
		filteringParser:  filteringParser,
		paginationParser: paginationParser,
	}, nil
}

// ParsedSearchRequest embeds all the specialized parsed request types.
type ParsedSearchRequest struct {
	*FilteringRequest
	*PaginatedRequest
	query string
}

// GetQuery returns the raw search query.
func (p *ParsedSearchRequest) GetQuery() string {
	return p.query
}

// GetTSQuery returns the postgres tsquery expression derived from the raw
// query, with prefix matching on every token. Returns "" when the query
// holds no indexable token.
func (p *ParsedSearchRequest) GetTSQuery() string {
	return BuildPrefixTSQuery(p.query)
}

// Parse parses the given request. Any error should be returned as an InvalidArgument error.
func (p *SearchRequestParser[T, R]) Parse(request T) (*ParsedSearchRequest, error) {
	// A query must hold at least one indexable token.
	if BuildPrefixTSQuery(request.GetQuery()) == "" {
		return nil, fmt.Errorf("query must contain at least one searchable term")
	}

	// Parse filtering
	filteringRequest, err := p.filteringParser.Parse(request)
	if err != nil {
		return nil, fmt.Errorf("parsing filtering: %w", err)
	}

	// Parse pagination
	paginatedRequest, err := p.paginationParser.Parse(request)
	if err != nil {
		return nil, fmt.Errorf("parsing pagination: %w", err)
	}

	return &ParsedSearchRequest{
		FilteringRequest: filteringRequest,
		PaginatedRequest: paginatedRequest,
		query:            request.GetQuery(),
	}, nil
}

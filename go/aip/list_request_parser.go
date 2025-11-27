package aip

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// listRequest defines the interface of an AIP list request.
// The R type parameter represents the Resource type being listed.
type listRequest[R proto.Message] interface {
	filteringRequest
	paginationRequest
	orderingRequest
}

// ListRequestParser implements aip parsing using composition of specialized parsers.
type ListRequestParser[T listRequest[R], R proto.Message] struct {
	filteringParser  *FilteringRequestParser[T, R]
	paginationParser *PaginationRequestParser[T]
	orderingParser   *OrderingRequestParser[T, R]
}

func MustNewListRequestParser[T listRequest[R], R proto.Message]() *ListRequestParser[T, R] {
	parser, err := NewListRequestParser[T, R]()
	if err != nil {
		panic(err)
	}
	return parser
}

// NewListRequestParser instantiates and returns a new parser.
func NewListRequestParser[T listRequest[R], R proto.Message]() (*ListRequestParser[T, R], error) {
	filteringParser, err := NewFilteringRequestParser[T, R]()
	if err != nil {
		return nil, fmt.Errorf("creating filtering parser: %w", err)
	}

	paginationParser, err := NewPaginationRequestParser[T]()
	if err != nil {
		return nil, fmt.Errorf("creating pagination parser: %w", err)
	}

	orderingParser, err := NewOrderingRequestParser[T, R]()
	if err != nil {
		return nil, fmt.Errorf("creating ordering parser: %w", err)
	}

	return &ListRequestParser[T, R]{
		filteringParser:  filteringParser,
		paginationParser: paginationParser,
		orderingParser:   orderingParser,
	}, nil
}

// ParsedListRequest embeds all the specialized parsed request types.
type ParsedListRequest struct {
	*FilteringRequest
	*PaginatedRequest
	*OrderingRequest
}

// Parse parses the given request. Any error should be returned as an InvalidArgument error.
func (p *ListRequestParser[T, R]) Parse(request T) (*ParsedListRequest, error) {
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

	// Parse ordering
	orderingRequest, err := p.orderingParser.Parse(request)
	if err != nil {
		return nil, fmt.Errorf("parsing ordering: %w", err)
	}

	return &ParsedListRequest{
		FilteringRequest: filteringRequest,
		PaginatedRequest: paginatedRequest,
		OrderingRequest:  orderingRequest,
	}, nil
}

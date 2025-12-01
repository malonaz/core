package aip

import (
	"fmt"

	"buf.build/go/protovalidate"
	"go.einride.tech/aip/pagination"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/pbutil"
)

// ////////////////////////////// INTERFACE //////////////////////////
type paginationRequest interface {
	proto.Message
	pagination.Request
}

// ////////////////////////////// PARSER //////////////////////////
type PaginationRequestParser[T paginationRequest] struct {
	validator protovalidate.Validator
	options   *aippb.PaginationOptions
}

func MustNewPaginationRequestParser[T paginationRequest]() *PaginationRequestParser[T] {
	parser, err := NewPaginationRequestParser[T]()
	if err != nil {
		panic(err)
	}
	return parser
}

func NewPaginationRequestParser[T paginationRequest]() (*PaginationRequestParser[T], error) {
	validator, err := protovalidate.New(
		protovalidate.WithDisableLazy(),
		protovalidate.WithMessages(&aippb.PaginationOptions{}),
	)
	if err != nil {
		return nil, fmt.Errorf("instantiated validator for pagination request parser: %v", err)
	}

	// Parse options from the generic type T
	var zero T
	options, err := pbutil.GetMessageOption[*aippb.PaginationOptions](zero, aippb.E_Pagination)
	if err != nil {
		return nil, fmt.Errorf("getting message options: %v", err)
	}
	// Validate options
	if err := validator.Validate(options); err != nil {
		return nil, fmt.Errorf("validating options: %v", err)
	}

	return &PaginationRequestParser[T]{
		validator: validator,
		options:   options,
	}, nil
}

func (p *PaginationRequestParser[T]) Parse(request T) (*PaginatedRequest, error) {
	if request.GetPageSize() == 0 {
		p.setPageSize(request, p.options.DefaultPageSize)
	}

	// Parse page token.
	pageToken, err := pagination.ParsePageToken(request)
	if err != nil {
		return nil, fmt.Errorf("parsing page token: %w", err)
	}
	return &PaginatedRequest{
		request:   request,
		pageToken: pageToken,
	}, nil
}

// ////////////////////////////// PARSED REQUEST //////////////////////////
type PaginatedRequest struct {
	request   paginationRequest
	pageToken pagination.PageToken
}

func (r *PaginatedRequest) GetNextPageToken(itemsFetched int) string {
	if r.request.GetPageSize() == 0 || itemsFetched <= int(r.request.GetPageSize()) {
		return ""
	}
	return r.pageToken.Next(r.request).String()
}

func (r *PaginatedRequest) GetSQLPaginationClause() string {
	return fmt.Sprintf("OFFSET %d LIMIT %d", r.pageToken.Offset, r.request.GetPageSize()+1)
}

func (r *PaginatedRequest) GetOffset() int64 {
	return r.pageToken.Offset
}

// ///////////////////////////// UTILS //////////////////////////////
func (p *PaginationRequestParser[T]) setPageSize(request paginationRequest, pageSize uint32) {
	// Get the protobuf message descriptor
	msgReflect := request.ProtoReflect()
	// Get the field descriptor for "page_size"
	fields := msgReflect.Descriptor().Fields()
	pageSizeField := fields.ByName("page_size")
	// Set the page_size value
	msgReflect.Set(pageSizeField, protoreflect.ValueOfInt32(int32(pageSize)))
}

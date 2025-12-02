package aip

import (
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	"go.einride.tech/aip/ordering"
	"go.einride.tech/spanner-aip/spanordering"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/pbutil"
)

// ////////////////////////////// INTERFACE //////////////////////////
type orderingRequest interface {
	proto.Message
	ordering.Request
}

// ////////////////////////////// PARSER //////////////////////////
type OrderingRequestParser[T orderingRequest, R proto.Message] struct {
	validator   protovalidate.Validator
	options     *aippb.OrderingOptions
	tree        *Tree
	pathToAllow map[string]bool
}

func MustNewOrderingRequestParser[T orderingRequest, R proto.Message]() *OrderingRequestParser[T, R] {
	parser, err := NewOrderingRequestParser[T, R]()
	if err != nil {
		panic(err)
	}
	return parser
}

func NewOrderingRequestParser[T orderingRequest, R proto.Message]() (*OrderingRequestParser[T, R], error) {
	validator, err := protovalidate.New(
		protovalidate.WithDisableLazy(),
		protovalidate.WithMessages(&aippb.OrderingOptions{}),
	)
	if err != nil {
		return nil, fmt.Errorf("instantiated validator for ordering request parser: %v", err)
	}

	// Parse options from the generic type T
	var zero T
	options, err := pbutil.GetMessageOption[*aippb.OrderingOptions](zero, aippb.E_Ordering)
	if err != nil {
		return nil, fmt.Errorf("getting message options: %v", err)
	}
	// Validate options
	if err := validator.Validate(options); err != nil {
		return nil, fmt.Errorf("validating options: %v", err)
	}

	var zeroResource R
	paths := strings.Join(options.GetPaths(), ",")
	if err := pbutil.ValidateMask(zeroResource, paths); err != nil {
		return nil, fmt.Errorf("validating filtering paths: %w", err)
	}

	// Create a tree and explore.
	tree, err := BuildResourceTree[R](10, options.GetPaths())
	if err != nil {
		return nil, err
	}

	pathToAllow := map[string]bool{}
	for _, node := range tree.Nodes {
		pathToAllow[node.Path] = node.AllowedPath
	}

	return &OrderingRequestParser[T, R]{
		validator:   validator,
		options:     options,
		tree:        tree,
		pathToAllow: pathToAllow,
	}, nil
}

func (p *OrderingRequestParser[T, R]) Parse(request T) (*OrderingRequest, error) {
	// Set default order_by if not specified
	if request.GetOrderBy() == "" {
		p.setOrderBy(request, p.options.Default)
	}
	orderByClause := request.GetOrderBy()

	// First pass to validate.
	{
		orderBy, err := ordering.ParseOrderBy(request)
		if err != nil {
			return nil, fmt.Errorf("parsing order by: %w", err)
		}

		for _, field := range orderBy.Fields {
			allow, ok := p.pathToAllow[field.Path]
			if !ok {
				return nil, fmt.Errorf("invalid order path %s", field.Path)
			}
			if !allow {
				return nil, fmt.Errorf("ordering by path %s not allowed", field.Path)
			}
		}
	}

	// Apply the replacement.
	for node := range p.tree.AllowedNodes() {
		orderByClause = node.ApplyReplacement(orderByClause)
	}
	orderByClause = strings.ReplaceAll(orderByClause, "@", ".")
	p.setOrderBy(request, orderByClause)

	// Second pass to transpile.
	orderBy, err := ordering.ParseOrderBy(request)
	if err != nil {
		return nil, fmt.Errorf("parsing order by: %w", err)
	}

	return &OrderingRequest{
		request: request,
		orderBy: orderBy,
	}, nil
}

// ////////////////////////////// PARSED REQUEST //////////////////////////
type OrderingRequest struct {
	request orderingRequest
	orderBy ordering.OrderBy
}

func (p *OrderingRequest) GetSQLOrderByClause() string {
	return spanordering.TranspileOrderBy(p.orderBy)
}

// ///////////////////////////// UTILS //////////////////////////////
func (p *OrderingRequestParser[T, R]) setOrderBy(request orderingRequest, orderBy string) {
	// Get the protobuf message descriptor
	msgReflect := request.ProtoReflect()
	// Get the field descriptor for "order_by"
	fields := msgReflect.Descriptor().Fields()
	orderByField := fields.ByName("order_by")
	// Set the order_by value
	msgReflect.Set(orderByField, protoreflect.ValueOfString(orderBy))
}

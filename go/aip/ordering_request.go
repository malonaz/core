package aip

import (
	"fmt"
	"strings"

	"go.einride.tech/aip/ordering"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/aip/transpiler/postgres"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

// ////////////////////////////// INTERFACE //////////////////////////
type orderingRequest interface {
	proto.Message
	ordering.Request
}

// ////////////////////////////// PARSER //////////////////////////
type OrderingRequestParser[T orderingRequest, R proto.Message] struct {
	options     *aippb.OrderingOptions
	tree        *Tree
	pathToAllow map[string]bool
	pathToNode  map[string]*Node
}

func MustNewOrderingRequestParser[T orderingRequest, R proto.Message]() *OrderingRequestParser[T, R] {
	parser, err := NewOrderingRequestParser[T, R]()
	if err != nil {
		panic(err)
	}
	return parser
}

func NewOrderingRequestParser[T orderingRequest, R proto.Message]() (*OrderingRequestParser[T, R], error) {
	var zero T
	options, err := pbutil.GetMessageOption[*aippb.OrderingOptions](zero, aippb.E_Ordering)
	if err != nil {
		return nil, fmt.Errorf("getting message options: %v", err)
	}

	var zeroResource R
	if err := pbfieldmask.FromPaths(options.GetPaths()...).Validate(zeroResource); err != nil {
		return nil, fmt.Errorf("validating paths: %w", err)
	}

	tree, err := BuildResourceTree[R](WithAllowedPaths(options.GetPaths()))
	if err != nil {
		return nil, err
	}

	pathToAllow := map[string]bool{}
	pathToNode := map[string]*Node{}
	for _, node := range tree.Nodes {
		pathToAllow[node.Path] = node.AllowedPath
		pathToNode[node.Path] = node
	}

	parser := &OrderingRequestParser[T, R]{
		options:     options,
		tree:        tree,
		pathToAllow: pathToAllow,
		pathToNode:  pathToNode,
	}

	request := zero.ProtoReflect().New().Interface().(T)
	if _, err := parser.Parse(request); err != nil {
		return nil, fmt.Errorf("invalid default %q: %w", options.GetDefault(), err)
	}
	return parser, nil
}

func (p *OrderingRequestParser[T, R]) Parse(request T) (*OrderingRequest, error) {
	if request.GetOrderBy() == "" {
		p.setOrderBy(request, p.options.Default)
	}

	// Expand "name" to composite key fields before parsing.
	p.setOrderBy(request, p.expandNameField(request.GetOrderBy()))

	orderBy, err := ordering.ParseOrderBy(request)
	if err != nil {
		return nil, fmt.Errorf("parsing order by: %w", err)
	}

	// Validate and resolve each field to its fully qualified column name.
	for i, field := range orderBy.Fields {
		allow, ok := p.pathToAllow[field.Path]
		if !ok {
			return nil, fmt.Errorf("invalid order path %s", field.Path)
		}
		if !allow {
			return nil, fmt.Errorf("ordering by path %s not allowed", field.Path)
		}
		orderBy.Fields[i].Path = p.resolveFieldPath(field.Path)
	}

	return &OrderingRequest{
		request: request,
		orderBy: orderBy,
	}, nil
}

// resolveFieldPath converts a proto field path to a fully qualified table.column reference.
func (p *OrderingRequestParser[T, R]) resolveFieldPath(path string) string {
	node, ok := p.pathToNode[path]
	if !ok {
		return path
	}

	columnName := path
	if node.ReplacementPath != "" {
		columnName = node.ReplacementPath
	}

	if node.TableName != "" {
		return node.TableName + "." + columnName
	}
	return columnName
}

// expandNameField expands occurrences of "name" in the order by clause to the composite key fields.
func (p *OrderingRequestParser[T, R]) expandNameField(orderByClause string) string {
	if p.tree.Resource == nil {
		return orderByClause
	}

	if !p.pathToAllow["name"] {
		return orderByClause
	}

	parts := strings.Split(orderByClause, ",")
	var expandedParts []string

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		tokens := strings.Fields(part)
		if len(tokens) == 0 {
			continue
		}

		fieldPath := tokens[0]
		direction := ""
		if len(tokens) > 1 {
			direction = " " + tokens[1]
		}

		if fieldPath == "name" {
			for _, compositeField := range p.getCompositeKeyFields() {
				expandedParts = append(expandedParts, compositeField+direction)
			}
		} else {
			expandedParts = append(expandedParts, part)
		}
	}

	return strings.Join(expandedParts, ", ")
}

// getCompositeKeyFields returns the list of ID column names for the resource's composite key.
func (p *OrderingRequestParser[T, R]) getCompositeKeyFields() []string {
	if p.tree.Resource == nil {
		return nil
	}

	var fields []string
	patternVars := p.tree.Resource.PatternVariables
	singular := p.tree.Resource.Singular

	for _, variable := range patternVars {
		var columnName string
		if variable == singular && p.tree.IDColumnName != "" {
			columnName = p.tree.IDColumnName
		} else {
			columnName = variable + "_id"
		}
		fields = append(fields, columnName)
	}

	return fields
}

// ////////////////////////////// PARSED REQUEST //////////////////////////
type OrderingRequest struct {
	request orderingRequest
	orderBy ordering.OrderBy
}

func (p *OrderingRequest) GetSQLOrderByClause() string {
	return postgres.TranspileOrderBy(p.orderBy)
}

func (p *OrderingRequest) GetOrderBy() ordering.OrderBy {
	return p.orderBy
}

// ///////////////////////////// UTILS //////////////////////////////
func (p *OrderingRequestParser[T, R]) setOrderBy(request orderingRequest, orderBy string) {
	msgReflect := request.ProtoReflect()
	fields := msgReflect.Descriptor().Fields()
	orderByField := fields.ByName("order_by")
	msgReflect.Set(orderByField, protoreflect.ValueOfString(orderBy))
}

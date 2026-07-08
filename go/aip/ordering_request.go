package aip

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
	"go.einride.tech/aip/ordering"
	"google.golang.org/protobuf/proto"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/aip/transpiler/postgres"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

// ////////////////////////////// INTERFACE //////////////////////////
type orderingRequest interface {
	proto.Message
	ordering.Request
	SetOrderBy(string)
}

// ////////////////////////////// OPTS //////////////////////////
type OrderingRequestOpt func(*orderingRequestOpts)

type orderingRequestOpts struct {
	withFQN bool
}

// WithOrderingFQN prepends table names to column references in ORDER BY SQL.
func WithOrderingFQN() OrderingRequestOpt {
	return func(o *orderingRequestOpts) {
		o.withFQN = true
	}
}

// ////////////////////////////// PARSER //////////////////////////
type OrderingRequestParser[T orderingRequest, R proto.Message] struct {
	options               *aippb.OrderingOptions
	tree                  *Tree
	pathToAllow           map[string]bool
	pathToNode            map[string]*Node
	withFQN               bool
	compositeKeyColumnSet map[string]struct{}
	tableName             string
}

func MustNewOrderingRequestParser[T orderingRequest, R proto.Message](opts ...OrderingRequestOpt) *OrderingRequestParser[T, R] {
	parser, err := NewOrderingRequestParser[T, R](opts...)
	if err != nil {
		panic(err)
	}
	return parser
}

func NewOrderingRequestParser[T orderingRequest, R proto.Message](opts ...OrderingRequestOpt) (*OrderingRequestParser[T, R], error) {
	var options orderingRequestOpts
	for _, opt := range opts {
		opt(&options)
	}

	var zero T
	orderingOptions, err := pbutil.GetMessageOption[*aippb.OrderingOptions](zero, aippb.E_Ordering)
	if err != nil {
		return nil, fmt.Errorf("getting message options: %v", err)
	}

	var zeroResource R
	if err := pbfieldmask.FromPaths(orderingOptions.GetPaths()...).Validate(zeroResource); err != nil {
		return nil, fmt.Errorf("validating paths: %w", err)
	}

	tree, err := BuildResourceTree[R](WithAllowedPaths(orderingOptions.GetPaths()))
	if err != nil {
		return nil, err
	}

	pathToAllow := map[string]bool{}
	pathToNode := map[string]*Node{}
	for _, node := range tree.Nodes {
		pathToAllow[node.Path] = node.AllowedPath
		pathToNode[node.Path] = node
	}

	// Resolve the resource's table name from any non-joined node.
	var tableName string
	for _, node := range tree.Nodes {
		if node.JoinTableName == "" {
			tableName = node.TableName
			break
		}
	}

	parser := &OrderingRequestParser[T, R]{
		options:     orderingOptions,
		tree:        tree,
		pathToAllow: pathToAllow,
		pathToNode:  pathToNode,
		withFQN:     options.withFQN,
		tableName:   tableName,
	}

	// Columns produced by expanding "name" are DB columns, not proto paths,
	// so they must bypass proto-path validation in Parse.
	parser.compositeKeyColumnSet = map[string]struct{}{}
	for _, columnName := range parser.getCompositeKeyFields() {
		parser.compositeKeyColumnSet[columnName] = struct{}{}
	}

	request := zero.ProtoReflect().New().Interface().(T)
	if _, err := parser.Parse(request); err != nil {
		return nil, fmt.Errorf("invalid default %q: %w", orderingOptions.GetDefault(), err)
	}
	return parser, nil
}

func (p *OrderingRequestParser[T, R]) Parse(request T) (*OrderingRequest, error) {
	if request.GetOrderBy() == "" {
		request.SetOrderBy(p.options.Default)
	}

	// Expand "name" to composite key fields before parsing.
	request.SetOrderBy(p.expandNameField(request.GetOrderBy()))

	orderBy, err := ordering.ParseOrderBy(request)
	if err != nil {
		return nil, fmt.Errorf("parsing order by: %w", err)
	}

	// Validate and resolve each field to its column name (optionally fully qualified).
	for i, field := range orderBy.Fields {
		// Composite key columns come from "name" expansion and are implicitly allowed.
		if _, ok := p.compositeKeyColumnSet[field.Path]; ok {
			if p.withFQN && p.tableName != "" {
				orderBy.Fields[i].Path = p.tableName + "." + field.Path
			}
			continue
		}
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

// resolveFieldPath converts a proto field path to a column reference,
// optionally qualified with the table name when WithOrderingFQN is set.
func (p *OrderingRequestParser[T, R]) resolveFieldPath(path string) string {
	node, ok := p.pathToNode[path]
	if !ok {
		return path
	}

	columnName := path
	if node.ReplacementPath != "" {
		columnName = node.ReplacementPath
	}

	if p.withFQN && node.TableName != "" {
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

// getCompositeKeyFields returns the ID column names for the resource's composite key.
// This mirrors the codegen in protoc-gen-core/plugin/model: parent pattern variables
// map to "<variable>_id"; the resource's own ID column (last variable on non-singleton
// resources) is derived from the singular (e.g. {revision} on "quoteRevision" ->
// "quote_revision_id"), with the model's id_column_name override taking precedence.
func (p *OrderingRequestParser[T, R]) getCompositeKeyFields() []string {
	if p.tree.Resource == nil {
		return nil
	}

	patternVariables := p.tree.Resource.PatternVariables
	fields := make([]string, 0, len(patternVariables))
	for i, variable := range patternVariables {
		columnName := variable + "_id"
		isLast := i == len(patternVariables)-1
		if isLast && !p.tree.Resource.Singleton {
			columnName = xstrings.ToSnakeCase(p.tree.Resource.Singular) + "_id"
			if p.tree.IDColumnName != "" {
				columnName = p.tree.IDColumnName
			}
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

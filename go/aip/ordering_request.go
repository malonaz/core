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
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
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
	if err := pbfieldmask.FromPaths(options.GetPaths()...).Validate(zeroResource); err != nil {
		return nil, fmt.Errorf("validating paths: %w", err)
	}

	// Create a tree and explore.
	tree, err := BuildResourceTree[R](WithAllowedPaths(options.GetPaths()))
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

	// Expand "name" field to composite key fields before other replacements
	orderByClause = p.expandNameField(orderByClause)

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

// expandNameField expands occurrences of "name" in the order by clause to the composite key fields.
// For example, "name desc" becomes "organization_id desc, user_id desc, chat_id desc"
// For singleton resources (where the resource itself has no ID), it expands to parent fields only.
func (p *OrderingRequestParser[T, R]) expandNameField(orderByClause string) string {
	if p.tree.Resource == nil {
		return orderByClause
	}

	// Check if "name" is an allowed path - if not, don't expand (let validation fail normally)
	if !p.pathToAllow["name"] {
		return orderByClause
	}

	// Parse the order by clause to find "name" fields
	parts := strings.Split(orderByClause, ",")
	var expandedParts []string

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Parse field and direction
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
			// Expand to composite key fields
			compositeFields := p.getCompositeKeyFields()
			for _, cf := range compositeFields {
				expandedParts = append(expandedParts, cf+direction)
			}
		} else {
			expandedParts = append(expandedParts, part)
		}
	}

	return strings.Join(expandedParts, ", ")
}

// getCompositeKeyFields returns the list of ID column names for the resource's composite key.
// For a resource with pattern "organizations/{organization}/users/{user}/chats/{chat}",
// this returns ["organization_id", "user_id", "chat_id"].
// For singleton resources, the resource's own ID is excluded.
// If the resource has a custom id_column_name in model_opts, that is used for the resource's own ID.
func (p *OrderingRequestParser[T, R]) getCompositeKeyFields() []string {
	if p.tree.Resource == nil {
		return nil
	}

	var fields []string
	patternVars := p.tree.Resource.PatternVariables
	singular := p.tree.Resource.Singular

	for _, variable := range patternVars {
		var columnName string
		// Check if this is the resource's own ID (matches singular name)
		if variable == singular && p.tree.IDColumnName != "" {
			// Use custom id_column_name from model_opts
			columnName = p.tree.IDColumnName
		} else {
			// Default: convert pattern variable to column name (e.g., "organization" -> "organization_id")
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

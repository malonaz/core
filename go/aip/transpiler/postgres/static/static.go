package static

import (
	"fmt"
	"strings"

	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/aip/transpiler/postgres"
)

// Transpiler transpiles AIP-160 filters and AIP-132 order_by clauses
// against a resource message descriptor into static SQL clauses — constants
// inlined as literals — for callers baking SQL where no runtime parameters
// exist, such as code generators.
//
// Only the resource's own top-level stored scalar fields are addressable:
// joined, byte-serialized, repeated and nested fields are excluded, since a
// static clause runs against the resource's bare table.
type Transpiler struct {
	declarations      *filtering.Declarations
	macroDeclarations *filtering.Declarations
	macros            []filtering.Macro
	enumByType        map[protoreflect.FullName]protoreflect.EnumDescriptor
	pathToFQN         map[string]string
}

// TranspilerOption customizes a Transpiler.
type TranspilerOption func(*transpilerOptions)

type transpilerOptions struct {
	registry *protoregistry.Files
}

// WithRegistry provides the file registry resolving resource types, for
// callers whose descriptors are not linked into the binary.
func WithRegistry(registry *protoregistry.Files) TranspilerOption {
	return func(o *transpilerOptions) { o.registry = registry }
}

// NewTranspiler builds a transpiler over the given resource message descriptor.
func NewTranspiler(descriptor protoreflect.MessageDescriptor, opts ...TranspilerOption) (*Transpiler, error) {
	var options transpilerOptions
	for _, opt := range opts {
		opt(&options)
	}
	treeOptions := []aip.TreeOption{aip.WithMaxDepth(1), aip.WithAllowedPaths([]string{"*"})}
	if options.registry != nil {
		treeOptions = append(treeOptions, aip.WithRegistry(options.registry))
	}

	// First pass surfaces every top-level node; the second restricts the tree
	// to the addressable ones so declarations and error messages line up.
	tree, err := aip.BuildResourceTreeFromDescriptor(descriptor, treeOptions...)
	if err != nil {
		return nil, err
	}
	t := &Transpiler{
		enumByType: map[protoreflect.FullName]protoreflect.EnumDescriptor{},
		pathToFQN:  map[string]string{},
	}
	var allowedPaths []string
	for node := range tree.FilterableNodes() {
		if !staticallyAddressable(node) {
			continue
		}
		allowedPaths = append(allowedPaths, node.Path)
		fqn := node.Path
		if node.ColumnName != "" {
			fqn = node.ColumnName
		}
		t.pathToFQN[node.Path] = node.TableName + "." + fqn
		if node.EnumType != nil {
			enumDescriptor := node.EnumType.Descriptor()
			t.enumByType[enumDescriptor.FullName()] = enumDescriptor
		}
	}
	tree, err = aip.BuildResourceTreeFromDescriptor(descriptor, aip.WithMaxDepth(1), aip.WithAllowedPaths(allowedPaths), aip.WithRegistry(options.registry))
	if err != nil {
		return nil, err
	}

	t.declarations, t.macroDeclarations, t.macros, err = aip.NewFilterDeclarations(tree, true /*withFQN*/)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// TranspileFilter transpiles an AIP-160 filter into a bare SQL boolean
// expression, with column references qualified by their table name. An empty
// filter yields an empty clause.
func (t *Transpiler) TranspileFilter(filter string) (string, error) {
	if filter == "" {
		return "", nil
	}
	parsed, err := filtering.ParseFilter(staticRequest{filter: filter}, t.declarations)
	if err != nil {
		return "", fmt.Errorf("parsing filter: %w", err)
	}
	parsed, err = filtering.ApplyMacros(parsed, t.macroDeclarations, t.macros...)
	if err != nil {
		return "", fmt.Errorf("applying macros: %w", err)
	}
	clause, _, err := postgres.TranspileFilter(parsed,
		postgres.WithInlineParams(),
		postgres.WithEnumResolver(func(name protoreflect.FullName) protoreflect.EnumDescriptor {
			return t.enumByType[name]
		}),
	)
	if err != nil {
		return "", fmt.Errorf("transpiling filter to SQL: %w", err)
	}
	return strings.TrimPrefix(clause, "WHERE "), nil
}

// TranspileOrderBy transpiles an AIP-132 order_by into a bare SQL ordering
// expression, with column references qualified by their table name. An empty
// order_by yields an empty clause.
func (t *Transpiler) TranspileOrderBy(orderBy string) (string, error) {
	if orderBy == "" {
		return "", nil
	}
	parsed, err := ordering.ParseOrderBy(staticRequest{orderBy: orderBy})
	if err != nil {
		return "", fmt.Errorf("parsing order_by: %w", err)
	}
	for i, field := range parsed.Fields {
		fqn, ok := t.pathToFQN[field.Path]
		if !ok {
			return "", fmt.Errorf("order_by field %q is not a stored scalar field", field.Path)
		}
		parsed.Fields[i].Path = fqn
	}
	return strings.TrimPrefix(postgres.TranspileOrderBy(parsed), "ORDER BY "), nil
}

// staticallyAddressable reports whether a node is a top-level stored scalar
// column of the resource's own table.
func staticallyAddressable(node *aip.Node) bool {
	if node.Depth != 0 || node.JoinTableName != "" || node.AsJsonBytes || node.AsProtoBytes || node.IsRepeated || node.IsMap {
		return false
	}
	return node.ExprType != nil || node.EnumType != nil
}

// staticRequest adapts bare filter/order_by strings to the einride request interfaces.
type staticRequest struct {
	filter  string
	orderBy string
}

func (r staticRequest) GetFilter() string  { return r.filter }
func (r staticRequest) GetOrderBy() string { return r.orderBy }

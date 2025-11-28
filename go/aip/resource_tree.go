package aip

import (
	"fmt"
	"iter"
	"regexp"
	"slices"
	"strings"

	"buf.build/go/protovalidate"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
)

type TreeConfig struct {
	TransformNestedPath bool
}

type Tree struct {
	Validator      protovalidate.Validator
	AllowAllPaths  bool
	AllowedPathSet map[string]struct{}
	MaxDepth       int
	Nodes          []*Node
	Config         *TreeConfig
}

type TreeOption func(*TreeConfig)

func WithTransformNestedPath() TreeOption {
	return func(tc *TreeConfig) {
		tc.TransformNestedPath = true
	}
}

// In filtering_request.go, extract tree building to a shared function
func BuildResourceTree[R proto.Message](maxDepth int, allowedPaths []string, opts ...TreeOption) (*Tree, error) {
	config := &TreeConfig{}
	for _, opt := range opts {
		opt(config)
	}
	validator, err := protovalidate.New(
		protovalidate.WithDisableLazy(),
		protovalidate.WithMessages(&modelpb.FieldOpts{}),
	)

	// Move the tree building logic here
	tree, err := newTree(validator, maxDepth, allowedPaths)
	if err != nil {
		return nil, err
	}

	var resourceZero R
	resourceDescriptor := resourceZero.ProtoReflect().Descriptor()
	fields := resourceDescriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fieldName := field.TextName()
		if fieldName == "name" {
			continue
		}
		if err := tree.Explore(fieldName, field, 0); err != nil {
			return nil, fmt.Errorf("exploring %s: %v", fieldName, err)
		}
	}

	// Sort the tree.
	tree.SortAsc()

	// Implement the replacements.
	fieldPathToReplacement := map[string]string{}
	for _, node := range tree.Nodes {
		node.AllowedPath = tree.IsPathAllowed(node) // Set the status.

		// Adjust path.
		replacementPath := node.Path

		// Check column name override on root object.
		if node.Depth == 0 && node.ColumnName != "" {
			replacementPath = node.ColumnName
		}
		// Handle nested fields.
		if node.Depth > 0 {
			// Handle the case where this node is nested under a root path with a replacement path.
			// The root replacement must already be in the map because we sort the tree in ascending node depth.
			rootNodePath := node.RootNodePath()
			if rootNodeReplacement, ok := fieldPathToReplacement[rootNodePath]; ok {
				replacementPath = strings.Replace(replacementPath, rootNodePath, rootNodeReplacement, 1) // 1 to only make one replacement.
			}
			if config.TransformNestedPath {
				replacementPath = strings.ReplaceAll(replacementPath, ".", "@")
			}
		}
		// Tag the replacement.
		if replacementPath != node.Path {
			node.ReplacementPath = replacementPath
			node.ReplacementPathRegexp = getReplacementPathRegexp(node.Path)
		}
	}

	return tree, nil
}

func (t *Tree) AllowedNodes() iter.Seq[*Node] {
	return func(yield func(*Node) bool) {
		for _, node := range t.Nodes {
			if node.AllowedPath {
				if !yield(node) {
					return
				}
			}
		}
	}
}

func newTree(validator protovalidate.Validator, maxDepth int, allowedPaths []string) (*Tree, error) {
	var allowAllPaths bool
	allowedPathSet := make(map[string]struct{}, len(allowedPaths))
	for _, allowedPath := range allowedPaths {
		allowedPathSet[allowedPath] = struct{}{}
		if allowedPath == "*" {
			allowAllPaths = true
		}
	}
	if allowAllPaths && len(allowedPaths) != 1 {
		return nil, fmt.Errorf("cannot use '*' in combination with other paths")
	}

	return &Tree{
		Validator:      validator,
		AllowAllPaths:  allowAllPaths,
		AllowedPathSet: allowedPathSet,
		MaxDepth:       10,
	}, nil
}

func (t *Tree) SortAsc() {
	slices.SortFunc(t.Nodes, func(a, b *Node) int { return a.Depth - b.Depth })
}

func (t *Tree) Add(n *Node) {
	t.Nodes = append(t.Nodes, n)
}

// IsPathAllowed checks if a path is allowed for filtering based on the configured paths.
// Returns true if:
// - No paths are configured (AllowedPathSet is empty), meaning all paths are allowed
// - The path exactly matches an allowed path
// - The path matches a wildcard pattern (e.g., "nested.*" allows "nested.field1")
// - The path is a parent of an allowed path (e.g., "nested" is allowed if "nested.field1" is allowed)
func (t *Tree) IsPathAllowed(node *Node) bool {
	// This should not be possible but better to be defensive.
	if len(t.AllowedPathSet) == 0 {
		return false
	}
	if t.AllowAllPaths {
		return true
	}

	// Check exact match
	path := node.Path
	if _, ok := t.AllowedPathSet[path]; ok {
		return true
	}

	// Check wildcard matches
	// For path "nested.field1", check if "nested.*" is allowed
	// For path "nested.deep.field", check both "nested.*" and "nested.deep.*"
	parts := strings.Split(path, ".")
	for i := 1; i <= len(parts); i++ {
		wildcardPath := strings.Join(parts[:i], ".") + ".*"
		if _, ok := t.AllowedPathSet[wildcardPath]; ok {
			return true
		}
	}

	return false
}

type Node struct {
	// Parse time.
	Depth      int
	ColumnName string
	Path       string
	Nullable   bool
	ExprType   *v1alpha1.Type
	EnumType   protoreflect.EnumType

	// Replacement stuff.
	AllowedPath           bool
	ReplacementPath       string
	ReplacementPathRegexp *regexp.Regexp
}

func (n *Node) RootNodePath() string {
	if idx := strings.Index(n.Path, "."); idx != -1 {
		return n.Path[:idx]
	}
	return n.Path
}

func getReplacementPathRegexp(path string) *regexp.Regexp {
	return regexp.MustCompile(`\b` + regexp.QuoteMeta(path) + `\b(?:[^.]|$)`)
}

// ApplyReplacement replaces all occurrences of a field path in the clause with its replacement.
// The field is treated as an atomic unit - dots are literal separators, not regex wildcards.
// A field matches only when:
//   - It starts at a word boundary
//   - It ends at a word boundary
//   - It's NOT followed by a dot (which would make it part of a longer path)
//
// After replacement, if the replacement contains '@' (indicating a nested JSONB field),
// it wraps the replacement in JSONB().
func (n *Node) ApplyReplacement(clause string) string {
	if n.ReplacementPath == "" {
		return clause
	}
	result := n.ReplacementPathRegexp.ReplaceAllStringFunc(clause, func(match string) string {
		// The match includes the field + one extra character (or nothing at end)
		// We need to preserve that extra character
		fieldLen := len(n.Path)
		suffix := match[fieldLen:] // Empty string or the non-dot character

		replacementPath := n.ReplacementPath
		if strings.Contains(replacementPath, "@") {
			replacementPath = "JSONB(" + replacementPath + ")"
		}
		return replacementPath + suffix
	})

	return result
}

func (t *Tree) Explore(fieldPath string, fieldDesc protoreflect.FieldDescriptor, depth int) error {
	if depth == t.MaxDepth+1 { // Exact check as sanity check that we are traversing correctly.
		return nil
	}

	// Parse options.
	fieldName := fieldDesc.TextName()
	options := fieldDesc.Options()
	var fieldOpts *modelpb.FieldOpts
	if proto.HasExtension(options, modelpb.E_FieldOpts) {
		fieldOpts = proto.GetExtension(options, modelpb.E_FieldOpts).(*modelpb.FieldOpts)
		if err := t.Validator.Validate(fieldOpts); err != nil {
			return fmt.Errorf("validating fields opts %s: %v", fieldName, err)
		}
	}

	// We do not store this field in the db.
	if fieldOpts.GetSkip() {
		return nil
	}

	// Create and add the node.
	node := &Node{Depth: depth, Path: fieldPath, Nullable: fieldOpts.GetNullable(), ColumnName: fieldOpts.GetColumnName()}
	t.Add(node)

	switch fieldDesc.Kind() {
	case protoreflect.BoolKind:
		node.ExprType = &v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: v1alpha1.Type_BOOL}}

	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind, protoreflect.Sint64Kind,
		protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		node.ExprType = &v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: v1alpha1.Type_INT64}}

	case protoreflect.FloatKind, protoreflect.DoubleKind:
		node.ExprType = &v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: v1alpha1.Type_DOUBLE}}

	case protoreflect.StringKind:
		node.ExprType = &v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: v1alpha1.Type_STRING}}

	case protoreflect.EnumKind:
		enumType, err := protoregistry.GlobalTypes.FindEnumByName(fieldDesc.Enum().FullName())
		if err != nil {
			return fmt.Errorf("finding enum type %s: %w", fieldDesc.Enum().FullName(), err)
		}
		node.EnumType = enumType

	case protoreflect.MessageKind:
		msgFullName := fieldDesc.Message().FullName()
		switch msgFullName {
		case "google.protobuf.Timestamp":
			node.ExprType = &v1alpha1.Type{TypeKind: &v1alpha1.Type_WellKnown{WellKnown: v1alpha1.Type_TIMESTAMP}}

		default:
			if fieldOpts.GetAsJsonBytes() || depth > 0 {
				// Recursively handle nested message fields
				nestedFieldsDescriptor := fieldDesc.Message().Fields()
				for i := 0; i < nestedFieldsDescriptor.Len(); i++ {
					nestedFieldDesc := nestedFieldsDescriptor.Get(i)
					nestedFieldName := nestedFieldDesc.TextName()
					nestedFieldPath := fieldPath + "." + nestedFieldName
					if err := t.Explore(nestedFieldPath, nestedFieldDesc, depth+1); err != nil {
						return fmt.Errorf("%s: %v", nestedFieldPath, err)
					}
				}
			}
		}
	}

	return nil
}

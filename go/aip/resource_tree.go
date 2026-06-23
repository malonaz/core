package aip

import (
	"errors"
	"fmt"
	"iter"
	"regexp"
	"slices"
	"strings"

	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/go/pbutil"
)

const (
	defaultMaxDepth = 10
)

// TreeConfig holds configuration for building a resource tree.
type TreeConfig struct {
	maxDepth     int
	allowedPaths []string
	registry     *protoregistry.Files
}

// Tree represents the parsed structure of a protobuf resource message,
// including all its fields as nodes with metadata about types, column mappings,
// and allowed paths for filtering/ordering/updating.
type Tree struct {
	Config         *TreeConfig
	Resource       *ParsedResource
	AllowAllPaths  bool
	AllowedPathSet map[string]struct{}
	Nodes          []*Node
	IDColumnName   string
	Registry       *protoregistry.Files
}

// TreeOption configures how a resource tree is built.
type TreeOption func(*TreeConfig)

// WithMaxDepth sets the maximum depth for exploring nested message fields.
func WithMaxDepth(maxDepth int) TreeOption {
	return func(tc *TreeConfig) {
		tc.maxDepth = maxDepth
	}
}

// WithAllowedPaths restricts which field paths are considered allowed.
// Use "*" to allow all paths.
func WithAllowedPaths(paths []string) TreeOption {
	return func(tc *TreeConfig) {
		tc.allowedPaths = paths
	}
}

// WithRegistry sets the protobuf file registry used to resolve cross-message
// references (e.g. join parent resources). Defaults to protoregistry.GlobalFiles.
func WithRegistry(registry *protoregistry.Files) TreeOption {
	return func(tc *TreeConfig) {
		tc.registry = registry
	}
}

// BuildResourceTreeFromDescriptor builds a tree from a raw message descriptor
// without requiring a concrete Go type. Useful for dynamic/codegen scenarios.
func BuildResourceTreeFromDescriptor(msgDesc protoreflect.MessageDescriptor, opts ...TreeOption) (*Tree, error) {
	config := &TreeConfig{}
	for _, opt := range opts {
		opt(config)
	}
	tree, err := newTree(config)
	if err != nil {
		return nil, err
	}

	fields := msgDesc.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if err := tree.Explore(field.TextName(), field, 0); err != nil {
			return nil, fmt.Errorf("exploring %s: %v", field.TextName(), err)
		}
	}

	tree.SortAsc()
	for _, node := range tree.Nodes {
		node.AllowedPath = tree.IsPathAllowed(node)
	}
	return tree, nil
}

// BuildResourceTree builds a tree for a concrete protobuf resource type.
// It parses the resource descriptor, model options, and field options to
// produce nodes with column name mappings and replacement paths.
func BuildResourceTree[R proto.Message](opts ...TreeOption) (*Tree, error) {
	config := &TreeConfig{}
	for _, opt := range opts {
		opt(config)
	}
	tree, err := newTree(config)
	if err != nil {
		return nil, err
	}

	var resourceZero R
	{
		resourceMessage := resourceZero.ProtoReflect().Interface()
		resourceDescriptor, err := pbutil.GetMessageOption[*annotationspb.ResourceDescriptor](
			resourceMessage,
			annotationspb.E_Resource,
		)
		if err != nil {
			return nil, fmt.Errorf("getting resource descriptor: %w", err)
		}
		resource, err := ParseResource(resourceDescriptor)
		if err != nil {
			return nil, fmt.Errorf("parsing resource descriptor: %w", err)
		}
		tree.Resource = resource
	}

	defaultTableName := tree.Resource.Singular
	{
		modelOpts, err := pbutil.GetMessageOption[*modelpb.ModelOpts](resourceZero, modelpb.E_ModelOpts)
		if err == nil && modelOpts != nil {
			tree.IDColumnName = modelOpts.GetIdColumnName()
			if modelOpts.GetTableName() != "" {
				defaultTableName = modelOpts.GetTableName()
			}
		}
	}

	resourceDescriptor := resourceZero.ProtoReflect().Descriptor()
	fields := resourceDescriptor.Fields()

	fieldPathToJoin := map[string]*modelpb.Join{}
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fOpts, fErr := pbutil.GetExtension[*modelpb.FieldOpts](field.Options(), modelpb.E_FieldOpts)
		if fErr == nil && fOpts.GetJoin() != nil {
			fieldPathToJoin[field.TextName()] = fOpts.GetJoin()
		}
	}

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fieldName := field.TextName()
		if err := tree.Explore(fieldName, field, 0); err != nil {
			return nil, fmt.Errorf("exploring %s: %v", fieldName, err)
		}
	}

	tree.SortAsc()

	fieldPathToReplacement := map[string]string{}
	for _, node := range tree.Nodes {
		node.AllowedPath = tree.IsPathAllowed(node)

		if node.JoinTableName != "" {
			node.TableName = node.JoinTableName
			if join, ok := fieldPathToJoin[node.Path]; ok && node.ColumnName == "" {
				joinColName, err := resolveJoinFieldColumnName(tree.Registry, join)
				if err != nil {
					return nil, fmt.Errorf("resolving join column name for %s: %v", node.Path, err)
				}
				node.ColumnName = joinColName
			}
		} else {
			node.TableName = defaultTableName
		}

		replacementPath := node.Path

		if node.Depth == 0 && node.ColumnName != "" {
			replacementPath = node.ColumnName
		}

		if node.Depth > 0 {
			rootNodePath := node.RootNodePath()
			if rootNodeReplacement, ok := fieldPathToReplacement[rootNodePath]; ok {
				replacementPath = strings.Replace(replacementPath, rootNodePath, rootNodeReplacement, 1)
			}
		}

		if replacementPath != node.Path {
			node.ReplacementPath = replacementPath
			node.ReplacementPathRegexp = getReplacementPathRegexp(node.Path)
			fieldPathToReplacement[node.Path] = replacementPath
		}
	}

	return tree, nil
}

// OrderableNodes returns an iterator over nodes that can be used in ORDER BY clauses.
func (t *Tree) OrderableNodes() iter.Seq[*Node] {
	return t.FilterableNodes()
}

// FilterableNodes returns an iterator over nodes that can be used in filter expressions.
// Excludes IDENTIFIER and INPUT_ONLY fields.
func (t *Tree) FilterableNodes() iter.Seq[*Node] {
	return func(yield func(*Node) bool) {
		for node := range t.allowedNodes() {
			if node.HasFieldBehavior(annotationspb.FieldBehavior_IDENTIFIER) || node.HasFieldBehavior(annotationspb.FieldBehavior_INPUT_ONLY) {
				continue
			}
			if !yield(node) {
				return
			}
		}
	}
}

// UpdatableNodes returns an iterator over nodes that can be updated.
// Excludes IDENTIFIER, OUTPUT_ONLY, and IMMUTABLE fields.
func (t *Tree) UpdatableNodes() iter.Seq[*Node] {
	return func(yield func(*Node) bool) {
		for node := range t.allowedNodes() {
			if node.HasFieldBehavior(annotationspb.FieldBehavior_IDENTIFIER) ||
				node.HasFieldBehavior(annotationspb.FieldBehavior_OUTPUT_ONLY) ||
				node.HasFieldBehavior(annotationspb.FieldBehavior_IMMUTABLE) {
				continue
			}
			if !yield(node) {
				return
			}
		}
	}
}

func (t *Tree) allowedNodes() iter.Seq[*Node] {
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

func newTree(config *TreeConfig) (*Tree, error) {
	if config.maxDepth == 0 {
		config.maxDepth = defaultMaxDepth
	}
	if config.registry == nil {
		config.registry = protoregistry.GlobalFiles
	}

	var allowAllPaths bool
	allowedPathSet := make(map[string]struct{}, len(config.allowedPaths))
	for _, allowedPath := range config.allowedPaths {
		allowedPathSet[allowedPath] = struct{}{}
		if allowedPath == "*" {
			allowAllPaths = true
		}
	}
	if allowAllPaths && len(config.allowedPaths) != 1 {
		return nil, fmt.Errorf("cannot use '*' in combination with other paths")
	}

	return &Tree{
		Config:         config,
		AllowAllPaths:  allowAllPaths,
		AllowedPathSet: allowedPathSet,
		Registry:       config.registry,
	}, nil
}

// SortAsc sorts nodes by depth in ascending order.
func (t *Tree) SortAsc() {
	slices.SortFunc(t.Nodes, func(a, b *Node) int { return a.Depth - b.Depth })
}

// Add appends a node to the tree.
func (t *Tree) Add(n *Node) {
	t.Nodes = append(t.Nodes, n)
}

// IsPathAllowed reports whether the given node's path is permitted by the
// tree's configured allowed paths. A path is considered allowed if:
//   - The tree is configured with the wildcard "*" (all paths allowed).
//   - The path exactly matches a configured allowed path.
//   - A proper ancestor of the path is a configured allowed path, meaning
//     the allowed path implicitly covers all descendants (e.g. "metadata"
//     allows "metadata.country", "metadata.email_addresses", etc.).
//
// Returns false if no allowed paths are configured.
func (t *Tree) IsPathAllowed(node *Node) bool {
	if len(t.AllowedPathSet) == 0 {
		return false
	}
	if t.AllowAllPaths {
		return true
	}

	path := node.Path

	if _, ok := t.AllowedPathSet[path]; ok {
		return true
	}

	// Walk up the path hierarchy checking each ancestor.
	parts := strings.Split(path, ".")
	for i := 1; i < len(parts); i++ {
		parent := strings.Join(parts[:i], ".")
		if _, ok := t.AllowedPathSet[parent]; ok {
			return true
		}
	}

	return false
}

// Node represents a single field in the resource tree, carrying all metadata
// needed for SQL generation, filtering declarations, and update parsing.
type Node struct {
	Depth            int
	TableName        string
	ColumnName       string
	Path             string
	Nullable         bool
	ExprType         *v1alpha1.Type
	EnumType         protoreflect.EnumType
	AsJsonBytes      bool
	AsProtoBytes     bool
	FieldBehaviorSet map[annotationspb.FieldBehavior]struct{}
	IsRepeated       bool
	IsMap            bool
	// JoinTableName is set for fields populated via a JOIN; it holds the
	// resolved table name of the parent resource referenced by the join.
	JoinTableName string

	AllowedPath           bool
	ReplacementPath       string
	ReplacementPathRegexp *regexp.Regexp
}

// HasFieldBehavior returns true if the node has the given field behavior annotation.
func (n *Node) HasFieldBehavior(fb annotationspb.FieldBehavior) bool {
	_, ok := n.FieldBehaviorSet[fb]
	return ok
}

// RootNodePath returns the top-level field name for a potentially nested path.
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
// The field is treated as an atomic unit — it matches only at word boundaries and
// is NOT followed by a dot (which would make it part of a longer path).
func (n *Node) ApplyReplacement(clause string) string {
	if n.ReplacementPath == "" {
		return clause
	}
	result := n.ReplacementPathRegexp.ReplaceAllStringFunc(clause, func(match string) string {
		fieldLen := len(n.Path)
		suffix := match[fieldLen:]
		return n.ReplacementPath + suffix
	})
	return result
}

// Explore recursively walks a field descriptor and adds nodes to the tree.
// It handles scalars, enums, maps, well-known types, and nested messages.
func (t *Tree) Explore(fieldPath string, fieldDesc protoreflect.FieldDescriptor, depth int) error {
	if depth == t.Config.maxDepth {
		return nil
	}

	fieldName := fieldDesc.TextName()
	options := fieldDesc.Options()
	fieldOpts, err := pbutil.GetExtension[*modelpb.FieldOpts](options, modelpb.E_FieldOpts)
	if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
		return fmt.Errorf("getting field opts for %s: %v", fieldName, err)
	}
	if fieldOpts.GetSkip() {
		return nil
	}

	// Resolve join table name from the parent resource if this is a join field.
	var joinTableName string
	if join := fieldOpts.GetJoin(); join != nil {
		tableName, err := resolveJoinTableName(t.Registry, join)
		if err != nil {
			return fmt.Errorf("resolving join table for %s: %v", fieldName, err)
		}
		joinTableName = tableName
	}

	node := &Node{
		Depth:         depth,
		Path:          fieldPath,
		Nullable:      fieldOpts.GetNullable(),
		ColumnName:    fieldOpts.GetColumnName(),
		AsJsonBytes:   depth == 0 && fieldOpts.GetAsJsonBytes(),
		AsProtoBytes:  depth == 0 && fieldOpts.GetAsProtoBytes(),
		IsRepeated:    fieldDesc.IsList(),
		IsMap:         fieldDesc.IsMap(),
		JoinTableName: joinTableName,
	}
	t.Add(node)

	behaviors, err := pbutil.GetExtension[[]annotationspb.FieldBehavior](options, annotationspb.E_FieldBehavior)
	if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
		return fmt.Errorf("getting field behaviors for %s: %v", fieldName, err)
	}
	if len(behaviors) > 0 {
		node.FieldBehaviorSet = make(map[annotationspb.FieldBehavior]struct{}, len(behaviors))
		for _, fb := range behaviors {
			node.FieldBehaviorSet[fb] = struct{}{}
		}
	}

	// wrapIfRepeated wraps a type in a list type when the field is repeated.
	wrapIfRepeated := func(elemType *v1alpha1.Type) *v1alpha1.Type {
		if node.IsRepeated {
			return &v1alpha1.Type{
				TypeKind: &v1alpha1.Type_ListType_{
					ListType: &v1alpha1.Type_ListType{
						ElemType: elemType,
					},
				},
			}
		}
		return elemType
	}

	switch fieldDesc.Kind() {
	case protoreflect.BoolKind:
		node.ExprType = wrapIfRepeated(&v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: v1alpha1.Type_BOOL}})

	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind, protoreflect.Sint64Kind,
		protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		node.ExprType = wrapIfRepeated(&v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: v1alpha1.Type_INT64}})

	case protoreflect.FloatKind, protoreflect.DoubleKind:
		node.ExprType = wrapIfRepeated(&v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: v1alpha1.Type_DOUBLE}})

	case protoreflect.StringKind:
		node.ExprType = wrapIfRepeated(&v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: v1alpha1.Type_STRING}})

	case protoreflect.EnumKind:
		node.EnumType = dynamicpb.NewEnumType(fieldDesc.Enum())

	case protoreflect.MessageKind:
		if fieldDesc.IsMap() {
			keyKind := fieldDesc.MapKey().Kind()
			valueKind := fieldDesc.MapValue().Kind()
			if keyKind != protoreflect.StringKind {
				break
			}
			if valueKind != protoreflect.StringKind {
				break
			}
			node.ExprType = &v1alpha1.Type{
				TypeKind: &v1alpha1.Type_MapType_{
					MapType: &v1alpha1.Type_MapType{
						KeyType:   &v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: v1alpha1.Type_STRING}},
						ValueType: &v1alpha1.Type{TypeKind: &v1alpha1.Type_Primitive{Primitive: v1alpha1.Type_STRING}},
					},
				},
			}
		} else {
			msgFullName := fieldDesc.Message().FullName()
			switch msgFullName {
			case "google.protobuf.Timestamp":
				node.ExprType = wrapIfRepeated(&v1alpha1.Type{TypeKind: &v1alpha1.Type_WellKnown{WellKnown: v1alpha1.Type_TIMESTAMP}})
			case "google.protobuf.Duration":
				node.ExprType = wrapIfRepeated(&v1alpha1.Type{TypeKind: &v1alpha1.Type_WellKnown{WellKnown: v1alpha1.Type_DURATION}})
			// Skip well-known wrapper/recursive types that cause combinatorial explosion.
			case "google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue",
				"google.protobuf.Any":
			default:
				if fieldOpts.GetAsJsonBytes() || depth > 0 {
					elemType := &v1alpha1.Type{
						TypeKind: &v1alpha1.Type_MessageType{
							MessageType: string(msgFullName),
						},
					}
					node.ExprType = wrapIfRepeated(elemType)
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
	}

	return nil
}

// resolveJoinTableName resolves the database table name for a join field's parent resource.
// It looks up the parent resource type in the registry, extracts the singular name from
// its resource descriptor as the default table name, then checks for a model_opts.table_name override.
func resolveJoinTableName(registry *protoregistry.Files, join *modelpb.Join) (string, error) {
	parentMsg, err := findMessageByResourceType(registry, join.GetParent())
	if err != nil {
		return "", err
	}

	resourceDescriptor, err := pbutil.GetExtension[*annotationspb.ResourceDescriptor](parentMsg.Options(), annotationspb.E_Resource)
	if err != nil {
		return "", fmt.Errorf("getting resource descriptor: %v", err)
	}
	tableName := resourceDescriptor.GetSingular()

	modelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](parentMsg.Options(), modelpb.E_ModelOpts)
	if err == nil && modelOpts != nil && modelOpts.GetTableName() != "" {
		tableName = modelOpts.GetTableName()
	}

	return tableName, nil
}

// findMessageByResourceType scans all files in the registry for a message
// whose google.api.resource type matches the given resource type string.
func findMessageByResourceType(registry *protoregistry.Files, resourceType string) (protoreflect.MessageDescriptor, error) {
	var found protoreflect.MessageDescriptor
	registry.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			msg := msgs.Get(i)
			rd, err := pbutil.GetExtension[*annotationspb.ResourceDescriptor](msg.Options(), annotationspb.E_Resource)
			if err != nil || rd == nil {
				continue
			}
			if rd.GetType() == resourceType {
				found = msg
				return false
			}
		}
		return true
	})
	if found == nil {
		return nil, fmt.Errorf("message with resource type %q not found", resourceType)
	}
	return found, nil
}

// resolveJoinFieldColumnName resolves the database column name for a joined field
// by looking up the referenced field on the parent message. Returns the field's
// column_name override if set, otherwise the field's proto name.
func resolveJoinFieldColumnName(registry *protoregistry.Files, join *modelpb.Join) (string, error) {
	parentMsg, err := findMessageByResourceType(registry, join.GetParent())
	if err != nil {
		return "", err
	}

	fieldName := join.GetField()
	fieldDesc := parentMsg.Fields().ByName(protoreflect.Name(fieldName))
	if fieldDesc == nil {
		return "", fmt.Errorf("field %q not found on parent %q", fieldName, join.GetParent())
	}

	fieldOpts, err := pbutil.GetExtension[*modelpb.FieldOpts](fieldDesc.Options(), modelpb.E_FieldOpts)
	if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
		return "", fmt.Errorf("getting field opts for %s: %v", fieldName, err)
	}

	if fieldOpts.GetColumnName() != "" {
		return fieldOpts.GetColumnName(), nil
	}

	return fieldName, nil
}

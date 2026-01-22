// go/aip/resource_tree.go
package aip

import (
	"fmt"
	"iter"
	"regexp"
	"slices"
	"strings"

	"buf.build/go/protovalidate"
	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/go/pbutil"
)

const (
	defaultMaxDepth = 10
)

type TreeConfig struct {
	maxDepth     int
	allowedPaths []string
}

type Tree struct {
	Config         *TreeConfig
	Validator      protovalidate.Validator
	Resource       *ParsedResource
	AllowAllPaths  bool
	AllowedPathSet map[string]struct{}
	Nodes          []*Node
	IDColumnName   string
}

type TreeOption func(*TreeConfig)

func WithMaxDepth(maxDepth int) TreeOption {
	return func(tc *TreeConfig) {
		tc.maxDepth = maxDepth
	}
}

func WithAllowedPaths(paths []string) TreeOption {
	return func(tc *TreeConfig) {
		tc.allowedPaths = paths
	}
}

func BuildResourceTreeFromDescriptor(msgDesc protoreflect.MessageDescriptor, opts ...TreeOption) (*Tree, error) {
	config := &TreeConfig{}
	for _, opt := range opts {
		opt(config)
	}
	validator, err := protovalidate.New(
		protovalidate.WithDisableLazy(),
		protovalidate.WithMessages(&modelpb.FieldOpts{}),
	)
	if err != nil {
		return nil, err
	}

	tree, err := newTree(config, validator)
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

func BuildResourceTree[R proto.Message](opts ...TreeOption) (*Tree, error) {
	config := &TreeConfig{}
	for _, opt := range opts {
		opt(config)
	}
	validator, err := protovalidate.New(
		protovalidate.WithDisableLazy(),
		protovalidate.WithMessages(&modelpb.FieldOpts{}),
	)
	if err != nil {
		return nil, err
	}

	tree, err := newTree(config, validator)
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

	{
		modelOpts, err := pbutil.GetMessageOption[*modelpb.ModelOpts](resourceZero, modelpb.E_ModelOpts)
		if err == nil && modelOpts != nil {
			tree.IDColumnName = modelOpts.GetIdColumnName()
		}
	}

	resourceDescriptor := resourceZero.ProtoReflect().Descriptor()
	fields := resourceDescriptor.Fields()
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

func (t *Tree) OrderableNodes() iter.Seq[*Node] {
	return t.FilterableNodes()
}

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

func newTree(config *TreeConfig, validator protovalidate.Validator) (*Tree, error) {
	if config.maxDepth == 0 {
		config.maxDepth = defaultMaxDepth
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
		Validator:      validator,
		AllowAllPaths:  allowAllPaths,
		AllowedPathSet: allowedPathSet,
	}, nil
}

func (t *Tree) SortAsc() {
	slices.SortFunc(t.Nodes, func(a, b *Node) int { return a.Depth - b.Depth })
}

func (t *Tree) Add(n *Node) {
	t.Nodes = append(t.Nodes, n)
}

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
	Depth            int
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

	AllowedPath           bool
	ReplacementPath       string
	ReplacementPathRegexp *regexp.Regexp
}

func (n *Node) HasFieldBehavior(fb annotationspb.FieldBehavior) bool {
	_, ok := n.FieldBehaviorSet[fb]
	return ok
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

func (t *Tree) Explore(fieldPath string, fieldDesc protoreflect.FieldDescriptor, depth int) error {
	if depth == t.Config.maxDepth {
		return nil
	}

	fieldName := fieldDesc.TextName()
	options := fieldDesc.Options()
	var fieldOpts *modelpb.FieldOpts
	if proto.HasExtension(options, modelpb.E_FieldOpts) {
		fieldOpts = proto.GetExtension(options, modelpb.E_FieldOpts).(*modelpb.FieldOpts)
		if err := t.Validator.Validate(fieldOpts); err != nil {
			return fmt.Errorf("validating fields opts %s: %v", fieldName, err)
		}
	}

	if fieldOpts.GetSkip() {
		return nil
	}

	isRepeated := fieldDesc.Cardinality() == protoreflect.Repeated && !fieldDesc.IsMap()

	node := &Node{
		Depth:        depth,
		Path:         fieldPath,
		Nullable:     fieldOpts.GetNullable(),
		ColumnName:   fieldOpts.GetColumnName(),
		AsJsonBytes:  depth == 0 && fieldOpts.GetAsJsonBytes(),
		AsProtoBytes: depth == 0 && fieldOpts.GetAsProtoBytes(),
		IsRepeated:   isRepeated,
		IsMap:        fieldDesc.IsMap(),
	}
	t.Add(node)

	if proto.HasExtension(options, annotationspb.E_FieldBehavior) {
		behaviors := proto.GetExtension(options, annotationspb.E_FieldBehavior).([]annotationspb.FieldBehavior)
		node.FieldBehaviorSet = make(map[annotationspb.FieldBehavior]struct{}, len(behaviors))
		for _, fb := range behaviors {
			node.FieldBehaviorSet[fb] = struct{}{}
		}
	}

	// Helper to wrap type in list if repeated
	wrapIfRepeated := func(elemType *v1alpha1.Type) *v1alpha1.Type {
		if isRepeated {
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
		enumType, err := protoregistry.GlobalTypes.FindEnumByName(fieldDesc.Enum().FullName())
		if err != nil {
			return fmt.Errorf("finding enum type %s: %w", fieldDesc.Enum().FullName(), err)
		}
		node.EnumType = enumType

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

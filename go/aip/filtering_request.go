package aip

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"buf.build/go/protovalidate"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/spanner-aip/spanfiltering"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/go/pbutil"
)

// //////////////////////////// INTERFACE //////////////////////////
type filteringRequest interface {
	proto.Message
	filtering.Request
}

// //////////////////////////// PARSER //////////////////////////
type FilteringRequestParser[T filteringRequest, R proto.Message] struct {
	resourceMessage R
	validator       protovalidate.Validator
	declarations    *filtering.Declarations
	tree            *Tree
}

func MustNewFilteringRequestParser[T filteringRequest, R proto.Message]() *FilteringRequestParser[T, R] {
	parser, err := NewFilteringRequestParser[T, R]()
	if err != nil {
		panic(err)
	}
	return parser
}

func NewFilteringRequestParser[T filteringRequest, R proto.Message]() (*FilteringRequestParser[T, R], error) {
	validator, err := protovalidate.New(
		protovalidate.WithDisableLazy(),
		protovalidate.WithMessages(&aippb.FilteringOptions{}, &modelpb.FieldOpts{}),
	)
	if err != nil {
		return nil, fmt.Errorf("instantiating validator for filtering request parser: %v", err)
	}

	// Parse options from the generic type T
	var zero T
	filteringOptions, err := pbutil.GetMessageOption[*aippb.FilteringOptions](zero, aippb.E_Filtering)
	if err != nil {
		return nil, fmt.Errorf("getting filtering options: %v", err)
	}
	// Validate options
	if err := validator.Validate(filteringOptions); err != nil {
		return nil, fmt.Errorf("validating options: %v", err)
	}

	// Create a zero value of the resource type for validation
	var resourceZero R

	// Create a tree and explore.
	tree, err := NewTree(validator, 10, filteringOptions.GetPaths())
	if err != nil {
		return nil, err
	}
	resourceDescriptor := resourceZero.ProtoReflect().Descriptor()
	fields := resourceDescriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fieldName := field.TextName()
		if fieldName == "name" { // Forbidden: this is the resource name.
			continue
		}

		if err := tree.Explore(fieldName, field, 0); err != nil {
			return nil, fmt.Errorf("exploring %s: %v", fieldName, field)
		}
	}

	// Instantiate data we need to collect.
	fieldPathToReplacement := map[string]string{}
	var declarationOptions []filtering.DeclarationOption
	isNullFunctionOverloads := getIsNullFunctionDefaultOverloads()

	// Sort the nodes in increase depth order and process.
	tree.SortAsc()
	for _, node := range tree.Nodes {
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
			replacementPath = strings.ReplaceAll(replacementPath, ".", "@")
		}
		// Tag the replacement.
		if replacementPath != node.Path {
			node.ReplacementPath = replacementPath
			node.ReplacementPathRegexp = getReplacementPathRegexp(node.Path)
		}
	}

	for _, node := range tree.AllowedNodes() {
		replacementPath := node.Path
		if node.ReplacementPath != "" {
			replacementPath = node.ReplacementPath
		}

		if node.ExprType != nil {
			declarationOptions = append(declarationOptions, filtering.DeclareIdent(replacementPath, node.ExprType))
		}
		if node.EnumType != nil {
			declarationOptions = append(declarationOptions, filtering.DeclareEnumIdent(replacementPath, node.EnumType))
			if node.Nullable || node.Depth > 0 { // It's either a root nullable field or a nullable-by-default JSONB value.
				enumIdentType := filtering.TypeEnum(node.EnumType)
				isNullOverload := filtering.NewFunctionOverload(
					spanfiltering.FunctionIsNull+"_"+enumIdentType.GetMessageType(), filtering.TypeBool, enumIdentType,
				)
				isNullFunctionOverloads = append(isNullFunctionOverloads, isNullOverload)
			}
		}
	}

	// Standard functions.
	declarationOptions = append(declarationOptions, filtering.DeclareStandardFunctions())
	// Jsonb function.
	declarationOptions = append(declarationOptions, jsonbFunctionDeclarationOption)
	// Construct the isNull declaration option
	isNullDeclarationOption := filtering.DeclareFunction(spanfiltering.FunctionIsNull, isNullFunctionOverloads...)
	declarationOptions = append(declarationOptions, isNullDeclarationOption)

	// Build the declarations
	declarations, err := filtering.NewDeclarations(declarationOptions...)
	if err != nil {
		return nil, fmt.Errorf("creating filter declarations: %w", err)
	}

	return &FilteringRequestParser[T, R]{
		resourceMessage: resourceZero,
		validator:       validator,
		declarations:    declarations,
		tree:            tree,
	}, nil
}

func (p *FilteringRequestParser[T, R]) Parse(request T) (*FilteringRequest, error) {
	filterClause := request.GetFilter()
	// A nested field is a path in a proto message like "hello.hi.how"
	// We need to replace those with `JSONB(hello@hi@now)`
	// We must be careful to ensure that if there's a `hello.hi` declared and a `hello.hi.now` declared.
	// We don't do `JSONB(hello.hi).now` / this would be problematic.
	for _, node := range p.tree.AllowedNodes() {
		filterClause = node.ApplyReplacement(filterClause)
	}
	p.setFilter(request, filterClause)

	// Parse filtering
	filter, err := filtering.ParseFilter(request, p.declarations)
	if err != nil {
		return nil, fmt.Errorf("parsing filter: %w", err)
	}

	// Transpile to SQL
	whereClause, whereParams, err := spanfiltering.TranspileFilter(filter)
	if err != nil {
		return nil, fmt.Errorf("transpiling filter to SQL: %w", err)
	}

	return &FilteringRequest{
		request:     request,
		filter:      filter,
		whereClause: whereClause,
		whereParams: whereParams,
	}, nil
}

// //////////////////////////// PARSED REQUEST //////////////////////////
type FilteringRequest struct {
	request     filtering.Request
	filter      filtering.Filter
	whereClause string
	whereParams []any
}

func (f *FilteringRequest) GetSQLWhereClause() (string, []any) {
	return f.whereClause, f.whereParams
}

// /////////////////////////// UTILS //////////////////////////////
func (p *FilteringRequestParser[T, R]) setFilter(request filteringRequest, filter string) {
	// Get the protobuf message descriptor
	msgReflect := request.ProtoReflect()
	// Get the field descriptor for "filter"
	fields := msgReflect.Descriptor().Fields()
	filterField := fields.ByName("filter")
	// Set the filter value
	msgReflect.Set(filterField, protoreflect.ValueOfString(filter))
}

var (
	jsonbFunctionDeclarationOption = filtering.DeclareFunction(
		spanfiltering.FunctionJSONB,
		filtering.NewFunctionOverload(
			spanfiltering.FunctionJSONB+"_string",
			filtering.TypeString,
			filtering.TypeString,
		),

		// Type casted as concrete type.
		filtering.NewFunctionOverload(
			spanfiltering.FunctionJSONB+"_bool",
			filtering.TypeBool,
			filtering.TypeBool,
		),
		filtering.NewFunctionOverload(
			spanfiltering.FunctionJSONB+"_int",
			filtering.TypeInt,
			filtering.TypeInt,
		),
	)
)

func getIsNullFunctionDefaultOverloads() []*v1alpha1.Decl_FunctionDecl_Overload {
	return []*v1alpha1.Decl_FunctionDecl_Overload{
		filtering.NewFunctionOverload(
			spanfiltering.FunctionIsNull+"_string",
			filtering.TypeBool,
			filtering.TypeString,
		),
		filtering.NewFunctionOverload(
			spanfiltering.FunctionIsNull+"_enum",
			filtering.TypeBool,
			filtering.TypeString,
		),
		filtering.NewFunctionOverload(
			spanfiltering.FunctionIsNull+"_bool",
			filtering.TypeBool,
			filtering.TypeBool,
		),
		filtering.NewFunctionOverload(
			spanfiltering.FunctionIsNull+"_int",
			filtering.TypeBool,
			filtering.TypeInt,
		),
		filtering.NewFunctionOverload(
			spanfiltering.FunctionIsNull+"_float",
			filtering.TypeBool,
			filtering.TypeFloat,
		),
	}
}

type Tree struct {
	Validator      protovalidate.Validator
	AllowAllPaths  bool
	AllowedPathSet map[string]struct{}
	MaxDepth       int
	Nodes          []*Node
}

func NewTree(validator protovalidate.Validator, maxDepth int, allowedPaths []string) (*Tree, error) {
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

func (t *Tree) AllowedNodes() []*Node {
	var allowedNodes = make([]*Node, 0, len(t.Nodes))
	for _, node := range t.Nodes {
		if t.IsPathAllowed(node) {
			allowedNodes = append(allowedNodes, node)
		}
	}
	return allowedNodes
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

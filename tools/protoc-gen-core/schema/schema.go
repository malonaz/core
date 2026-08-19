// Package schema resolves the database mapping of proto resources: tables,
// identifier columns, parent joins and singleton children. It is the single
// source of truth shared by the model, postgres and rpc generators.
package schema

import (
	"errors"
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/go/aip/transpiler/postgres/static"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/tools/protoc-gen-core/resource"
)

// Table identifies a database table, optionally schema-qualified.
type Table struct {
	Schema string
	Name   string
}

// TableOf resolves the table of a resource from its model options.
func TableOf(parsedResource *resource.ParsedResource, modelOpts *modelpb.ModelOpts) Table {
	name := parsedResource.SingularSnakeCase()
	if modelOpts.GetTableName() != "" {
		name = modelOpts.GetTableName()
	}
	return Table{Schema: modelOpts.GetSchemaName(), Name: name}
}

// Qualified returns the schema-qualified table name, or the bare name when no schema is set.
func (t Table) Qualified() string {
	if t.Schema == "" {
		return t.Name
	}
	return t.Schema + "." + t.Name
}

// SchemaOrPublic returns the table's schema, defaulting to "public".
func (t Table) SchemaOrPublic() string {
	if t.Schema == "" {
		return "public"
	}
	return t.Schema
}

// ColumnBinding maps one resource name pattern variable to its database column.
type ColumnBinding struct {
	// Variable is the snake_case pattern variable, e.g. "shelf".
	Variable string
	// Column is the database column holding the variable's ID, e.g. "shelf_id".
	Column string
	// Shared is true when the variable appears in every pattern of the
	// resource; non-shared variables are stored as nullable columns.
	Shared bool
}

// GoFieldName returns the Go struct field name of the binding, e.g. "ShelfID".
func (b ColumnBinding) GoFieldName() string {
	camelCase := xstrings.ToCamelCase(b.Variable)
	return strings.ToUpper(camelCase[:1]) + camelCase[1:] + "ID"
}

// ColumnBindings returns the database column bound to each variable of the
// pattern. The id_column_name override applies to the resource's own
// identifier — the final pattern variable — and is invalid on singletons,
// which have no identifier of their own.
func ColumnBindings(pattern *resource.ParsedPattern, modelOpts *modelpb.ModelOpts) ([]ColumnBinding, error) {
	idColumnName := modelOpts.GetIdColumnName()
	if idColumnName != "" && pattern.Singleton {
		return nil, fmt.Errorf("singleton resource %s declares an id_column_name", pattern.Resource.Desc.Type)
	}
	bindings := make([]ColumnBinding, len(pattern.Variables))
	for i, variable := range pattern.Variables {
		column := variable + "_id"
		if idColumnName != "" && i == len(pattern.Variables)-1 {
			column = idColumnName
		}
		bindings[i] = ColumnBinding{Variable: variable, Column: column, Shared: true}
	}
	return bindings, nil
}

// UnionColumnBindings returns the database column bound to each variable of
// the union of the resource's patterns. Variables shared by every pattern map
// to non-nullable columns; pattern-specific variables map to nullable columns.
func UnionColumnBindings(parsedResource *resource.ParsedResource, modelOpts *modelpb.ModelOpts) ([]ColumnBinding, error) {
	if len(parsedResource.Patterns) == 1 {
		return ColumnBindings(parsedResource.Patterns[0], modelOpts)
	}
	unionVariables, err := parsedResource.UnionVariables()
	if err != nil {
		return nil, err
	}
	idColumnName := modelOpts.GetIdColumnName()
	bindings := make([]ColumnBinding, len(unionVariables))
	for i, unionVariable := range unionVariables {
		column := unionVariable.Name + "_id"
		if idColumnName != "" && i == len(unionVariables)-1 {
			column = idColumnName
		}
		bindings[i] = ColumnBinding{Variable: unionVariable.Name, Column: column, Shared: unionVariable.Shared}
	}
	return bindings, nil
}

// Columns returns the column of each binding.
func Columns(bindings []ColumnBinding) []string {
	columns := make([]string, len(bindings))
	for i, binding := range bindings {
		columns[i] = binding.Column
	}
	return columns
}

// JoinTarget is the joined table and column backing a single joined field.
type JoinTarget struct {
	Table Table
	// Alias is the table alias the join is emitted under. Ancestor joins use
	// the table name; reference joins use the reference field name and query
	// joins their anchor field name, so two joins against the same table
	// cannot collide.
	Alias    string
	Column   string
	Nullable bool
	// Ancestor is true for ancestor joins, whose fields must mirror the
	// source column's nullability.
	Ancestor bool
}

// JoinCondition equates a column on the joined table with an expression on
// the child table.
type JoinCondition struct {
	// SourceColumn is the column on the joined table.
	SourceColumn string
	// ChildExpr is a printf template with a single %s placeholder for the
	// child table qualifier, e.g. "%s.organization_id" or
	// "split_part(%s.active_revision, '/', 12)".
	ChildExpr string
}

// Render returns the SQL condition, qualifying both sides.
func (c JoinCondition) Render(alias, childTable string) string {
	return fmt.Sprintf("%s.%s = %s", alias, c.SourceColumn, fmt.Sprintf(c.ChildExpr, childTable))
}

// JoinField is a child column populated from a joined table column.
type JoinField struct {
	// Alias is the column alias on the child, i.e. the proto field name.
	Alias string
	// Column is the backing column on the joined table.
	Column string
}

// Join groups all joined fields of a message against one join source.
type Join struct {
	// Key identifies the join source: the ancestor resource type, or the
	// reference/anchor field name.
	Key   string
	Table Table
	// Alias is the table alias the join is emitted under.
	Alias string
	// Left is true when the join is emitted as a LEFT JOIN: reference joins
	// on nullable fields, and every query join.
	Left       bool
	Conditions []JoinCondition
	Fields     []JoinField
	// Query marks a descendant join: emitted as a LEFT JOIN LATERAL
	// selecting at most one correlated child row.
	Query *JoinQuery
}

// JoinQuery is the resolved SQL of a query join's row selection over the
// child table. Filter, OrderBy and NameExpr are qualified by Inner, the alias
// the child table is emitted under inside the lateral subquery: its own bare
// table name.
type JoinQuery struct {
	Inner    string
	Filter   string
	OrderBy  string
	NameExpr string
}

// joinSource bundles everything resolvable from a join's source resource type.
type joinSource struct {
	message   *protogen.Message
	modelOpts *modelpb.ModelOpts
	resource  *resource.ParsedResource
}

func resolveJoinSource(sourceType string) (*joinSource, error) {
	sourceMessage, err := resource.GetMessageByResourceType(sourceType)
	if err != nil {
		return nil, fmt.Errorf("resolving join source resource %q: %w", sourceType, err)
	}
	sourceModelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](sourceMessage.Desc.Options(), modelpb.E_ModelOpts)
	if err != nil {
		return nil, fmt.Errorf("getting model_opts for join source %q: %w", sourceType, err)
	}
	sourceResource, err := resource.ParseFromMessage(sourceMessage)
	if err != nil {
		return nil, fmt.Errorf("parsing join source resource %q: %w", sourceType, err)
	}
	return &joinSource{message: sourceMessage, modelOpts: sourceModelOpts, resource: sourceResource}, nil
}

// resolveReferenceSource resolves the resource referenced by a resource-name
// field via its google.api.resource_reference annotation.
func resolveReferenceSource(referenceField *protogen.Field) (*joinSource, error) {
	name := referenceField.Desc.TextName()
	if referenceField.Desc.Kind() != protoreflect.StringKind || referenceField.Desc.IsList() {
		return nil, fmt.Errorf("reference field %q must be a singular string", name)
	}
	options := referenceField.Desc.Options()
	if options == nil || !proto.HasExtension(options, annotationspb.E_ResourceReference) {
		return nil, fmt.Errorf("reference field %q must declare a google.api.resource_reference", name)
	}
	reference, ok := proto.GetExtension(options, annotationspb.E_ResourceReference).(*annotationspb.ResourceReference)
	if !ok || reference.GetType() == "" || reference.GetType() == "*" {
		return nil, fmt.Errorf("reference field %q must declare a concrete resource_reference type", name)
	}
	return resolveJoinSource(reference.GetType())
}

func fieldByTextName(message *protogen.Message, name string) *protogen.Field {
	for _, field := range message.Fields {
		if field.Desc.TextName() == name {
			return field
		}
	}
	return nil
}

// SetFilesRegistry provides the file registry backing query-join filter
// transpilation. It must be set once before joins are resolved.
func SetFilesRegistry(registry *protoregistry.Files) { filesRegistry = registry }

var filesRegistry *protoregistry.Files

// joinKind discriminates the three join shapes.
type joinKind int

const (
	// ancestorJoin joins the containing row of an ancestor resource.
	ancestorJoin joinKind = iota
	// referenceJoin joins the row named by a stored resource-name field.
	referenceJoin
	// queryJoin selects at most one correlated descendant row.
	queryJoin
)

func joinKindOf(join *modelpb.Join) (joinKind, error) {
	if join.GetResourceType() == "" {
		return 0, fmt.Errorf("join must declare a resource_type")
	}
	if join.GetField() == "" {
		return 0, fmt.Errorf("join must declare a field")
	}
	switch {
	case join.GetReference() != "":
		return referenceJoin, nil
	case join.GetQuery() != nil:
		return queryJoin, nil
	default:
		return ancestorJoin, nil
	}
}

// anchorField returns the query join annotated on a field, if any: such
// fields anchor a lateral join that reference joins can chain onto.
func anchorField(field *protogen.Field) (*modelpb.Join, error) {
	fieldOpts, err := pbutil.GetExtension[*modelpb.FieldOpts](field.Desc.Options(), modelpb.E_FieldOpts)
	if err != nil {
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			return nil, nil
		}
		return nil, err
	}
	join := fieldOpts.GetJoin()
	if join.GetQuery() == nil {
		return nil, nil
	}
	return join, nil
}

// ResolveJoin resolves the joined table, alias and column of a join-annotated field.
func ResolveJoin(message *protogen.Message, field *protogen.Field, join *modelpb.Join) (*JoinTarget, error) {
	kind, err := joinKindOf(join)
	if err != nil {
		return nil, err
	}

	var source *joinSource
	var alias string
	switch kind {
	case ancestorJoin:
		if source, err = resolveJoinSource(join.GetResourceType()); err != nil {
			return nil, err
		}
		alias = TableOf(source.resource, source.modelOpts).Name
	case queryJoin:
		// The anchor projects the child's name, reconstructed inside the
		// lateral subquery, and lends its own field name as the join alias.
		if join.GetField() != "name" {
			return nil, fmt.Errorf("query join field must be %q, got %q: chain other fields via reference", "name", join.GetField())
		}
		if source, err = resolveJoinSource(join.GetResourceType()); err != nil {
			return nil, err
		}
		return &JoinTarget{
			Table:    TableOf(source.resource, source.modelOpts),
			Alias:    field.Desc.TextName(),
			Column:   "name",
			Nullable: true,
		}, nil
	case referenceJoin:
		referenceField := fieldByTextName(message, join.GetReference())
		if referenceField == nil {
			return nil, fmt.Errorf("reference field %q not found on %s", join.GetReference(), message.Desc.FullName())
		}
		anchor, err := anchorField(referenceField)
		if err != nil {
			return nil, err
		}
		if anchor != nil {
			// A reference onto a query join's anchor collapses into the
			// anchor's lateral join.
			if anchor.GetResourceType() != join.GetResourceType() {
				return nil, fmt.Errorf("reference %q resolves to resource %q, not %q", join.GetReference(), anchor.GetResourceType(), join.GetResourceType())
			}
			source, err = resolveJoinSource(join.GetResourceType())
		} else {
			source, err = resolveReferenceSource(referenceField)
		}
		if err != nil {
			return nil, err
		}
		if source.resource.Desc.GetType() != join.GetResourceType() {
			return nil, fmt.Errorf("reference %q resolves to resource %q, not %q", join.GetReference(), source.resource.Desc.GetType(), join.GetResourceType())
		}
		alias = join.GetReference()
	}

	if join.GetField() == "name" {
		return nil, fmt.Errorf("field %q is only joinable via a query join, whose lateral subquery reconstructs it", "name")
	}
	for _, sourceField := range source.message.Fields {
		if sourceField.Desc.TextName() != join.GetField() {
			continue
		}
		sourceFieldOpts, err := pbutil.GetExtension[*modelpb.FieldOpts](sourceField.Desc.Options(), modelpb.E_FieldOpts)
		if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
			return nil, fmt.Errorf("getting field_opts for joined field %q: %w", join.GetField(), err)
		}
		column := join.GetField()
		if sourceFieldOpts.GetColumnName() != "" {
			column = sourceFieldOpts.GetColumnName()
		}
		return &JoinTarget{
			Table:    TableOf(source.resource, source.modelOpts),
			Alias:    alias,
			Column:   column,
			Nullable: sourceFieldOpts.GetNullable(),
			Ancestor: kind == ancestorJoin,
		}, nil
	}
	return nil, fmt.Errorf("field %q not found on joined resource %q", join.GetField(), source.resource.Desc.Type)
}

// ParseJoins collects a message's join annotations, grouped by join source in
// field-declaration order. Reference joins chained onto a query join's anchor
// field share the anchor's lateral join.
func ParseJoins(message *protogen.Message) ([]Join, error) {
	var keys []string
	keyToJoin := map[string]*Join{}

	for _, field := range message.Fields {
		fieldOpts, err := pbutil.GetExtension[*modelpb.FieldOpts](field.Desc.Options(), modelpb.E_FieldOpts)
		if err != nil {
			if errors.Is(err, pbutil.ErrExtensionNotFound) {
				continue
			}
			return nil, fmt.Errorf("getting field_opts for %s: %w", field.Desc.Name(), err)
		}
		joinOpts := fieldOpts.GetJoin()
		if joinOpts == nil {
			continue
		}

		target, err := ResolveJoin(message, field, joinOpts)
		if err != nil {
			return nil, fmt.Errorf("resolving join for field %s: %w", field.Desc.Name(), err)
		}
		kind, err := joinKindOf(joinOpts)
		if err != nil {
			return nil, err
		}

		key := joinKey(field, joinOpts)
		join, ok := keyToJoin[key]
		if !ok {
			join = &Join{Key: key, Table: target.Table, Alias: target.Alias}
			switch kind {
			case ancestorJoin:
				join.Conditions, err = ancestorJoinConditions(joinOpts.GetResourceType())
			case queryJoin:
				join.Left = true
				join.Conditions, err = descendantJoinConditions(message, joinOpts.GetResourceType())
				if err == nil {
					join.Query, err = buildJoinQuery(message, joinOpts)
				}
			case referenceJoin:
				referenceField := fieldByTextName(message, joinOpts.GetReference())
				anchor, anchorErr := anchorField(referenceField)
				if anchorErr != nil {
					return nil, anchorErr
				}
				if anchor != nil {
					// The anchor field declares the group; a chained
					// reference encountered first is an ordering error.
					return nil, fmt.Errorf("field %s references query join anchor %q, which must be declared before it", field.Desc.Name(), joinOpts.GetReference())
				}
				join.Conditions, err = referenceJoinConditions(message, joinOpts.GetReference())
				join.Left = fieldOpts.GetNullable()
			}
			if err != nil {
				return nil, err
			}
			keyToJoin[key] = join
			keys = append(keys, key)
		}
		// One reference or query join is a single LEFT/INNER decision, so all
		// of its fields must agree on nullability.
		if kind != ancestorJoin && join.Left != fieldOpts.GetNullable() {
			return nil, fmt.Errorf("field %s joined via %q must have nullable=%v", field.Desc.Name(), join.Alias, join.Left)
		}
		join.Fields = append(join.Fields, JoinField{Alias: string(field.Desc.Name()), Column: target.Column})
	}

	joins := make([]Join, 0, len(keys))
	for _, key := range keys {
		joins = append(joins, *keyToJoin[key])
	}
	return joins, nil
}

// joinKey identifies a field's join group: query anchors and the references
// chained onto them share the anchor field's key.
func joinKey(field *protogen.Field, join *modelpb.Join) string {
	switch {
	case join.GetReference() != "":
		return "reference:" + join.GetReference()
	case join.GetQuery() != nil:
		return "reference:" + field.Desc.TextName()
	}
	return "ancestor:" + join.GetResourceType()
}

// descendantJoinConditions equates each of this resource's identifier columns
// with the descendant's corresponding foreign key column, correlating the
// lateral subquery with the outer row.
func descendantJoinConditions(message *protogen.Message, descendantType string) ([]JoinCondition, error) {
	descendant, err := resolveJoinSource(descendantType)
	if err != nil {
		return nil, err
	}
	ownResource, err := resource.ParseFromMessage(message)
	if err != nil {
		return nil, err
	}
	ownModelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](message.Desc.Options(), modelpb.E_ModelOpts)
	if err != nil {
		return nil, fmt.Errorf("getting model_opts for %s: %w", message.Desc.FullName(), err)
	}
	ownPattern, err := ownResource.SinglePattern()
	if err != nil {
		return nil, err
	}
	descendantPattern, err := descendant.resource.SinglePattern()
	if err != nil {
		return nil, fmt.Errorf("query join on %q: %w", descendantType, err)
	}
	if !strings.HasPrefix(descendantPattern.Value, ownPattern.Value+"/") {
		return nil, fmt.Errorf("query join resource pattern %q does not extend %q", descendantPattern.Value, ownPattern.Value)
	}

	ownBindings, err := ColumnBindings(ownPattern, ownModelOpts)
	if err != nil {
		return nil, err
	}
	descendantBindings, err := ColumnBindings(descendantPattern, descendant.modelOpts)
	if err != nil {
		return nil, err
	}
	descendantColumns := make(map[string]string, len(descendantBindings))
	for _, binding := range descendantBindings {
		descendantColumns[binding.Variable] = binding.Column
	}

	conditions := make([]JoinCondition, 0, len(ownBindings))
	for _, ownBinding := range ownBindings {
		descendantColumn, ok := descendantColumns[ownBinding.Variable]
		if !ok {
			return nil, fmt.Errorf("descendant %q has no identifier column for variable %q", descendantType, ownBinding.Variable)
		}
		conditions = append(conditions, JoinCondition{SourceColumn: descendantColumn, ChildExpr: "%s." + ownBinding.Column})
	}
	return conditions, nil
}

// buildJoinQuery transpiles a query join's filter and order_by against the
// child resource, and composes the child's name expression, all qualified by
// the child's own bare table name — the alias it takes inside the lateral.
func buildJoinQuery(message *protogen.Message, join *modelpb.Join) (*JoinQuery, error) {
	child, err := resolveJoinSource(join.GetResourceType())
	if err != nil {
		return nil, err
	}
	childTable := TableOf(child.resource, child.modelOpts)
	ownResource, err := resource.ParseFromMessage(message)
	if err != nil {
		return nil, err
	}
	ownModelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](message.Desc.Options(), modelpb.E_ModelOpts)
	if err != nil {
		return nil, fmt.Errorf("getting model_opts for %s: %w", message.Desc.FullName(), err)
	}
	// The lateral correlates on the outer table by bare name: a shared table
	// would make those references ambiguous.
	if ownTable := TableOf(ownResource, ownModelOpts); ownTable.Name == childTable.Name {
		return nil, fmt.Errorf("query join on %q shares this resource's table %q", join.GetResourceType(), ownTable.Name)
	}

	if join.GetQuery().GetOrderBy() == "" {
		return nil, fmt.Errorf("query join on %q must declare an order_by", join.GetResourceType())
	}
	transpiler, err := static.NewTranspiler(child.message.Desc, static.WithRegistry(filesRegistry))
	if err != nil {
		return nil, fmt.Errorf("building transpiler for %q: %w", join.GetResourceType(), err)
	}
	filter, err := transpiler.TranspileFilter(join.GetQuery().GetFilter())
	if err != nil {
		return nil, fmt.Errorf("query join filter on %q: %w", join.GetResourceType(), err)
	}
	orderBy, err := transpiler.TranspileOrderBy(join.GetQuery().GetOrderBy())
	if err != nil {
		return nil, fmt.Errorf("query join order_by on %q: %w", join.GetResourceType(), err)
	}

	childPattern, err := child.resource.SinglePattern()
	if err != nil {
		return nil, err
	}
	childBindings, err := ColumnBindings(childPattern, child.modelOpts)
	if err != nil {
		return nil, err
	}
	return &JoinQuery{
		Inner:    childTable.Name,
		Filter:   filter,
		OrderBy:  orderBy,
		NameExpr: resourceNameExpr(childPattern, childBindings, childTable.Name),
	}, nil
}

// resourceNameExpr composes the SQL expression reconstructing a resource's
// name from its identifier columns, e.g.
// "'shelves/' || shelf.shelf_id || '/books/' || book.book_id".
func resourceNameExpr(pattern *resource.ParsedPattern, bindings []ColumnBinding, qualifier string) string {
	variableToColumn := make(map[string]string, len(bindings))
	for _, binding := range bindings {
		variableToColumn[binding.Variable] = binding.Column
	}
	var parts []string
	for _, segment := range strings.Split(pattern.Value, "/") {
		if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			variable := segment[1 : len(segment)-1]
			parts = append(parts, qualifier+"."+variableToColumn[variable])
			continue
		}
		if len(parts) == 0 {
			parts = append(parts, "'"+segment+"/'")
			continue
		}
		parts = append(parts, "'/"+segment+"/'")
	}
	// Merge adjacent literals produced by consecutive literal segments.
	return strings.Join(parts, " || ")
}

// ancestorJoinConditions equates each of the ancestor's identifier columns
// with this resource's corresponding foreign key column.
func ancestorJoinConditions(ancestorType string) ([]JoinCondition, error) {
	parent, err := resolveJoinSource(ancestorType)
	if err != nil {
		return nil, err
	}
	parentPattern, err := parent.resource.SinglePattern()
	if err != nil {
		return nil, fmt.Errorf("ancestor join %q: %w", ancestorType, err)
	}
	parentBindings, err := ColumnBindings(parentPattern, parent.modelOpts)
	if err != nil {
		return nil, err
	}
	conditions := make([]JoinCondition, len(parentBindings))
	for i, binding := range parentBindings {
		conditions[i] = JoinCondition{SourceColumn: binding.Column, ChildExpr: "%s." + binding.Variable + "_id"}
	}
	return conditions, nil
}

// referenceJoinConditions equates the shared identifier columns of the
// referencing and referenced resources, and extracts the referenced
// resource's own identifier from the stored resource name.
func referenceJoinConditions(message *protogen.Message, referenceFieldName string) ([]JoinCondition, error) {
	referenceField := fieldByTextName(message, referenceFieldName)
	if referenceField == nil {
		return nil, fmt.Errorf("reference field %q not found on %s", referenceFieldName, message.Desc.FullName())
	}
	source, err := resolveReferenceSource(referenceField)
	if err != nil {
		return nil, err
	}

	ownModelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](message.Desc.Options(), modelpb.E_ModelOpts)
	if err != nil {
		return nil, fmt.Errorf("getting model_opts for %s: %w", message.Desc.FullName(), err)
	}
	ownResource, err := resource.ParseFromMessage(message)
	if err != nil {
		return nil, err
	}
	ownPattern, err := ownResource.SinglePattern()
	if err != nil {
		return nil, err
	}
	sourcePattern, err := source.resource.SinglePattern()
	if err != nil {
		return nil, fmt.Errorf("join reference %q: %w", referenceFieldName, err)
	}
	if sourcePattern.Singleton {
		return nil, fmt.Errorf("join reference %q targets singleton resource %s", referenceFieldName, source.resource.Desc.Type)
	}
	if !strings.HasPrefix(sourcePattern.Value, ownPattern.Value+"/") {
		return nil, fmt.Errorf("referenced resource pattern %q does not extend %q", sourcePattern.Value, ownPattern.Value)
	}

	ownBindings, err := ColumnBindings(ownPattern, ownModelOpts)
	if err != nil {
		return nil, err
	}
	variableToOwnColumn := make(map[string]string, len(ownBindings))
	for _, binding := range ownBindings {
		variableToOwnColumn[binding.Variable] = binding.Column
	}
	sourceBindings, err := ColumnBindings(sourcePattern, source.modelOpts)
	if err != nil {
		return nil, err
	}

	referenceFieldOpts, err := pbutil.GetExtension[*modelpb.FieldOpts](referenceField.Desc.Options(), modelpb.E_FieldOpts)
	if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
		return nil, fmt.Errorf("getting field_opts for reference field %q: %w", referenceFieldName, err)
	}
	referenceColumn := referenceField.Desc.TextName()
	if referenceFieldOpts.GetColumnName() != "" {
		referenceColumn = referenceFieldOpts.GetColumnName()
	}

	conditions := make([]JoinCondition, 0, len(sourceBindings))
	for i, sourceBinding := range sourceBindings {
		// The referenced resource's own identifier is the last segment of the
		// stored name; the segment index is static from the pattern. An empty
		// reference extracts "" and matches nothing.
		if i == len(sourceBindings)-1 {
			segmentIndex := strings.Count(sourcePattern.Value, "/") + 1
			conditions = append(conditions, JoinCondition{
				SourceColumn: sourceBinding.Column,
				ChildExpr:    fmt.Sprintf("split_part(%%s.%s, '/', %d)", referenceColumn, segmentIndex),
			})
			continue
		}
		ownColumn, ok := variableToOwnColumn[sourceBinding.Variable]
		if !ok {
			return nil, fmt.Errorf("referenced resource variable %q has no identifier column on %s", sourceBinding.Variable, message.Desc.FullName())
		}
		conditions = append(conditions, JoinCondition{SourceColumn: sourceBinding.Column, ChildExpr: "%s." + ownColumn})
	}
	return conditions, nil
}

// SingletonChild is a persisted singleton child resource, created and deleted
// alongside its parent.
type SingletonChild struct {
	Resource *resource.ParsedResource
	// Pattern is the child's singleton pattern parented by the resource it was
	// resolved against. Its variables are exactly the parent's identifiers.
	Pattern   *resource.ParsedPattern
	Message   *protogen.Message
	ModelOpts *modelpb.ModelOpts
}

// Table resolves the child's table.
func (c SingletonChild) Table() Table {
	return TableOf(c.Resource, c.ModelOpts)
}

// SingletonChildren returns the persisted singleton children of a resource.
// Children without model options are not persisted and are skipped; any other
// resolution failure — or a soft-deletability mismatch with the parent — is an error.
func SingletonChildren(parentMessage *protogen.Message, parent *resource.ParsedResource) ([]SingletonChild, error) {
	parentSoftDeletable := parentMessage.Desc.Fields().ByName("delete_time") != nil
	var children []SingletonChild
	for _, childResource := range parent.Children {
		childPattern := singletonPatternUnder(childResource, parent)
		if childPattern == nil {
			continue
		}
		childMessage, err := resource.GetMessageByResourceType(childResource.Desc.Type)
		if err != nil {
			return nil, fmt.Errorf("resolving message for singleton child %s: %w", childResource.Desc.Type, err)
		}
		childModelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](childMessage.Desc.Options(), modelpb.E_ModelOpts)
		if err != nil {
			if errors.Is(err, pbutil.ErrExtensionNotFound) {
				continue
			}
			return nil, fmt.Errorf("getting model_opts for singleton child %s: %w", childResource.Desc.Type, err)
		}
		childSoftDeletable := childMessage.Desc.Fields().ByName("delete_time") != nil
		if parentSoftDeletable != childSoftDeletable {
			if parentSoftDeletable {
				return nil, fmt.Errorf("singleton child %s must have a delete_time field because its parent %s is soft-deletable", childResource.Desc.Type, parent.Desc.Type)
			}
			return nil, fmt.Errorf("singleton child %s has a delete_time field but its parent %s is not soft-deletable", childResource.Desc.Type, parent.Desc.Type)
		}
		children = append(children, SingletonChild{
			Resource:  childResource,
			Pattern:   childPattern,
			Message:   childMessage,
			ModelOpts: childModelOpts,
		})
	}
	return children, nil
}

// singletonPatternUnder returns the child's singleton pattern parented by the
// given resource, if any.
func singletonPatternUnder(child, parent *resource.ParsedResource) *resource.ParsedPattern {
	for _, pattern := range child.Patterns {
		if pattern.Singleton && pattern.Parent != nil && pattern.Parent.Resource.Desc.GetType() == parent.Desc.GetType() {
			return pattern
		}
	}
	return nil
}

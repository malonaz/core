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

	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
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
	// Alias is the table alias the join is emitted under. Parent joins use
	// the table name; reference joins use the reference field name so two
	// joins against the same table cannot collide.
	Alias    string
	Column   string
	Nullable bool
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
	// Key identifies the join source: the parent resource type or the
	// reference field name.
	Key   string
	Table Table
	// Alias is the table alias the join is emitted under.
	Alias string
	// Left is true when the join is emitted as a LEFT JOIN: reference joins
	// on nullable fields.
	Left       bool
	Conditions []JoinCondition
	Fields     []JoinField
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

// ResolveJoin resolves the joined table, alias and column of a join annotation.
func ResolveJoin(message *protogen.Message, join *modelpb.Join) (*JoinTarget, error) {
	var source *joinSource
	var alias string
	switch {
	case join.GetParent() != "":
		var err error
		if source, err = resolveJoinSource(join.GetParent()); err != nil {
			return nil, err
		}
		alias = TableOf(source.resource, source.modelOpts).Name
	case join.GetReference() != "":
		referenceField := fieldByTextName(message, join.GetReference())
		if referenceField == nil {
			return nil, fmt.Errorf("reference field %q not found on %s", join.GetReference(), message.Desc.FullName())
		}
		var err error
		if source, err = resolveReferenceSource(referenceField); err != nil {
			return nil, err
		}
		alias = join.GetReference()
	default:
		return nil, fmt.Errorf("join must set exactly one of parent or reference")
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
		}, nil
	}
	return nil, fmt.Errorf("field %q not found on joined resource %q", join.GetField(), source.resource.Desc.Type)
}

// ParseJoins collects a message's join annotations, grouped by join source in
// field-declaration order.
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

		target, err := ResolveJoin(message, joinOpts)
		if err != nil {
			return nil, fmt.Errorf("resolving join for field %s: %w", field.Desc.Name(), err)
		}

		key := joinKey(joinOpts)
		join, ok := keyToJoin[key]
		if !ok {
			join = &Join{Key: key, Table: target.Table, Alias: target.Alias}
			switch {
			case joinOpts.GetParent() != "":
				join.Conditions, err = parentJoinConditions(joinOpts.GetParent())
			case joinOpts.GetReference() != "":
				join.Conditions, err = referenceJoinConditions(message, joinOpts.GetReference())
				join.Left = fieldOpts.GetNullable()
			}
			if err != nil {
				return nil, err
			}
			keyToJoin[key] = join
			keys = append(keys, key)
		}
		// One reference join is a single LEFT/INNER decision, so all of its
		// fields must agree on nullability.
		if joinOpts.GetReference() != "" && join.Left != fieldOpts.GetNullable() {
			return nil, fmt.Errorf("fields joined via reference %q disagree on nullable", joinOpts.GetReference())
		}
		join.Fields = append(join.Fields, JoinField{Alias: string(field.Desc.Name()), Column: target.Column})
	}

	joins := make([]Join, 0, len(keys))
	for _, key := range keys {
		joins = append(joins, *keyToJoin[key])
	}
	return joins, nil
}

func joinKey(join *modelpb.Join) string {
	if join.GetParent() != "" {
		return "parent:" + join.GetParent()
	}
	return "reference:" + join.GetReference()
}

// parentJoinConditions equates each of the parent's identifier columns with
// the child's corresponding foreign key column.
func parentJoinConditions(parentType string) ([]JoinCondition, error) {
	parent, err := resolveJoinSource(parentType)
	if err != nil {
		return nil, err
	}
	parentPattern, err := parent.resource.SinglePattern()
	if err != nil {
		return nil, fmt.Errorf("join parent %q: %w", parentType, err)
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

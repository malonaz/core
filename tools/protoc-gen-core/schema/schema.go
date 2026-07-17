// Package schema resolves the database mapping of proto resources: tables,
// identifier columns, parent joins and singleton children. It is the single
// source of truth shared by the model, postgres and rpc generators.
package schema

import (
	"errors"
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"

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
}

// GoFieldName returns the Go struct field name of the binding, e.g. "ShelfID".
func (b ColumnBinding) GoFieldName() string {
	camelCase := xstrings.ToCamelCase(b.Variable)
	return strings.ToUpper(camelCase[:1]) + camelCase[1:] + "ID"
}

// ColumnBindings returns the database column bound to each pattern variable of
// the resource. The id_column_name override applies to the resource's own
// identifier — the final pattern variable — and is invalid on singletons,
// which have no identifier of their own.
func ColumnBindings(parsedResource *resource.ParsedResource, modelOpts *modelpb.ModelOpts) ([]ColumnBinding, error) {
	idColumnName := modelOpts.GetIdColumnName()
	if idColumnName != "" && parsedResource.Singleton {
		return nil, fmt.Errorf("singleton resource %s declares an id_column_name", parsedResource.Desc.Type)
	}
	bindings := make([]ColumnBinding, len(parsedResource.PatternVariables))
	for i, variable := range parsedResource.PatternVariables {
		column := variable + "_id"
		if idColumnName != "" && i == len(parsedResource.PatternVariables)-1 {
			column = idColumnName
		}
		bindings[i] = ColumnBinding{Variable: variable, Column: column}
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

// JoinTarget is the parent table and column backing a single joined field.
type JoinTarget struct {
	Table    Table
	Column   string
	Nullable bool
}

// JoinCondition equates a parent table column with a child table column.
type JoinCondition struct {
	ParentColumn string
	ChildColumn  string
}

// JoinField is a child column populated from a parent table column.
type JoinField struct {
	// Alias is the column alias on the child, i.e. the proto field name.
	Alias string
	// Column is the backing column on the parent table.
	Column string
}

// Join groups all joined fields of a message against one parent resource.
type Join struct {
	ParentType string
	Table      Table
	Conditions []JoinCondition
	Fields     []JoinField
}

// joinParent bundles everything resolvable from a join's parent resource type.
type joinParent struct {
	message   *protogen.Message
	modelOpts *modelpb.ModelOpts
	resource  *resource.ParsedResource
}

func resolveJoinParent(parentType string) (*joinParent, error) {
	parentMessage, err := resource.GetMessageByResourceType(parentType)
	if err != nil {
		return nil, fmt.Errorf("resolving join parent resource %q: %w", parentType, err)
	}
	parentModelOpts, err := pbutil.GetExtension[*modelpb.ModelOpts](parentMessage.Desc.Options(), modelpb.E_ModelOpts)
	if err != nil {
		return nil, fmt.Errorf("getting model_opts for join parent %q: %w", parentType, err)
	}
	parentResource, err := resource.ParseFromMessage(parentMessage)
	if err != nil {
		return nil, fmt.Errorf("parsing join parent resource %q: %w", parentType, err)
	}
	return &joinParent{message: parentMessage, modelOpts: parentModelOpts, resource: parentResource}, nil
}

// ResolveJoin resolves the parent table and column of a join annotation.
func ResolveJoin(join *modelpb.Join) (*JoinTarget, error) {
	parent, err := resolveJoinParent(join.GetParent())
	if err != nil {
		return nil, err
	}
	for _, parentField := range parent.message.Fields {
		if parentField.Desc.TextName() != join.GetField() {
			continue
		}
		parentFieldOpts, err := pbutil.GetExtension[*modelpb.FieldOpts](parentField.Desc.Options(), modelpb.E_FieldOpts)
		if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
			return nil, fmt.Errorf("getting field_opts for parent field %q: %w", join.GetField(), err)
		}
		column := join.GetField()
		if parentFieldOpts.GetColumnName() != "" {
			column = parentFieldOpts.GetColumnName()
		}
		return &JoinTarget{
			Table:    TableOf(parent.resource, parent.modelOpts),
			Column:   column,
			Nullable: parentFieldOpts.GetNullable(),
		}, nil
	}
	return nil, fmt.Errorf("field %q not found on parent resource %q", join.GetField(), join.GetParent())
}

// ParseJoins collects a message's join annotations, grouped by parent resource
// in field-declaration order.
func ParseJoins(message *protogen.Message) ([]Join, error) {
	var parentTypes []string
	parentTypeToJoin := map[string]*Join{}

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

		target, err := ResolveJoin(joinOpts)
		if err != nil {
			return nil, fmt.Errorf("resolving join for field %s: %w", field.Desc.Name(), err)
		}

		join, ok := parentTypeToJoin[joinOpts.GetParent()]
		if !ok {
			conditions, err := joinConditions(joinOpts.GetParent())
			if err != nil {
				return nil, err
			}
			join = &Join{ParentType: joinOpts.GetParent(), Table: target.Table, Conditions: conditions}
			parentTypeToJoin[joinOpts.GetParent()] = join
			parentTypes = append(parentTypes, joinOpts.GetParent())
		}
		join.Fields = append(join.Fields, JoinField{Alias: string(field.Desc.Name()), Column: target.Column})
	}

	joins := make([]Join, 0, len(parentTypes))
	for _, parentType := range parentTypes {
		joins = append(joins, *parentTypeToJoin[parentType])
	}
	return joins, nil
}

// joinConditions equates each of the parent's identifier columns with the
// child's corresponding foreign key column.
func joinConditions(parentType string) ([]JoinCondition, error) {
	parent, err := resolveJoinParent(parentType)
	if err != nil {
		return nil, err
	}
	parentBindings, err := ColumnBindings(parent.resource, parent.modelOpts)
	if err != nil {
		return nil, err
	}
	conditions := make([]JoinCondition, len(parentBindings))
	for i, binding := range parentBindings {
		conditions[i] = JoinCondition{ParentColumn: binding.Column, ChildColumn: binding.Variable + "_id"}
	}
	return conditions, nil
}

// SingletonChild is a persisted singleton child resource, created and deleted
// alongside its parent.
type SingletonChild struct {
	Resource  *resource.ParsedResource
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
		if !childResource.Singleton {
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
		children = append(children, SingletonChild{Resource: childResource, Message: childMessage, ModelOpts: childModelOpts})
	}
	return children, nil
}

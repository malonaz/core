package pbjson

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	genuipb "github.com/malonaz/core/genproto/ai/genui/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
)

func fieldOf(t *testing.T, message protoreflect.ProtoMessage, fieldName string) protoreflect.FieldDescriptor {
	t.Helper()
	fieldDescriptor := message.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	require.NotNil(t, fieldDescriptor, "field %s not found", fieldName)
	return fieldDescriptor
}

func TestIsRequiredRule(t *testing.T) {
	t.Run("required field", func(t *testing.T) {
		fieldRules, err := getFieldRules(fieldOf(t, &genuipb.Text{}, "markdown"))
		require.NoError(t, err)
		require.True(t, isRequiredRule(fieldRules))
	})

	t.Run("unconstrained field", func(t *testing.T) {
		fieldRules, err := getFieldRules(fieldOf(t, &genuipb.KeyValueList{}, "title"))
		require.NoError(t, err)
		require.Nil(t, fieldRules)
		require.False(t, isRequiredRule(fieldRules))
	})
}

func TestApplyRepeatedRules(t *testing.T) {
	t.Run("min items only", func(t *testing.T) {
		fieldDescriptor, schema := fieldOf(t, &genuipb.KeyValueList{}, "items"), &jsonpb.Schema{}
		fieldRules, err := getFieldRules(fieldDescriptor)
		require.NoError(t, err)
		applyRepeatedRules(schema, fieldDescriptor, fieldRules)
		require.Equal(t, int32(1), schema.MinItems)
		require.Equal(t, int32(0), schema.MaxItems)
	})

	t.Run("min and max items", func(t *testing.T) {
		fieldDescriptor, schema := fieldOf(t, &genuipb.Choice{}, "options"), &jsonpb.Schema{}
		fieldRules, err := getFieldRules(fieldDescriptor)
		require.NoError(t, err)
		applyRepeatedRules(schema, fieldDescriptor, fieldRules)
		require.Equal(t, int32(2), schema.MinItems)
		require.Equal(t, int32(8), schema.MaxItems)
	})

	t.Run("item level rules land on items schema", func(t *testing.T) {
		fieldDescriptor := fieldOf(t, &genuipb.ResourceList{}, "resource_names")
		schema := &jsonpb.Schema{Items: &jsonpb.Schema{Type: "string"}}
		fieldRules, err := getFieldRules(fieldDescriptor)
		require.NoError(t, err)
		applyRepeatedRules(schema, fieldDescriptor, fieldRules)
		require.Equal(t, int32(1), schema.MinItems)
		require.Equal(t, int32(1), schema.Items.MinLength)
	})
}

func TestApplyEnumRules(t *testing.T) {
	t.Run("not_in drops unspecified", func(t *testing.T) {
		fieldDescriptor := fieldOf(t, &genuipb.Chart{}, "type")
		schema := &jsonpb.Schema{Type: "string", Enum: []string{"CHART_TYPE_UNSPECIFIED", "CHART_TYPE_BAR", "CHART_TYPE_LINE", "CHART_TYPE_PIE"}}
		fieldRules, err := getFieldRules(fieldDescriptor)
		require.NoError(t, err)
		applyEnumRules(schema, fieldDescriptor, fieldRules.GetEnum())
		require.Equal(t, []string{"CHART_TYPE_BAR", "CHART_TYPE_LINE", "CHART_TYPE_PIE"}, schema.Enum)
	})
}

func TestDescribeConstraints(t *testing.T) {
	t.Run("between", func(t *testing.T) {
		require.Equal(t, "Constraints: must contain between 2 and 8 items.", describeConstraints(&jsonpb.Schema{MinItems: 2, MaxItems: 8}))
	})

	t.Run("at least", func(t *testing.T) {
		require.Equal(t, "Constraints: must contain at least 1 item(s).", describeConstraints(&jsonpb.Schema{MinItems: 1}))
	})

	t.Run("unconstrained", func(t *testing.T) {
		require.Empty(t, describeConstraints(&jsonpb.Schema{}))
	})
}

func TestDescribeOneofs(t *testing.T) {
	t.Run("required oneof", func(t *testing.T) {
		description, err := describeOneofs((&genuipb.ActionRowAction{}).ProtoReflect().Descriptor())
		require.NoError(t, err)
		require.Contains(t, description, "Set exactly one of")
		require.Contains(t, description, "open_resource")
		require.Contains(t, description, "copy_text")
		require.Contains(t, description, "open_url")
	})

	t.Run("no oneof", func(t *testing.T) {
		description, err := describeOneofs((&genuipb.Text{}).ProtoReflect().Descriptor())
		require.NoError(t, err)
		require.Empty(t, description)
	})
}

func TestAppendDescription(t *testing.T) {
	require.Equal(t, "A. B.", appendDescription("A.", "B."))
	require.Equal(t, "B.", appendDescription("", "B."))
	require.Equal(t, "A.", appendDescription("A.", ""))
}

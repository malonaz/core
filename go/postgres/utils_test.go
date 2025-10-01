package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetDBColumns(t *testing.T) {
	type sample struct {
		B int    `db:"ya"`
		A int    `db:"yo"`
		C string `db:"bla"`
		D []string
		E int  `dbbb:"notThisOne"`
		f bool `db:"notThisOneEither"`
	}
	tags := GetDBColumns(sample{})
	require.Equal(t, []string{"ya", "yo", "bla"}, tags)
}

func GetNewNullString(t *testing.T) {
	t.Run("valid string", func(t *testing.T) {
		str := "validString"
		ns := NewNullString(str)
		require.True(t, ns.Valid)
		require.Equal(t, str, ns.String)
	})

	t.Run("invalid string", func(t *testing.T) {
		ns := NewNullString("")
		require.False(t, ns.Valid)
		require.Equal(t, "", ns.String)
	})
}

func TestGetDBColumnsEmbedded(t *testing.T) {
	type EmbeddedStruct struct {
		EmbeddedField1 string `db:"embedded_1"`
		EmbeddedField2 int    `db:"embedded_2"`
	}

	type ParentStruct struct {
		EmbeddedStruct
		ParentField1 int    `db:"parent_1"`
		ParentField2 string `db:"parent_2"`
		NonTagField  float64
	}

	t.Run("AllFieldsNoExceptions", func(t *testing.T) {
		obj := ParentStruct{}
		tags := GetDBColumns(obj)
		expectedTags := []string{"embedded_1", "embedded_2", "parent_1", "parent_2"}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("ExcludeEmbeddedFields", func(t *testing.T) {
		obj := ParentStruct{}
		tags := GetDBColumns(obj, "embedded_1", "embedded_2")
		expectedTags := []string{"parent_1", "parent_2"}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("ExcludeParentFields", func(t *testing.T) {
		obj := ParentStruct{}
		tags := GetDBColumns(obj, "parent_1", "parent_2")
		expectedTags := []string{"embedded_1", "embedded_2"}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("ExcludeAllFields", func(t *testing.T) {
		obj := ParentStruct{}
		tags := GetDBColumns(obj, "embedded_1", "embedded_2", "parent_1", "parent_2")
		expectedTags := []string{}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("ExcludeNonexistentField", func(t *testing.T) {
		obj := ParentStruct{}
		tags := GetDBColumns(obj, "nonexistent_field")
		expectedTags := []string{"embedded_1", "embedded_2", "parent_1", "parent_2"}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("ObjectWithNoDBTags", func(t *testing.T) {
		type NoTagStruct struct {
			Field1 string
			Field2 int
		}
		obj := NoTagStruct{}
		tags := GetDBColumns(obj)
		require.Empty(t, tags)
	})

	t.Run("MixedExcludeWithNonTaggedField", func(t *testing.T) {
		obj := ParentStruct{}
		tags := GetDBColumns(obj, "nonexistent_field", "NonTagField")
		expectedTags := []string{"embedded_1", "embedded_2", "parent_1", "parent_2"}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("NilPointerException", func(t *testing.T) {
		fn := func() { GetDBColumns(nil) }
		require.Panics(t, fn, "the function should panic when nil is passed")
	})

	t.Run("NonStructParameter", func(t *testing.T) {
		fn := func() { GetDBColumns(123) }
		require.Panics(t, fn, "the function should panic when non-struct parameter is passed")
	})
}

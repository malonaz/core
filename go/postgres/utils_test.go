package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetDBColumns(t *testing.T) {
	type sample struct {
		B int    `db:"public.my_table.ya" schema:"public" table:"my_table"`
		A int    `db:"public.my_table.yo" schema:"public" table:"my_table"`
		C string `db:"public.my_table.bla" schema:"public" table:"my_table"`
		D []string
		E int  `dbbb:"notThisOne"`
		f bool `db:"public.my_table.notThisOneEither" schema:"public" table:"my_table"`
	}
	tags := GetDBColumns(sample{})
	require.Equal(t, []string{"public.my_table.ya", "public.my_table.yo", "public.my_table.bla"}, tags)
}

func TestGetDBColumnsUnqualified(t *testing.T) {
	type sample struct {
		B int    `db:"library.shelf.ya" schema:"library" table:"shelf"`
		A int    `db:"library.shelf.yo" schema:"library" table:"shelf"`
		C string `db:"library.shelf.bla" schema:"library" table:"shelf"`
	}
	tags := GetDBColumns(sample{}, WithUnqualifiedColumns())
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
		EmbeddedField1 string `db:"public.parent.embedded_1" schema:"public" table:"parent"`
		EmbeddedField2 int    `db:"public.parent.embedded_2" schema:"public" table:"parent"`
	}

	type ParentStruct struct {
		EmbeddedStruct
		ParentField1 int    `db:"public.parent.parent_1" schema:"public" table:"parent"`
		ParentField2 string `db:"public.parent.parent_2" schema:"public" table:"parent"`
		NonTagField  float64
	}

	t.Run("AllFieldsNoExceptions", func(t *testing.T) {
		tags := GetDBColumns(ParentStruct{})
		expectedTags := []string{"public.parent.embedded_1", "public.parent.embedded_2", "public.parent.parent_1", "public.parent.parent_2"}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("ExcludeEmbeddedFields", func(t *testing.T) {
		tags := GetDBColumns(ParentStruct{}, ExceptColumns("public.parent.embedded_1", "public.parent.embedded_2"))
		expectedTags := []string{"public.parent.parent_1", "public.parent.parent_2"}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("ExcludeParentFields", func(t *testing.T) {
		tags := GetDBColumns(ParentStruct{}, ExceptColumns("public.parent.parent_1", "public.parent.parent_2"))
		expectedTags := []string{"public.parent.embedded_1", "public.parent.embedded_2"}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("ExcludeAllFields", func(t *testing.T) {
		tags := GetDBColumns(ParentStruct{}, ExceptColumns("public.parent.embedded_1", "public.parent.embedded_2", "public.parent.parent_1", "public.parent.parent_2"))
		expectedTags := []string{}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("ExcludeNonexistentField", func(t *testing.T) {
		tags := GetDBColumns(ParentStruct{}, ExceptColumns("nonexistent_field"))
		expectedTags := []string{"public.parent.embedded_1", "public.parent.embedded_2", "public.parent.parent_1", "public.parent.parent_2"}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("ObjectWithNoDBTags", func(t *testing.T) {
		type NoTagStruct struct {
			Field1 string
			Field2 int
		}
		tags := GetDBColumns(NoTagStruct{})
		require.Empty(t, tags)
	})

	t.Run("MixedExcludeWithNonTaggedField", func(t *testing.T) {
		tags := GetDBColumns(ParentStruct{}, ExceptColumns("nonexistent_field", "NonTagField"))
		expectedTags := []string{"public.parent.embedded_1", "public.parent.embedded_2", "public.parent.parent_1", "public.parent.parent_2"}
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

	t.Run("WithUnqualifiedColumns", func(t *testing.T) {
		tags := GetDBColumns(ParentStruct{}, WithUnqualifiedColumns())
		expectedTags := []string{"embedded_1", "embedded_2", "parent_1", "parent_2"}
		require.ElementsMatch(t, expectedTags, tags)
	})

	t.Run("WithUnqualifiedColumnsAndExcept", func(t *testing.T) {
		tags := GetDBColumns(ParentStruct{}, WithUnqualifiedColumns(), ExceptColumns("public.parent.parent_2"))
		expectedTags := []string{"embedded_1", "embedded_2", "parent_1"}
		require.ElementsMatch(t, expectedTags, tags)
	})
}

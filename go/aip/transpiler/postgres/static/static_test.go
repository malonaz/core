package static

import (
	"testing"

	"github.com/stretchr/testify/require"

	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

var (
	bookDescriptor  = (&librarypb.Book{}).ProtoReflect().Descriptor()
	shelfDescriptor = (&librarypb.Shelf{}).ProtoReflect().Descriptor()
)

// Books correlated with their shelf, as a shelf's descendant join sees them.
func TestTranspiler_Correlation(t *testing.T) {
	transpiler, err := NewTranspiler(bookDescriptor, WithCorrelation(shelfDescriptor))
	require.NoError(t, err)

	t.Run("Filter", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			filter string
			want   string
		}{
			{"ColumnAgainstCorrelatedColumn", "create_time > this.inventory_time", "(book.create_time > shelf.inventory_time)"},
			{"Presence", "NOT this.inventory_time:*", "(NOT (shelf.inventory_time IS NOT NULL))"},
			{"CustomColumnName", `this.external_id = "ext"`, "(shelf.ext_id = 'ext')"},
			{"Enum", "this.genre = SHELF_GENRE_FICTION", "(shelf.genre = 1)"},
			{"MixedWithOwnFields", "page_count > 0 AND create_time > this.create_time", "((book.page_count > 0) AND (book.create_time > shelf.create_time))"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := transpiler.TranspileFilter(tc.filter)
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	})

	// Only the correlated row's own stored scalar columns exist to a subquery.
	t.Run("FilterRejects", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			filter string
		}{
			{"UnknownField", "this.no_such_field > 0"},
			{"JoinedField", "this.total_page_count > 0"},
			{"JSONBField", "this.metadata.capacity > 0"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := transpiler.TranspileFilter(tc.filter)
				require.ErrorContains(t, err, "undeclared identifier 'this'")
			})
		}
	})

	t.Run("OrderByRejects", func(t *testing.T) {
		_, err := transpiler.TranspileOrderBy("this.inventory_time desc")
		require.ErrorContains(t, err, "not a stored scalar field")
	})
}

func TestTranspiler_WithoutCorrelation(t *testing.T) {
	transpiler, err := NewTranspiler(bookDescriptor)
	require.NoError(t, err)
	_, err = transpiler.TranspileFilter("create_time > this.inventory_time")
	require.ErrorContains(t, err, "undeclared identifier 'this'")
}

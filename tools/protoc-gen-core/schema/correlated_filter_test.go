package schema_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/descriptorpb"

	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

// Descendant join filters address the joining row's own columns as `this`,
// correlating the subquery on them.
func TestCorrelatedFilter_Generation(t *testing.T) {
	const shelf = "malonaz.test.library.v1.Shelf"
	const uninventoried = "((NOT (shelf.inventory_time IS NOT NULL)) OR (book.create_time > shelf.inventory_time))"

	t.Run("Resolved", func(t *testing.T) {
		joins, err := resolveJoins(t, shelf)
		require.NoError(t, err)
		aliasToJoin := map[string]schema.Join{}
		for _, join := range joins {
			aliasToJoin[join.Alias] = join
		}
		require.Equal(t, uninventoried, aliasToJoin["uninventoried_book_count"].Aggregate.Filter)
		require.Equal(t, uninventoried, aliasToJoin["oldest_uninventoried_book"].Query.Filter)
	})

	// A joined column is no column of the row: RETURNING could not read it.
	t.Run("JoinedFieldRejected", func(t *testing.T) {
		err := parseJoins(t, shelf, field{"recent", descriptorpb.FieldDescriptorProto_TYPE_INT64, "",
			aggregate(modelpb.Aggregate_FUNCTION_COUNT, "name", "create_time > this.last_book_create_time"), ""})
		require.ErrorContains(t, err, "undeclared identifier 'this'")
	})
}

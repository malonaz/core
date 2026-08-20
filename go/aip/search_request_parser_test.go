package aip

import (
	"testing"

	"github.com/stretchr/testify/require"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

// Book joins bookmark.color twice (latest_bookmark_color / first_bookmark_color)
// under different aliases. Without FQN both macro idents collapse to a bare
// `color` and enum ident redeclaration fails; WithFQN must keep them unique.
func TestSearchRequestParser_JoinedColumnCollision(t *testing.T) {
	t.Parallel()

	// Without FQN the colliding joined columns must fail parser construction.
	_, err := NewSearchRequestParser[*libraryservicepb.SearchBooksRequest, *librarypb.Book]()
	require.ErrorContains(t, err, "redeclaration of color")

	// WithFQN (what codegen emits) must construct.
	_, err = NewSearchRequestParser[*libraryservicepb.SearchBooksRequest, *librarypb.Book](WithFQN())
	require.NoError(t, err)
}

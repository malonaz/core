package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

// createTestBookWithPageCount creates a book with an explicit page count, to
// exercise the latest_book query join's filter (page_count > 0).
func createTestBookWithPageCount(t *testing.T, shelfName, authorName, title string, pageCount int32) *librarypb.Book {
	t.Helper()
	createBookRequest := &libraryservicepb.CreateBookRequest{
		Parent: shelfName,
		Book: &librarypb.Book{
			Title:           title,
			Author:          authorName,
			Isbn:            "978-0553293357",
			PublicationYear: 2000,
			PageCount:       pageCount,
			Duration:        durationpb.New(100 * time.Second),
			Metadata: &librarypb.BookMetadata{
				Summary:  "A test book.",
				Language: "en",
			},
		},
	}
	book, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
	require.NoError(t, err)
	return book
}

func TestQueryJoin_ShelfLatestBook(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "QueryJoin Author")

	t.Run("NoBooks_JoinedValuesUnset", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "QueryJoin Empty Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		require.Empty(t, shelf.LatestBook)
		require.Empty(t, shelf.LatestBookTitle)

		gotShelf := getShelf(t, shelf.Name)
		require.Empty(t, gotShelf.LatestBook)
		require.Empty(t, gotShelf.LatestBookTitle)
	})

	t.Run("LatestByCreateTimeWins", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "QueryJoin Latest Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBook(t, shelf.Name, author.Name, "QueryJoin First Book")
		secondBook := createTestBook(t, shelf.Name, author.Name, "QueryJoin Second Book")

		gotShelf := getShelf(t, shelf.Name)
		require.Equal(t, secondBook.Name, gotShelf.LatestBook)
		require.Equal(t, "QueryJoin Second Book", gotShelf.LatestBookTitle)
	})

	t.Run("FilterExcludesPagelessBooks", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "QueryJoin Filter Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, author.Name, "QueryJoin Paged Book")
		createTestBookWithPageCount(t, shelf.Name, author.Name, "QueryJoin Pageless Book", 0)

		gotShelf := getShelf(t, shelf.Name)
		require.Equal(t, book.Name, gotShelf.LatestBook)
		require.Equal(t, "QueryJoin Paged Book", gotShelf.LatestBookTitle)
	})

	t.Run("PopulatedOnList", func(t *testing.T) {
		t.Parallel()
		scopedOrganizationParent := getOrganizationParent()
		scopedAuthor := createTestAuthor(t, scopedOrganizationParent, "QueryJoin List Author")
		shelf := createTestShelf(t, scopedOrganizationParent, "QueryJoin List Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, scopedAuthor.Name, "QueryJoin List Book")

		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, &libraryservicepb.ListShelvesRequest{
			Parent: scopedOrganizationParent,
		})
		require.NoError(t, err)
		require.Len(t, listShelvesResponse.Shelves, 1)
		require.Equal(t, book.Name, listShelvesResponse.Shelves[0].LatestBook)
	})

	t.Run("FilterOnJoinedField", func(t *testing.T) {
		t.Parallel()
		scopedOrganizationParent := getOrganizationParent()
		scopedAuthor := createTestAuthor(t, scopedOrganizationParent, "QueryJoin FilterJoin Author")
		shelf := createTestShelf(t, scopedOrganizationParent, "QueryJoin FilterJoin Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBook(t, shelf.Name, scopedAuthor.Name, "QueryJoin FilterJoin Book")
		createTestShelf(t, scopedOrganizationParent, "QueryJoin FilterJoin Empty Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		// AIP-160 filtering on the lateral-joined column.
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, &libraryservicepb.ListShelvesRequest{
			Parent: scopedOrganizationParent,
			Filter: `latest_book_title = "QueryJoin FilterJoin Book"`,
		})
		require.NoError(t, err)
		require.Len(t, listShelvesResponse.Shelves, 1)
		require.Equal(t, shelf.Name, listShelvesResponse.Shelves[0].Name)
	})
}

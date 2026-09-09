package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

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

// createTestBookAt creates a book with a pinned id and create_time (via the
// migration header), so ordering ties can be manufactured deterministically.
func createTestBookAt(t *testing.T, shelfName, authorName, bookID, title string, createTime time.Time) *librarypb.Book {
	t.Helper()
	migrationCtx := metadata.AppendToOutgoingContext(ctx, "x-migration-request", "true")
	book, err := libraryServiceClient.CreateBook(migrationCtx, &libraryservicepb.CreateBookRequest{
		Parent: shelfName,
		BookId: bookID,
		Book: &librarypb.Book{
			Title:      title,
			Author:     authorName,
			PageCount:  10,
			Duration:   durationpb.New(time.Second),
			CreateTime: timestamppb.New(createTime),
			Metadata:   &librarypb.BookMetadata{},
		},
	})
	require.NoError(t, err)
	return book
}

// The latest_draft_book join filter is `title = "Draft*"`, transpiled to a
// LIKE pattern whose `%` is spliced into every query of the resource. Each
// read path must survive it and agree on the answer.
func TestQueryJoin_WildcardFilter(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "QueryJoin Wildcard Author")
	shelf := createTestShelf(t, organizationParent, "QueryJoin Wildcard Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	createTestBook(t, shelf.Name, author.Name, "Not a draft")
	draft := createTestBook(t, shelf.Name, author.Name, "Draft: chapter one")
	createTestBook(t, shelf.Name, author.Name, "Also not a draft")

	t.Run("Get", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, draft.Name, getShelf(t, shelf.Name).LatestDraftBook)
	})

	t.Run("List", func(t *testing.T) {
		t.Parallel()
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, &libraryservicepb.ListShelvesRequest{
			Parent: organizationParent,
			Filter: `latest_draft_book = "` + draft.Name + `"`,
		})
		require.NoError(t, err)
		require.Len(t, listShelvesResponse.Shelves, 1)
		require.Equal(t, draft.Name, listShelvesResponse.Shelves[0].LatestDraftBook)
	})

	t.Run("BatchGet", func(t *testing.T) {
		t.Parallel()
		batchGetShelvesResponse, err := libraryServiceClient.BatchGetShelves(ctx, &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{shelf.Name},
		})
		require.NoError(t, err)
		require.Len(t, batchGetShelvesResponse.Shelves, 1)
		require.Equal(t, draft.Name, batchGetShelvesResponse.Shelves[0].LatestDraftBook)
	})

	// Update and Create resolve the join through RETURNING subqueries rather
	// than the lateral join.
	t.Run("Update", func(t *testing.T) {
		t.Parallel()
		updated := updateShelf(t, &librarypb.Shelf{Name: shelf.Name, DisplayName: "QueryJoin Wildcard Shelf Renamed"}, []string{"display_name"})
		require.Equal(t, draft.Name, updated.LatestDraftBook)
	})

	t.Run("Create", func(t *testing.T) {
		t.Parallel()
		created := createTestShelf(t, organizationParent, "QueryJoin Wildcard Fresh Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		require.Empty(t, created.LatestDraftBook)
	})
}

// A query join orders by the declared order_by, then by the joined resource's
// id: ties must resolve to the same row in the lateral join (Get/List) and in
// every RETURNING subquery (Create/Update), or latest_book and
// latest_book_title would name different books.
func TestQueryJoin_TieBreak(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "QueryJoin Tie Author")
	shelf := createTestShelf(t, organizationParent, "QueryJoin Tie Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	createTime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	// Created out of id order so insertion order cannot be what decides.
	createTestBookAt(t, shelf.Name, author.Name, "tie-b", "Tie B", createTime)
	first := createTestBookAt(t, shelf.Name, author.Name, "tie-a", "Tie A", createTime)
	createTestBookAt(t, shelf.Name, author.Name, "tie-c", "Tie C", createTime)

	t.Run("Get", func(t *testing.T) {
		t.Parallel()
		gotShelf := getShelf(t, shelf.Name)
		require.Equal(t, first.Name, gotShelf.LatestBook)
		require.Equal(t, "Tie A", gotShelf.LatestBookTitle)
	})

	t.Run("Update", func(t *testing.T) {
		t.Parallel()
		updated := updateShelf(t, &librarypb.Shelf{Name: shelf.Name, DisplayName: "QueryJoin Tie Shelf Renamed"}, []string{"display_name"})
		require.Equal(t, first.Name, updated.LatestBook)
		require.Equal(t, "Tie A", updated.LatestBookTitle)
	})
}

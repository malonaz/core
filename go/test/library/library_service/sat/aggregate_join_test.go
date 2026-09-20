package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	decimalpb "google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/protobuf/types/known/durationpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

// createTestBookWithPrice creates a pageless book priced at the given decimal
// string, or unpriced when empty.
func createTestBookWithPrice(t *testing.T, shelfName, authorName, title, price string) *librarypb.Book {
	t.Helper()
	book := &librarypb.Book{
		Title:    title,
		Author:   authorName,
		Duration: durationpb.New(time.Second),
		Metadata: &librarypb.BookMetadata{},
	}
	if price != "" {
		book.Price = &decimalpb.Decimal{Value: price}
	}
	created, err := libraryServiceClient.CreateBook(ctx, &libraryservicepb.CreateBookRequest{Parent: shelfName, Book: book})
	require.NoError(t, err)
	return created
}

// The shelf's aggregate joins fold its books: total_page_count sums page_count
// over books with pages (the latest_book filter), book_count counts every
// book, last_book_create_time is the latest create_time.
func TestAggregateJoin_ShelfBooks(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "AggregateJoin Author")

	t.Run("NoBooks_Unset", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "AggregateJoin Empty Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		for _, got := range []*librarypb.Shelf{shelf, getShelf(t, shelf.Name)} {
			require.Zero(t, got.TotalPageCount)
			require.Zero(t, got.BookCount)
			require.Nil(t, got.LastBookCreateTime)
		}
	})

	t.Run("FoldsEveryBook", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "AggregateJoin Sum Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBookWithPageCount(t, shelf.Name, author.Name, "AggregateJoin Book One", 100)
		createTestBookWithPageCount(t, shelf.Name, author.Name, "AggregateJoin Book Two", 250)
		last := createTestBookWithPageCount(t, shelf.Name, author.Name, "AggregateJoin Book Three", 7)

		gotShelf := getShelf(t, shelf.Name)
		require.EqualValues(t, 357, gotShelf.TotalPageCount)
		require.EqualValues(t, 3, gotShelf.BookCount)
		require.Equal(t, last.CreateTime.AsTime(), gotShelf.LastBookCreateTime.AsTime())
	})

	// The filter shapes the sum but not the count: a pageless book is a book.
	t.Run("FilterExcludesPagelessBooks", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "AggregateJoin Filter Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBookWithPageCount(t, shelf.Name, author.Name, "AggregateJoin Paged Book", 42)
		createTestBookWithPageCount(t, shelf.Name, author.Name, "AggregateJoin Pageless Book", 0)

		gotShelf := getShelf(t, shelf.Name)
		require.EqualValues(t, 42, gotShelf.TotalPageCount)
		require.EqualValues(t, 2, gotShelf.BookCount)
	})

	t.Run("OnlyPagelessBooks_SumUnset", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "AggregateJoin Pageless Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBookWithPageCount(t, shelf.Name, author.Name, "AggregateJoin Only Pageless Book", 0)

		gotShelf := getShelf(t, shelf.Name)
		require.Zero(t, gotShelf.TotalPageCount)
		require.EqualValues(t, 1, gotShelf.BookCount)
		require.Equal(t, book.CreateTime.AsTime(), gotShelf.LastBookCreateTime.AsTime())
	})

	// Update and Create resolve joins through RETURNING subqueries rather than
	// the lateral join; both must agree with Get.
	t.Run("PopulatedOnUpdate", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "AggregateJoin Update Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBookWithPageCount(t, shelf.Name, author.Name, "AggregateJoin Update Book", 12)
		last := createTestBookWithPageCount(t, shelf.Name, author.Name, "AggregateJoin Update Book Two", 30)

		updated := updateShelf(t, &librarypb.Shelf{Name: shelf.Name, DisplayName: "AggregateJoin Update Shelf Renamed"}, []string{"display_name"})
		require.EqualValues(t, 42, updated.TotalPageCount)
		require.EqualValues(t, 2, updated.BookCount)
		require.Equal(t, last.CreateTime.AsTime(), updated.LastBookCreateTime.AsTime())
	})

	// SUM over a NUMERIC column stays a decimal, exact: 0.1 + 0.2 is 0.3.
	t.Run("DecimalSum", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "AggregateJoin Decimal Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		require.Nil(t, shelf.TotalPrice)
		createTestBookWithPrice(t, shelf.Name, author.Name, "AggregateJoin Priced Book", "0.1")
		createTestBookWithPrice(t, shelf.Name, author.Name, "AggregateJoin Priced Book Two", "0.2")
		createTestBookWithPrice(t, shelf.Name, author.Name, "AggregateJoin Unpriced Book", "")

		require.Equal(t, "0.3", getShelf(t, shelf.Name).GetTotalPrice().GetValue())
		updated := updateShelf(t, &librarypb.Shelf{Name: shelf.Name, DisplayName: "AggregateJoin Decimal Shelf Renamed"}, []string{"display_name"})
		require.Equal(t, "0.3", updated.GetTotalPrice().GetValue())
	})

	t.Run("DecimalSum_NoPricedBooks_Unset", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "AggregateJoin Unpriced Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBookWithPrice(t, shelf.Name, author.Name, "AggregateJoin Unpriced Only Book", "")
		require.Nil(t, getShelf(t, shelf.Name).TotalPrice)
	})

	t.Run("DeletingABookChangesTheRead", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "AggregateJoin Delete Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		kept := createTestBookWithPageCount(t, shelf.Name, author.Name, "AggregateJoin Kept Book", 10)
		deleted := createTestBookWithPageCount(t, shelf.Name, author.Name, "AggregateJoin Deleted Book", 20)
		require.EqualValues(t, 30, getShelf(t, shelf.Name).TotalPageCount)

		_, err := libraryServiceClient.DeleteBook(ctx, &libraryservicepb.DeleteBookRequest{Name: deleted.Name})
		require.NoError(t, err)

		gotShelf := getShelf(t, shelf.Name)
		require.EqualValues(t, 10, gotShelf.TotalPageCount)
		require.EqualValues(t, 1, gotShelf.BookCount)
		require.Equal(t, kept.CreateTime.AsTime(), gotShelf.LastBookCreateTime.AsTime())
	})
}

// The aggregate is a lateral join, so List can filter and order on it like on
// any other joined column.
func TestAggregateJoin_List(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "AggregateJoin List Author")
	empty := createTestShelf(t, organizationParent, "AggregateJoin List Empty Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	small := createTestShelf(t, organizationParent, "AggregateJoin List Small Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	createTestBookWithPageCount(t, small.Name, author.Name, "AggregateJoin List Small Book", 50)
	large := createTestShelf(t, organizationParent, "AggregateJoin List Large Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	createTestBookWithPageCount(t, large.Name, author.Name, "AggregateJoin List Large Book One", 300)
	createTestBookWithPageCount(t, large.Name, author.Name, "AggregateJoin List Large Book Two", 400)
	createTestBookWithPrice(t, small.Name, author.Name, "AggregateJoin List Small Priced Book", "12.50")
	createTestBookWithPrice(t, large.Name, author.Name, "AggregateJoin List Large Priced Book", "7.25")

	listShelves := func(t *testing.T, filter, orderBy string) []*librarypb.Shelf {
		t.Helper()
		response, err := libraryServiceClient.ListShelves(ctx, &libraryservicepb.ListShelvesRequest{
			Parent:  organizationParent,
			Filter:  filter,
			OrderBy: orderBy,
		})
		require.NoError(t, err)
		return response.Shelves
	}
	names := func(shelves []*librarypb.Shelf) []string {
		names := make([]string, len(shelves))
		for i, shelf := range shelves {
			names[i] = shelf.Name
		}
		return names
	}

	t.Run("Populated", func(t *testing.T) {
		t.Parallel()
		byName := map[string]*librarypb.Shelf{}
		for _, shelf := range listShelves(t, "", "") {
			byName[shelf.Name] = shelf
		}
		require.EqualValues(t, 700, byName[large.Name].TotalPageCount)
		require.EqualValues(t, 3, byName[large.Name].BookCount)
		require.Zero(t, byName[empty.Name].TotalPageCount)
	})

	t.Run("FilterComparison", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, []string{large.Name}, names(listShelves(t, "total_page_count > 100", "")))
		require.ElementsMatch(t, []string{small.Name, large.Name}, names(listShelves(t, "total_page_count > 0", "")))
	})

	// SUM over no rows is NULL, so presence separates shelves with pages
	// from shelves without.
	t.Run("FilterPresence", func(t *testing.T) {
		t.Parallel()
		require.ElementsMatch(t, []string{small.Name, large.Name}, names(listShelves(t, "total_page_count:*", "")))
		require.Equal(t, []string{empty.Name}, names(listShelves(t, "NOT total_page_count:*", "")))
	})

	t.Run("OrderBy", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, []string{large.Name, small.Name, empty.Name}, names(listShelves(t, "", "total_page_count desc")))
		require.Equal(t, []string{small.Name, large.Name}, names(listShelves(t, "total_page_count:*", "total_page_count asc")))
	})

	t.Run("Decimal", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, []string{small.Name}, names(listShelves(t, "total_price > 10", "")))
		require.ElementsMatch(t, []string{small.Name, large.Name}, names(listShelves(t, "total_price:*", "")))
		require.Equal(t, []string{small.Name, large.Name, empty.Name}, names(listShelves(t, "", "total_price desc")))
	})
}

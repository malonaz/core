package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

// setShelfBestBook points the shelf's best_book reference at the given book.
func setShelfBestBook(t *testing.T, shelfName, bookName string) *librarypb.Shelf {
	t.Helper()
	updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
		Shelf: &librarypb.Shelf{
			Name:     shelfName,
			BestBook: bookName,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"best_book"}},
	}
	updatedShelf, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
	require.NoError(t, err)
	return updatedShelf
}

func TestReferenceJoin_ShelfBestBook(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "RefJoin Author")

	t.Run("EmptyReference_JoinedValueUnset", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "RefJoin Empty Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		require.Empty(t, shelf.BestBook)
		require.Equal(t, int32(0), shelf.BestBookPageCount)

		gotShelf := getShelf(t, shelf.Name)
		require.Equal(t, int32(0), gotShelf.BestBookPageCount)
	})

	t.Run("PopulatedOnUpdateResponse", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "RefJoin Update Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, author.Name, "RefJoin Update Book")
		require.Equal(t, int32(200), book.PageCount)

		updatedShelf := setShelfBestBook(t, shelf.Name, book.Name)
		require.Equal(t, book.Name, updatedShelf.BestBook)
		require.Equal(t, int32(200), updatedShelf.BestBookPageCount)
	})

	t.Run("PopulatedOnGet", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "RefJoin Get Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)
		book := createTestBook(t, shelf.Name, author.Name, "RefJoin Get Book")
		setShelfBestBook(t, shelf.Name, book.Name)

		gotShelf := getShelf(t, shelf.Name)
		require.Equal(t, book.Name, gotShelf.BestBook)
		require.Equal(t, int32(200), gotShelf.BestBookPageCount)
	})

	t.Run("PopulatedOnList", func(t *testing.T) {
		t.Parallel()
		scopedOrganizationParent := getOrganizationParent()
		scopedAuthor := createTestAuthor(t, scopedOrganizationParent, "RefJoin List Author")
		shelf := createTestShelf(t, scopedOrganizationParent, "RefJoin List Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, scopedAuthor.Name, "RefJoin List Book")
		setShelfBestBook(t, shelf.Name, book.Name)

		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: scopedOrganizationParent,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		require.Len(t, listShelvesResponse.Shelves, 1)
		require.Equal(t, int32(200), listShelvesResponse.Shelves[0].BestBookPageCount)
	})

	t.Run("PopulatedOnBatchGet", func(t *testing.T) {
		t.Parallel()
		shelfWithBook := createTestShelf(t, organizationParent, "RefJoin Batch Shelf A", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelfWithBook.Name, author.Name, "RefJoin Batch Book")
		setShelfBestBook(t, shelfWithBook.Name, book.Name)
		shelfWithoutBook := createTestShelf(t, organizationParent, "RefJoin Batch Shelf B", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{shelfWithBook.Name, shelfWithoutBook.Name},
		}
		batchGetShelvesResponse, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetShelvesResponse.Shelves, 2)
		require.Equal(t, int32(200), batchGetShelvesResponse.Shelves[0].BestBookPageCount)
		require.Equal(t, int32(0), batchGetShelvesResponse.Shelves[1].BestBookPageCount)
	})

	t.Run("ReflectsBookUpdate", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "RefJoin BookUpd Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, author.Name, "RefJoin BookUpd Book")
		setShelfBestBook(t, shelf.Name, book.Name)

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:      book.Name,
				PageCount: 999,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"page_count"}},
		}
		_, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		require.NoError(t, err)

		gotShelf := getShelf(t, shelf.Name)
		require.Equal(t, int32(999), gotShelf.BestBookPageCount)
	})

	t.Run("ReflectsReferenceChange", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "RefJoin Repoint Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		bookA := createTestBookWithYearAndPages(t, shelf.Name, author.Name, "RefJoin Repoint Book A", 2000, 100)
		bookB := createTestBookWithYearAndPages(t, shelf.Name, author.Name, "RefJoin Repoint Book B", 2000, 300)

		setShelfBestBook(t, shelf.Name, bookA.Name)
		require.Equal(t, int32(100), getShelf(t, shelf.Name).BestBookPageCount)

		setShelfBestBook(t, shelf.Name, bookB.Name)
		require.Equal(t, int32(300), getShelf(t, shelf.Name).BestBookPageCount)
	})

	t.Run("CrossShelfReference_NoMatch", func(t *testing.T) {
		t.Parallel()
		// The join equates the shared identifier columns, so a best_book
		// belonging to a different shelf never matches: LEFT JOIN yields NULL.
		shelfA := createTestShelf(t, organizationParent, "RefJoin Cross Shelf A", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		shelfB := createTestShelf(t, organizationParent, "RefJoin Cross Shelf B", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)
		foreignBook := createTestBook(t, shelfA.Name, author.Name, "RefJoin Cross Book")

		updatedShelf := setShelfBestBook(t, shelfB.Name, foreignBook.Name)
		require.Equal(t, foreignBook.Name, updatedShelf.BestBook)
		require.Equal(t, int32(0), updatedShelf.BestBookPageCount)
	})

	t.Run("DanglingReference_AfterBookHardDelete", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "RefJoin Dangling Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, author.Name, "RefJoin Dangling Book")
		setShelfBestBook(t, shelf.Name, book.Name)
		require.Equal(t, int32(200), getShelf(t, shelf.Name).BestBookPageCount)

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		// The shelf itself must survive its dangling reference.
		gotShelf := getShelf(t, shelf.Name)
		require.Equal(t, book.Name, gotShelf.BestBook)
		require.Equal(t, int32(0), gotShelf.BestBookPageCount)
	})

	t.Run("ClearReference", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "RefJoin Clear Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, author.Name, "RefJoin Clear Book")
		setShelfBestBook(t, shelf.Name, book.Name)

		clearedShelf := setShelfBestBook(t, shelf.Name, "")
		require.Empty(t, clearedShelf.BestBook)
		require.Equal(t, int32(0), clearedShelf.BestBookPageCount)
	})
}

func TestReferenceJoin_FilterByBestBookPageCount(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "RefJoin Filter Author")

	shelfSmall := createTestShelf(t, organizationParent, "RefJoin Filter Small Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	smallBook := createTestBookWithYearAndPages(t, shelfSmall.Name, author.Name, "RefJoin Filter Small Book", 2000, 100)
	setShelfBestBook(t, shelfSmall.Name, smallBook.Name)

	shelfLarge := createTestShelf(t, organizationParent, "RefJoin Filter Large Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)
	largeBook := createTestBookWithYearAndPages(t, shelfLarge.Name, author.Name, "RefJoin Filter Large Book", 2000, 500)
	setShelfBestBook(t, shelfLarge.Name, largeBook.Name)

	shelfNone := createTestShelf(t, organizationParent, "RefJoin Filter None Shelf", librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY)

	listShelves := func(filter string) []*librarypb.Shelf {
		t.Helper()
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: organizationParent,
			Filter: filter,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		return listShelvesResponse.Shelves
	}

	t.Run("ExactMatch", func(t *testing.T) {
		t.Parallel()
		results := listShelves(`best_book_page_count = 100`)
		require.Len(t, results, 1)
		require.Equal(t, shelfSmall.Name, results[0].Name)
	})

	t.Run("GreaterThan", func(t *testing.T) {
		t.Parallel()
		results := listShelves(`best_book_page_count > 100`)
		require.Len(t, results, 1)
		require.Equal(t, shelfLarge.Name, results[0].Name)
	})

	t.Run("Presence", func(t *testing.T) {
		t.Parallel()
		results := listShelves(`best_book_page_count:*`)
		require.Len(t, results, 2)
		nameSet := map[string]bool{}
		for _, shelf := range results {
			nameSet[shelf.Name] = true
		}
		require.True(t, nameSet[shelfSmall.Name])
		require.True(t, nameSet[shelfLarge.Name])
		require.False(t, nameSet[shelfNone.Name])
	})

	t.Run("NotPresent", func(t *testing.T) {
		t.Parallel()
		results := listShelves(`NOT best_book_page_count:*`)
		require.Len(t, results, 1)
		require.Equal(t, shelfNone.Name, results[0].Name)
	})

	t.Run("CombinedWithOwnColumn", func(t *testing.T) {
		t.Parallel()
		results := listShelves(`best_book_page_count >= 100 AND genre = SHELF_GENRE_HISTORY`)
		require.Len(t, results, 1)
		require.Equal(t, shelfLarge.Name, results[0].Name)
	})
}

func TestReferenceJoin_OrderByBestBookPageCount(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "RefJoin Order Author")

	shelfSmall := createTestShelf(t, organizationParent, "RefJoin Order Small Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	smallBook := createTestBookWithYearAndPages(t, shelfSmall.Name, author.Name, "RefJoin Order Small Book", 2000, 100)
	setShelfBestBook(t, shelfSmall.Name, smallBook.Name)

	shelfLarge := createTestShelf(t, organizationParent, "RefJoin Order Large Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	largeBook := createTestBookWithYearAndPages(t, shelfLarge.Name, author.Name, "RefJoin Order Large Book", 2000, 500)
	setShelfBestBook(t, shelfLarge.Name, largeBook.Name)

	shelfNone := createTestShelf(t, organizationParent, "RefJoin Order None Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

	t.Run("Ascending_NullsLast", func(t *testing.T) {
		t.Parallel()
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent:  organizationParent,
			OrderBy: "best_book_page_count asc",
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		require.Len(t, listShelvesResponse.Shelves, 3)
		require.Equal(t, shelfSmall.Name, listShelvesResponse.Shelves[0].Name)
		require.Equal(t, shelfLarge.Name, listShelvesResponse.Shelves[1].Name)
		require.Equal(t, shelfNone.Name, listShelvesResponse.Shelves[2].Name)
	})

	t.Run("Descending_NullsLast", func(t *testing.T) {
		t.Parallel()
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent:  organizationParent,
			OrderBy: "best_book_page_count desc",
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		require.Len(t, listShelvesResponse.Shelves, 3)
		require.Equal(t, shelfLarge.Name, listShelvesResponse.Shelves[0].Name)
		require.Equal(t, shelfSmall.Name, listShelvesResponse.Shelves[1].Name)
		require.Equal(t, shelfNone.Name, listShelvesResponse.Shelves[2].Name)
	})
}

func TestReferenceJoin_UpdateJoinedFieldRejected(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "RefJoin Unauth Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
		Shelf: &librarypb.Shelf{
			Name: shelf.Name,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"best_book_page_count"}},
	}
	_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
	grpcrequire.Error(t, codes.InvalidArgument, err)
}

func createTestBookWithYearAndPages(t *testing.T, shelfName, authorName, title string, year, pageCount int32) *librarypb.Book {
	t.Helper()
	book := createTestBookWithYear(t, shelfName, authorName, title, year)
	updateBookRequest := &libraryservicepb.UpdateBookRequest{
		Book: &librarypb.Book{
			Name:      book.Name,
			PageCount: pageCount,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"page_count"}},
	}
	updatedBook, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
	require.NoError(t, err)
	return updatedBook
}

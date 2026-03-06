package sat

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	libraryservicepb "github.com/malonaz/core/genproto/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/library/v1"
	"github.com/malonaz/core/go/aip"
)

func TestAIPPaginate_Authors(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	for i := range 5 {
		createTestAuthor(t, organizationParent, fmt.Sprintf("Paginate Author %02d", i))
	}

	t.Run("CollectsAllPages", func(t *testing.T) {
		t.Parallel()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:   organizationParent,
			PageSize: 2,
		}
		authors, err := aip.Paginate[*librarypb.Author](ctx, listAuthorsRequest, libraryServiceClient.ListAuthors)
		require.NoError(t, err)
		require.Len(t, authors, 5)
	})

	t.Run("SinglePageSufficient", func(t *testing.T) {
		t.Parallel()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:   organizationParent,
			PageSize: 100,
		}
		authors, err := aip.Paginate[*librarypb.Author](ctx, listAuthorsRequest, libraryServiceClient.ListAuthors)
		require.NoError(t, err)
		require.Len(t, authors, 5)
	})

	t.Run("PageSizeOne", func(t *testing.T) {
		t.Parallel()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:   organizationParent,
			PageSize: 1,
		}
		authors, err := aip.Paginate[*librarypb.Author](ctx, listAuthorsRequest, libraryServiceClient.ListAuthors)
		require.NoError(t, err)
		require.Len(t, authors, 5)
	})

	t.Run("WithFilter", func(t *testing.T) {
		t.Parallel()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:   organizationParent,
			PageSize: 1,
			Filter:   `display_name = "Paginate Author 00"`,
		}
		authors, err := aip.Paginate[*librarypb.Author](ctx, listAuthorsRequest, libraryServiceClient.ListAuthors)
		require.NoError(t, err)
		require.Len(t, authors, 1)
		require.Equal(t, "Paginate Author 00", authors[0].DisplayName)
	})

	t.Run("EmptyResult", func(t *testing.T) {
		t.Parallel()
		emptyParent := getOrganizationParent()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:   emptyParent,
			PageSize: 10,
		}
		authors, err := aip.Paginate[*librarypb.Author](ctx, listAuthorsRequest, libraryServiceClient.ListAuthors)
		require.NoError(t, err)
		require.Empty(t, authors)
	})

	t.Run("WithOrderBy", func(t *testing.T) {
		t.Parallel()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:   organizationParent,
			PageSize: 2,
			OrderBy:  "display_name asc",
		}
		authors, err := aip.Paginate[*librarypb.Author](ctx, listAuthorsRequest, libraryServiceClient.ListAuthors)
		require.NoError(t, err)
		require.Len(t, authors, 5)
		for i := 1; i < len(authors); i++ {
			require.True(t, authors[i-1].DisplayName <= authors[i].DisplayName)
		}
	})
}

func TestAIPPaginate_Books(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Paginate Books Author")
	shelf := createTestShelf(t, organizationParent, "Paginate Books Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	for i := range 7 {
		createTestBookWithTitle(t, shelf.Name, author.Name, fmt.Sprintf("Paginate Book %02d", i))
	}

	t.Run("CollectsAllPages", func(t *testing.T) {
		t.Parallel()
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:   shelf.Name,
			PageSize: 3,
		}
		books, err := aip.Paginate[*librarypb.Book](ctx, listBooksRequest, libraryServiceClient.ListBooks)
		require.NoError(t, err)
		require.Len(t, books, 7)
	})

	t.Run("PreservesOrder", func(t *testing.T) {
		t.Parallel()
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:   shelf.Name,
			PageSize: 2,
			OrderBy:  "title asc",
		}
		books, err := aip.Paginate[*librarypb.Book](ctx, listBooksRequest, libraryServiceClient.ListBooks)
		require.NoError(t, err)
		require.Len(t, books, 7)
		for i := 1; i < len(books); i++ {
			require.True(t, books[i-1].Title <= books[i].Title)
		}
	})
}

func TestAIPPaginate_PageIterator(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	for i := range 5 {
		createTestAuthor(t, organizationParent, fmt.Sprintf("PageIter Author %02d", i))
	}

	t.Run("YieldsPages", func(t *testing.T) {
		t.Parallel()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:   organizationParent,
			PageSize: 2,
		}
		pageCount := 0
		totalItems := 0
		for page, err := range aip.PageIterator[*librarypb.Author](ctx, listAuthorsRequest, libraryServiceClient.ListAuthors) {
			require.NoError(t, err)
			pageCount++
			totalItems += len(page)
		}
		require.Equal(t, 3, pageCount)
		require.Equal(t, 5, totalItems)
	})

	t.Run("EarlyBreak", func(t *testing.T) {
		t.Parallel()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:   organizationParent,
			PageSize: 2,
		}
		var firstPage []*librarypb.Author
		for page, err := range aip.PageIterator[*librarypb.Author](ctx, listAuthorsRequest, libraryServiceClient.ListAuthors) {
			require.NoError(t, err)
			firstPage = page
			break
		}
		require.Len(t, firstPage, 2)
	})
}

func TestAIPPaginate_Shelves(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	for i := range 4 {
		createTestShelf(t, organizationParent, fmt.Sprintf("Paginate Shelf %02d", i), librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	}

	listShelvesRequest := &libraryservicepb.ListShelvesRequest{
		Parent:   organizationParent,
		PageSize: 2,
	}
	shelves, err := aip.Paginate[*librarypb.Shelf](ctx, listShelvesRequest, libraryServiceClient.ListShelves)
	require.NoError(t, err)
	require.Len(t, shelves, 4)
}

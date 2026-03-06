package sat

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestOrderBy_SingleField_Ascending(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Order Test Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	author := createTestAuthor(t, organizationParent, "Order Test Author")

	bookA := createTestBookWithTitle(t, shelf.Name, author.Name, "AAA Book")
	bookB := createTestBookWithTitle(t, shelf.Name, author.Name, "BBB Book")
	bookC := createTestBookWithTitle(t, shelf.Name, author.Name, "CCC Book")

	listBooksRequest := &libraryservicepb.ListBooksRequest{
		Parent:  shelf.Name,
		OrderBy: "title asc",
	}
	listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
	require.NoError(t, err)
	require.Len(t, listBooksResponse.Books, 3)
	require.Equal(t, bookA.Name, listBooksResponse.Books[0].Name)
	require.Equal(t, bookB.Name, listBooksResponse.Books[1].Name)
	require.Equal(t, bookC.Name, listBooksResponse.Books[2].Name)
}

func TestOrderBy_SingleField_Descending(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Order Desc Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	author := createTestAuthor(t, organizationParent, "Order Desc Author")

	bookA := createTestBookWithTitle(t, shelf.Name, author.Name, "AAA Desc Book")
	bookB := createTestBookWithTitle(t, shelf.Name, author.Name, "BBB Desc Book")
	bookC := createTestBookWithTitle(t, shelf.Name, author.Name, "CCC Desc Book")

	listBooksRequest := &libraryservicepb.ListBooksRequest{
		Parent:  shelf.Name,
		OrderBy: "title desc",
	}
	listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
	require.NoError(t, err)
	require.Len(t, listBooksResponse.Books, 3)
	require.Equal(t, bookC.Name, listBooksResponse.Books[0].Name)
	require.Equal(t, bookB.Name, listBooksResponse.Books[1].Name)
	require.Equal(t, bookA.Name, listBooksResponse.Books[2].Name)
}

func TestOrderBy_ImplicitAscending(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Implicit Asc Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	author := createTestAuthor(t, organizationParent, "Implicit Asc Author")

	bookA := createTestBookWithTitle(t, shelf.Name, author.Name, "AAA Implicit")
	bookB := createTestBookWithTitle(t, shelf.Name, author.Name, "BBB Implicit")

	listBooksRequest := &libraryservicepb.ListBooksRequest{
		Parent:  shelf.Name,
		OrderBy: "title",
	}
	listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
	require.NoError(t, err)
	require.Len(t, listBooksResponse.Books, 2)
	require.Equal(t, bookA.Name, listBooksResponse.Books[0].Name)
	require.Equal(t, bookB.Name, listBooksResponse.Books[1].Name)
}

func TestOrderBy_DefaultOrdering(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Default Order Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	author := createTestAuthor(t, organizationParent, "Default Order Author")

	bookFirst := createTestBookWithTitle(t, shelf.Name, author.Name, "First Created")
	bookSecond := createTestBookWithTitle(t, shelf.Name, author.Name, "Second Created")
	bookThird := createTestBookWithTitle(t, shelf.Name, author.Name, "Third Created")

	listBooksRequest := &libraryservicepb.ListBooksRequest{
		Parent: shelf.Name,
	}
	listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
	require.NoError(t, err)
	require.Len(t, listBooksResponse.Books, 3)
	require.Equal(t, bookThird.Name, listBooksResponse.Books[0].Name)
	require.Equal(t, bookSecond.Name, listBooksResponse.Books[1].Name)
	require.Equal(t, bookFirst.Name, listBooksResponse.Books[2].Name)
}

func TestOrderBy_IntegerField(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Int Order Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	author := createTestAuthor(t, organizationParent, "Int Order Author")

	book1990 := createTestBookWithYear(t, shelf.Name, author.Name, "Book 1990", 1990)
	book2000 := createTestBookWithYear(t, shelf.Name, author.Name, "Book 2000", 2000)
	book2010 := createTestBookWithYear(t, shelf.Name, author.Name, "Book 2010", 2010)

	t.Run("Ascending", func(t *testing.T) {
		t.Parallel()
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "publication_year asc",
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 3)
		require.Equal(t, book1990.Name, listBooksResponse.Books[0].Name)
		require.Equal(t, book2000.Name, listBooksResponse.Books[1].Name)
		require.Equal(t, book2010.Name, listBooksResponse.Books[2].Name)
	})

	t.Run("Descending", func(t *testing.T) {
		t.Parallel()
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "publication_year desc",
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 3)
		require.Equal(t, book2010.Name, listBooksResponse.Books[0].Name)
		require.Equal(t, book2000.Name, listBooksResponse.Books[1].Name)
		require.Equal(t, book1990.Name, listBooksResponse.Books[2].Name)
	})
}

func TestOrderBy_MultipleFields(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Multi Order Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	author := createTestAuthor(t, organizationParent, "Multi Order Author")

	bookA2000 := createTestBookWithYear(t, shelf.Name, author.Name, "AAA Multi", 2000)
	bookA2010 := createTestBookWithYear(t, shelf.Name, author.Name, "AAA Multi", 2010)
	bookB2000 := createTestBookWithYear(t, shelf.Name, author.Name, "BBB Multi", 2000)
	bookB2010 := createTestBookWithYear(t, shelf.Name, author.Name, "BBB Multi", 2010)

	t.Run("TitleAsc_YearAsc", func(t *testing.T) {
		t.Parallel()
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "title asc, publication_year asc",
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 4)
		require.Equal(t, bookA2000.Name, listBooksResponse.Books[0].Name)
		require.Equal(t, bookA2010.Name, listBooksResponse.Books[1].Name)
		require.Equal(t, bookB2000.Name, listBooksResponse.Books[2].Name)
		require.Equal(t, bookB2010.Name, listBooksResponse.Books[3].Name)
	})

	t.Run("TitleAsc_YearDesc", func(t *testing.T) {
		t.Parallel()
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "title asc, publication_year desc",
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 4)
		require.Equal(t, bookA2010.Name, listBooksResponse.Books[0].Name)
		require.Equal(t, bookA2000.Name, listBooksResponse.Books[1].Name)
		require.Equal(t, bookB2010.Name, listBooksResponse.Books[2].Name)
		require.Equal(t, bookB2000.Name, listBooksResponse.Books[3].Name)
	})

	t.Run("TitleDesc_YearAsc", func(t *testing.T) {
		t.Parallel()
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "title desc, publication_year asc",
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 4)
		require.Equal(t, bookB2000.Name, listBooksResponse.Books[0].Name)
		require.Equal(t, bookB2010.Name, listBooksResponse.Books[1].Name)
		require.Equal(t, bookA2000.Name, listBooksResponse.Books[2].Name)
		require.Equal(t, bookA2010.Name, listBooksResponse.Books[3].Name)
	})

	t.Run("TitleDesc_YearDesc", func(t *testing.T) {
		t.Parallel()
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "title desc, publication_year desc",
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 4)
		require.Equal(t, bookB2010.Name, listBooksResponse.Books[0].Name)
		require.Equal(t, bookB2000.Name, listBooksResponse.Books[1].Name)
		require.Equal(t, bookA2010.Name, listBooksResponse.Books[2].Name)
		require.Equal(t, bookA2000.Name, listBooksResponse.Books[3].Name)
	})
}

func TestOrderBy_TimestampField(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	authorA := createTestAuthor(t, organizationParent, "Timestamp A")
	authorB := createTestAuthor(t, organizationParent, "Timestamp B")
	authorC := createTestAuthor(t, organizationParent, "Timestamp C")

	t.Run("CreateTimeAsc", func(t *testing.T) {
		t.Parallel()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:  organizationParent,
			OrderBy: "create_time asc",
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 3)
		require.Equal(t, authorA.Name, listAuthorsResponse.Authors[0].Name)
		require.Equal(t, authorB.Name, listAuthorsResponse.Authors[1].Name)
		require.Equal(t, authorC.Name, listAuthorsResponse.Authors[2].Name)
	})

	t.Run("CreateTimeDesc", func(t *testing.T) {
		t.Parallel()
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:  organizationParent,
			OrderBy: "create_time desc",
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 3)
		require.Equal(t, authorC.Name, listAuthorsResponse.Authors[0].Name)
		require.Equal(t, authorB.Name, listAuthorsResponse.Authors[1].Name)
		require.Equal(t, authorA.Name, listAuthorsResponse.Authors[2].Name)
	})
}

func TestOrderBy_WithFilter(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Filter Order Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	author := createTestAuthor(t, organizationParent, "Filter Order Author")

	book2000A := createTestBookWithYear(t, shelf.Name, author.Name, "AAA Filter 2000", 2000)
	book2000B := createTestBookWithYear(t, shelf.Name, author.Name, "BBB Filter 2000", 2000)
	createTestBookWithYear(t, shelf.Name, author.Name, "CCC Filter 1999", 1999)

	listBooksRequest := &libraryservicepb.ListBooksRequest{
		Parent:  shelf.Name,
		Filter:  "publication_year = 2000",
		OrderBy: "title desc",
	}
	listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
	require.NoError(t, err)
	require.Len(t, listBooksResponse.Books, 2)
	require.Equal(t, book2000B.Name, listBooksResponse.Books[0].Name)
	require.Equal(t, book2000A.Name, listBooksResponse.Books[1].Name)
}

func TestOrderBy_WithPagination(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Paginated Order Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	author := createTestAuthor(t, organizationParent, "Paginated Order Author")

	var expectedOrder []*librarypb.Book
	for i := range 5 {
		book := createTestBookWithTitle(t, shelf.Name, author.Name, fmt.Sprintf("Book %02d", i))
		expectedOrder = append(expectedOrder, book)
	}

	var allBooks []*librarypb.Book
	pageToken := ""
	for {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:    shelf.Name,
			OrderBy:   "title asc",
			PageSize:  2,
			PageToken: pageToken,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		allBooks = append(allBooks, listBooksResponse.Books...)
		if listBooksResponse.NextPageToken == "" {
			break
		}
		pageToken = listBooksResponse.NextPageToken
	}

	require.Len(t, allBooks, 5)
	for i, book := range allBooks {
		require.Equal(t, expectedOrder[i].Name, book.Name)
	}
}

func createTestBookWithTitle(t *testing.T, shelfName, authorName, title string) *librarypb.Book {
	t.Helper()
	createBookRequest := &libraryservicepb.CreateBookRequest{
		Parent: shelfName,
		Book: &librarypb.Book{
			Title:    title,
			Author:   authorName,
			Metadata: &librarypb.BookMetadata{},
		},
	}
	book, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
	require.NoError(t, err)
	return book
}

func createTestBookWithYear(t *testing.T, shelfName, authorName, title string, year int32) *librarypb.Book {
	t.Helper()
	createBookRequest := &libraryservicepb.CreateBookRequest{
		Parent: shelfName,
		Book: &librarypb.Book{
			Title:           title,
			Author:          authorName,
			PublicationYear: year,
			Metadata:        &librarypb.BookMetadata{},
		},
	}
	book, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
	require.NoError(t, err)
	return book
}

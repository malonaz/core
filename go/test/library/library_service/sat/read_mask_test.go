package sat

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malonaz/core/go/grpc/middleware"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestReadMask_Get(t *testing.T) {
	t.Parallel()

	t.Run("ReturnsOnlyRequestedFields", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "ReadMask Get Author")
		ctxWithReadMask := middleware.WithReadMask(ctx, "name,display_name")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		maskedAuthor, err := libraryServiceClient.GetAuthor(ctxWithReadMask, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, author.Name, maskedAuthor.Name)
		require.Equal(t, "ReadMask Get Author", maskedAuthor.DisplayName)
		require.Empty(t, maskedAuthor.Biography)
		require.Empty(t, maskedAuthor.EmailAddress)
		require.Nil(t, maskedAuthor.Metadata)
	})

	t.Run("NestedFields", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "ReadMask Nested Author")
		ctxWithReadMask := middleware.WithReadMask(ctx, "name,metadata.country")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		maskedAuthor, err := libraryServiceClient.GetAuthor(ctxWithReadMask, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, author.Name, maskedAuthor.Name)
		require.Empty(t, maskedAuthor.DisplayName)
		require.NotNil(t, maskedAuthor.Metadata)
		require.Equal(t, "US", maskedAuthor.Metadata.Country)
	})

	t.Run("WildcardReturnsAllFields", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "ReadMask Wildcard Author")
		ctxWithReadMask := middleware.WithReadMask(ctx, "*")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		maskedAuthor, err := libraryServiceClient.GetAuthor(ctxWithReadMask, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "ReadMask Wildcard Author", maskedAuthor.DisplayName)
		require.NotNil(t, maskedAuthor.Metadata)
	})
}

func TestReadMask_Get_Book(t *testing.T) {
	t.Parallel()

	t.Run("ReturnsOnlyRequestedFields", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "ReadMask Book Author")
		shelf := createTestShelf(t, organizationParent, "ReadMask Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, author.Name, "ReadMask Book")

		ctxWithReadMask := middleware.WithReadMask(ctx, "name,title")
		getBookRequest := &libraryservicepb.GetBookRequest{
			Name: book.Name,
		}
		maskedBook, err := libraryServiceClient.GetBook(ctxWithReadMask, getBookRequest)
		require.NoError(t, err)
		require.Equal(t, book.Name, maskedBook.Name)
		require.Equal(t, "ReadMask Book", maskedBook.Title)
		require.Empty(t, maskedBook.Author)
		require.Empty(t, maskedBook.Isbn)
		require.Nil(t, maskedBook.Metadata)
	})

	t.Run("NestedMetadataField", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "ReadMask BookNested Author")
		shelf := createTestShelf(t, organizationParent, "ReadMask BookNested Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, author.Name, "ReadMask BookNested")

		ctxWithReadMask := middleware.WithReadMask(ctx, "name,metadata.language")
		getBookRequest := &libraryservicepb.GetBookRequest{
			Name: book.Name,
		}
		maskedBook, err := libraryServiceClient.GetBook(ctxWithReadMask, getBookRequest)
		require.NoError(t, err)
		require.Equal(t, book.Name, maskedBook.Name)
		require.Empty(t, maskedBook.Title)
		require.NotNil(t, maskedBook.Metadata)
		require.Equal(t, "en", maskedBook.Metadata.Language)
		require.Empty(t, maskedBook.Metadata.Summary)
	})
}

func TestReadMask_List_AppliedToResourceElements(t *testing.T) {
	t.Parallel()

	t.Run("Authors", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		createTestAuthor(t, organizationParent, "ReadMask List Author A")
		createTestAuthor(t, organizationParent, "ReadMask List Author B")

		ctxWithReadMask := middleware.WithReadMask(ctx, "name,display_name")
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctxWithReadMask, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 2)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.Name)
			require.NotEmpty(t, author.DisplayName)
			require.Empty(t, author.Biography)
			require.Empty(t, author.EmailAddress)
			require.Nil(t, author.Metadata)
		}
	})

	t.Run("Books", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "ReadMask ListBooks Author")
		shelf := createTestShelf(t, organizationParent, "ReadMask ListBooks Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBook(t, shelf.Name, author.Name, "ReadMask ListBook A")
		createTestBook(t, shelf.Name, author.Name, "ReadMask ListBook B")

		ctxWithReadMask := middleware.WithReadMask(ctx, "name,title")
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctxWithReadMask, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 2)
		for _, book := range listBooksResponse.Books {
			require.NotEmpty(t, book.Name)
			require.NotEmpty(t, book.Title)
			require.Empty(t, book.Author)
			require.Empty(t, book.Isbn)
			require.Nil(t, book.Metadata)
		}
	})

	t.Run("Shelves", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		createTestShelf(t, organizationParent, "ReadMask ListShelf A", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestShelf(t, organizationParent, "ReadMask ListShelf B", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

		ctxWithReadMask := middleware.WithReadMask(ctx, "name,display_name")
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: organizationParent,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctxWithReadMask, listShelvesRequest)
		require.NoError(t, err)
		require.Len(t, listShelvesResponse.Shelves, 2)
		for _, shelf := range listShelvesResponse.Shelves {
			require.NotEmpty(t, shelf.Name)
			require.NotEmpty(t, shelf.DisplayName)
			require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_UNSPECIFIED, shelf.Genre)
			require.Nil(t, shelf.Metadata)
		}
	})
}

func TestReadMask_List_PreservesNextPageToken(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	for i := range 3 {
		createTestAuthor(t, organizationParent, fmt.Sprintf("ReadMask Page Author %02d", i))
	}

	ctxWithReadMask := middleware.WithReadMask(ctx, "name")
	listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
		Parent:   organizationParent,
		PageSize: 1,
	}
	listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctxWithReadMask, listAuthorsRequest)
	require.NoError(t, err)
	require.Len(t, listAuthorsResponse.Authors, 1)
	require.NotEmpty(t, listAuthorsResponse.NextPageToken)

	listAuthorsRequest.PageToken = listAuthorsResponse.NextPageToken
	listAuthorsResponse, err = libraryServiceClient.ListAuthors(ctxWithReadMask, listAuthorsRequest)
	require.NoError(t, err)
	require.Len(t, listAuthorsResponse.Authors, 1)
	require.NotEmpty(t, listAuthorsResponse.NextPageToken)
}

func TestReadMask_List_NestedFieldsOnResource(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	createTestAuthor(t, organizationParent, "ReadMask ListNested Author")

	ctxWithReadMask := middleware.WithReadMask(ctx, "name,metadata.country")
	listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
		Parent: organizationParent,
	}
	listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctxWithReadMask, listAuthorsRequest)
	require.NoError(t, err)
	require.Len(t, listAuthorsResponse.Authors, 1)
	author := listAuthorsResponse.Authors[0]
	require.NotEmpty(t, author.Name)
	require.Empty(t, author.DisplayName)
	require.NotNil(t, author.Metadata)
	require.Equal(t, "US", author.Metadata.Country)
}

func TestReadMask_BatchGet_AppliedToResourceElements(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author1 := createTestAuthor(t, organizationParent, "ReadMask BatchGet Author 1")
	author2 := createTestAuthor(t, organizationParent, "ReadMask BatchGet Author 2")

	ctxWithReadMask := middleware.WithReadMask(ctx, "name,display_name")
	batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
		Parent: organizationParent,
		Names:  []string{author1.Name, author2.Name},
	}
	batchGetAuthorsResponse, err := libraryServiceClient.BatchGetAuthors(ctxWithReadMask, batchGetAuthorsRequest)
	require.NoError(t, err)
	require.Len(t, batchGetAuthorsResponse.Authors, 2)
	for _, author := range batchGetAuthorsResponse.Authors {
		require.NotEmpty(t, author.Name)
		require.NotEmpty(t, author.DisplayName)
		require.Empty(t, author.Biography)
		require.Empty(t, author.EmailAddress)
		require.Nil(t, author.Metadata)
	}
}

func TestReadMask_Strict(t *testing.T) {
	t.Parallel()

	t.Run("ValidPathsSucceeds", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "ReadMask Strict Valid Author")
		ctxWithReadMask := middleware.WithReadMaskStrict(ctx, "name,display_name")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		maskedAuthor, err := libraryServiceClient.GetAuthor(ctxWithReadMask, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, author.Name, maskedAuthor.Name)
		require.Equal(t, "ReadMask Strict Valid Author", maskedAuthor.DisplayName)
		require.Empty(t, maskedAuthor.Biography)
	})

	t.Run("InvalidPathReturnsError", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "ReadMask Strict Invalid Author")
		ctxWithReadMask := middleware.WithReadMaskStrict(ctx, "name,nonexistent_field")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		_, err := libraryServiceClient.GetAuthor(ctxWithReadMask, getAuthorRequest)
		require.Error(t, err)
	})

	t.Run("InvalidPathOnListReturnsError", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		createTestAuthor(t, organizationParent, "ReadMask Strict List Author")
		ctxWithReadMask := middleware.WithReadMaskStrict(ctx, "name,bogus_field")
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
		}
		_, err := libraryServiceClient.ListAuthors(ctxWithReadMask, listAuthorsRequest)
		require.Error(t, err)
	})
}

func TestReadMask_CombinedWithFieldMask(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "ReadMask Combined Author")

	ctxCombined := middleware.WithFieldMask(ctx, "name,display_name,biography,email_address,metadata")
	ctxCombined = middleware.WithReadMask(ctxCombined, "name,display_name")
	getAuthorRequest := &libraryservicepb.GetAuthorRequest{
		Name: author.Name,
	}
	maskedAuthor, err := libraryServiceClient.GetAuthor(ctxCombined, getAuthorRequest)
	require.NoError(t, err)
	require.Equal(t, author.Name, maskedAuthor.Name)
	require.Equal(t, "ReadMask Combined Author", maskedAuthor.DisplayName)
	require.Empty(t, maskedAuthor.Biography)
	require.Empty(t, maskedAuthor.EmailAddress)
	require.Nil(t, maskedAuthor.Metadata)
}

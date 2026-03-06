package sat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	libraryservicepb "github.com/malonaz/core/genproto/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/library/v1"
)

func TestDelete_SoftDelete_Author(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("ReturnsDeleteTime", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "SoftDel Return Author")

		before := time.Now().UTC()
		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: author.Name,
		}
		deletedAuthor, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.NotNil(t, deletedAuthor.DeleteTime)
		deleteTime := deletedAuthor.DeleteTime.AsTime()
		require.True(t, !deleteTime.Before(before), "delete_time %v should be >= before %v", deleteTime, before)
		require.True(t, !deleteTime.After(after), "delete_time %v should be <= after %v", deleteTime, after)

		getAuthorRequest := &libraryservicepb.GetAuthorRequest{Name: author.Name}
		gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, deletedAuthor.DeleteTime, gotAuthor.DeleteTime)
	})

	t.Run("GetReturnsDeletedResource", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "SoftDel Get Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		getAuthorRequest := &libraryservicepb.GetAuthorRequest{Name: author.Name}
		gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		require.NoError(t, err)
		require.NotNil(t, gotAuthor.DeleteTime)
		require.Equal(t, author.DisplayName, gotAuthor.DisplayName)
	})

	t.Run("HiddenFromList", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "SoftDel Hidden Author 99211")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "SoftDel Hidden Author 99211"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Empty(t, listAuthorsResponse.Authors)
	})

	t.Run("ShowDeletedReveals", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "SoftDel ShowDel Author 88322")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:      organizationParent,
			Filter:      `display_name = "SoftDel ShowDel Author 88322"`,
			ShowDeleted: true,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
		require.NotNil(t, listAuthorsResponse.Authors[0].DeleteTime)
	})

	t.Run("DeleteAlreadyDeletedReturnsNotFound", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "SoftDel Twice Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		_, err = libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("AllowMissing_AlreadyDeleted", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "SoftDel AllowMissing Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		deleteAuthorRequest = &libraryservicepb.DeleteAuthorRequest{
			Name:         author.Name,
			AllowMissing: true,
		}
		_, err = libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)
	})

	t.Run("AllowMissing_NeverExisted", func(t *testing.T) {
		t.Parallel()
		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name:         organizationParent + "/authors/never-existed-softdel",
			AllowMissing: true,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: organizationParent + "/authors/nonexistent-softdel",
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("EtagMatch", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "SoftDel Etag Match Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: author.Name,
			Etag: author.Etag,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)
	})

	t.Run("EtagMismatch", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "SoftDel Etag Mismatch Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: author.Name,
			Etag: `"wrong-etag"`,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("PreservesFields", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "SoftDel Preserve Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
		deletedAuthor, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		require.Equal(t, author.Name, deletedAuthor.Name)
		require.Equal(t, author.DisplayName, deletedAuthor.DisplayName)
		require.Equal(t, author.Biography, deletedAuthor.Biography)
		require.Equal(t, author.EmailAddress, deletedAuthor.EmailAddress)
		require.Equal(t, author.CreateTime.AsTime(), deletedAuthor.CreateTime.AsTime())
	})
}

func TestDelete_SoftDelete_Shelf(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("ReturnsDeleteTime", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "SoftDel Shelf Time", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		before := time.Now().UTC()
		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		deletedShelf, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.NotNil(t, deletedShelf.DeleteTime)
		deleteTime := deletedShelf.DeleteTime.AsTime()
		require.True(t, !deleteTime.Before(before), "delete_time %v should be >= before %v", deleteTime, before)
		require.True(t, !deleteTime.After(after), "delete_time %v should be <= after %v", deleteTime, after)
	})

	t.Run("GetReturnsDeletedResource", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "SoftDel Shelf Get", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		getShelfRequest := &libraryservicepb.GetShelfRequest{Name: shelf.Name}
		gotShelf, err := libraryServiceClient.GetShelf(ctx, getShelfRequest)
		require.NoError(t, err)
		require.NotNil(t, gotShelf.DeleteTime)
		require.Equal(t, shelf.DisplayName, gotShelf.DisplayName)
	})

	t.Run("HiddenFromList", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "SoftDel Shelf Hidden 77433", librarypb.ShelfGenre_SHELF_GENRE_NON_FICTION)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: organizationParent,
			Filter: `display_name = "SoftDel Shelf Hidden 77433"`,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		require.Empty(t, listShelvesResponse.Shelves)
	})

	t.Run("ShowDeletedReveals", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "SoftDel Shelf ShowDel 66544", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent:      organizationParent,
			Filter:      `display_name = "SoftDel Shelf ShowDel 66544"`,
			ShowDeleted: true,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		require.Len(t, listShelvesResponse.Shelves, 1)
		require.NotNil(t, listShelvesResponse.Shelves[0].DeleteTime)
	})

	t.Run("DeleteAlreadyDeletedReturnsNotFound", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "SoftDel Shelf Twice", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		_, err = libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("AllowMissing_AlreadyDeleted", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "SoftDel Shelf AllowMissing", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		deleteShelfRequest = &libraryservicepb.DeleteShelfRequest{
			Name:         shelf.Name,
			AllowMissing: true,
		}
		_, err = libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)
	})

	t.Run("AllowMissing_NeverExisted", func(t *testing.T) {
		t.Parallel()
		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{
			Name:         organizationParent + "/shelves/never-existed-softdel",
			AllowMissing: true,
		}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{
			Name: organizationParent + "/shelves/nonexistent-softdel",
		}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("PreservesFields", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "SoftDel Shelf Preserve", librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		deletedShelf, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		require.Equal(t, shelf.Name, deletedShelf.Name)
		require.Equal(t, shelf.DisplayName, deletedShelf.DisplayName)
		require.Equal(t, shelf.Genre, deletedShelf.Genre)
		require.Equal(t, shelf.CreateTime.AsTime(), deletedShelf.CreateTime.AsTime())
	})
}

func TestDelete_HardDelete_Book(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "HardDel Book Author")
	shelf := createTestShelf(t, organizationParent, "HardDel Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("ResourceGone", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "HardDel Gone Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		getBookRequest := &libraryservicepb.GetBookRequest{Name: book.Name}
		_, err = libraryServiceClient.GetBook(ctx, getBookRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("RemovedFromList", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "HardDel List Book 55677")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `title = "HardDel List Book 55677"`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Empty(t, listBooksResponse.Books)
	})

	t.Run("DeleteAlreadyDeletedReturnsNotFound", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "HardDel Twice Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		_, err = libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("AllowMissing_NeverExisted", func(t *testing.T) {
		t.Parallel()
		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name:         shelf.Name + "/books/never-existed-harddel",
			AllowMissing: true,
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)
	})

	t.Run("AllowMissing_AlreadyDeleted", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "HardDel AllowMissing Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		deleteBookRequest = &libraryservicepb.DeleteBookRequest{
			Name:         book.Name,
			AllowMissing: true,
		}
		_, err = libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name: shelf.Name + "/books/nonexistent-harddel",
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("EtagMatch", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "HardDel Etag Match Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name: book.Name,
			Etag: book.Etag,
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)
	})

	t.Run("EtagMismatch", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "HardDel Etag Bad Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name: book.Name,
			Etag: `"wrong-etag"`,
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})
}

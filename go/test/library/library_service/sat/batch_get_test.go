package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestBatchGetAuthors(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author1 := createTestAuthor(t, organizationParent, "Batch Author 1")
	author2 := createTestAuthor(t, organizationParent, "Batch Author 2")
	author3 := createTestAuthor(t, organizationParent, "Batch Author 3")

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{author1.Name, author2.Name, author3.Name},
		}
		batchGetAuthorsResponse, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetAuthorsResponse.Authors, 3)
	})

	t.Run("SingleResource", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{author1.Name},
		}
		batchGetAuthorsResponse, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetAuthorsResponse.Authors, 1)
		require.Equal(t, author1.Name, batchGetAuthorsResponse.Authors[0].Name)
	})

	t.Run("PreservesOrder", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{author3.Name, author1.Name, author2.Name},
		}
		batchGetAuthorsResponse, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetAuthorsResponse.Authors, 3)
		require.Equal(t, author3.Name, batchGetAuthorsResponse.Authors[0].Name)
		require.Equal(t, author1.Name, batchGetAuthorsResponse.Authors[1].Name)
		require.Equal(t, author2.Name, batchGetAuthorsResponse.Authors[2].Name)
	})

	t.Run("MatchesIndividualGet", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{author1.Name, author2.Name},
		}
		batchGetAuthorsResponse, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetAuthorsResponse.Authors, 2)

		gotAuthor1 := getAuthor(t, author1.Name)
		gotAuthor2 := getAuthor(t, author2.Name)
		grpcrequire.Equal(t, gotAuthor1, batchGetAuthorsResponse.Authors[0])
		grpcrequire.Equal(t, gotAuthor2, batchGetAuthorsResponse.Authors[1])
	})

	t.Run("SoftDeletedReturned", func(t *testing.T) {
		t.Parallel()
		deletedAuthor := createTestAuthor(t, organizationParent, "Batch Deleted Author")
		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: deletedAuthor.Name}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{deletedAuthor.Name},
		}
		batchGetAuthorsResponse, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetAuthorsResponse.Authors, 1)
		require.NotNil(t, batchGetAuthorsResponse.Authors[0].DeleteTime)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{author1.Name, organizationParent + "/authors/nonexistent-batch"},
		}
		_, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Protovalidation_EmptyNames", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{},
		}
		_, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_DuplicateNames", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{author1.Name, author1.Name},
		}
		_, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_EmptyNameEntry", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{""},
		}
		_, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent_FullWildcard", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: "organizations/-",
			Names:  []string{author1.Name, author2.Name},
		}
		batchGetAuthorsResponse, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetAuthorsResponse.Authors, 2)
	})

	t.Run("WildcardName_Rejected", func(t *testing.T) {
		t.Parallel()
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{organizationParent + "/authors/-"},
		}
		_, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent_ChildParentMismatch", func(t *testing.T) {
		t.Parallel()
		otherOrganizationParent := getOrganizationParent()
		otherAuthor := createTestAuthor(t, otherOrganizationParent, "Other Org Author")
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: organizationParent,
			Names:  []string{otherAuthor.Name},
		}
		_, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent_CrossParent", func(t *testing.T) {
		t.Parallel()
		otherOrganizationParent := getOrganizationParent()
		otherAuthor := createTestAuthor(t, otherOrganizationParent, "Other Org Author")
		batchGetAuthorsRequest := &libraryservicepb.BatchGetAuthorsRequest{
			Parent: "organizations/-",
			Names:  []string{author1.Name, otherAuthor.Name},
		}
		batchGetAuthorsResponse, err := libraryServiceClient.BatchGetAuthors(ctx, batchGetAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, batchGetAuthorsResponse.Authors, 2)
		require.Equal(t, author1.Name, batchGetAuthorsResponse.Authors[0].Name)
		require.Equal(t, otherAuthor.Name, batchGetAuthorsResponse.Authors[1].Name)
	})

}

func TestBatchGetShelves(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf1 := createTestShelf(t, organizationParent, "Batch Shelf 1", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	shelf2 := createTestShelf(t, organizationParent, "Batch Shelf 2", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)
	shelf3 := createTestShelf(t, organizationParent, "Batch Shelf 3", librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{shelf1.Name, shelf2.Name, shelf3.Name},
		}
		batchGetShelvesResponse, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetShelvesResponse.Shelves, 3)
	})

	t.Run("SingleResource", func(t *testing.T) {
		t.Parallel()
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{shelf2.Name},
		}
		batchGetShelvesResponse, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetShelvesResponse.Shelves, 1)
		require.Equal(t, shelf2.Name, batchGetShelvesResponse.Shelves[0].Name)
	})

	t.Run("PreservesOrder", func(t *testing.T) {
		t.Parallel()
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{shelf3.Name, shelf1.Name, shelf2.Name},
		}
		batchGetShelvesResponse, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetShelvesResponse.Shelves, 3)
		require.Equal(t, shelf3.Name, batchGetShelvesResponse.Shelves[0].Name)
		require.Equal(t, shelf1.Name, batchGetShelvesResponse.Shelves[1].Name)
		require.Equal(t, shelf2.Name, batchGetShelvesResponse.Shelves[2].Name)
	})

	t.Run("MatchesIndividualGet", func(t *testing.T) {
		t.Parallel()
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{shelf1.Name},
		}
		batchGetShelvesResponse, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetShelvesResponse.Shelves, 1)

		gotShelf := getShelf(t, shelf1.Name)
		grpcrequire.Equal(t, gotShelf, batchGetShelvesResponse.Shelves[0])
	})

	t.Run("SoftDeletedReturned", func(t *testing.T) {
		t.Parallel()
		deletedShelf := createTestShelf(t, organizationParent, "Batch Deleted Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: deletedShelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{deletedShelf.Name},
		}
		batchGetShelvesResponse, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetShelvesResponse.Shelves, 1)
		require.NotNil(t, batchGetShelvesResponse.Shelves[0].DeleteTime)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{shelf1.Name, organizationParent + "/shelves/nonexistent-batch"},
		}
		_, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Protovalidation_EmptyNames", func(t *testing.T) {
		t.Parallel()
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{},
		}
		_, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_DuplicateNames", func(t *testing.T) {
		t.Parallel()
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{shelf1.Name, shelf1.Name},
		}
		_, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent", func(t *testing.T) {
		t.Parallel()
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: "organizations/-",
			Names:  []string{shelf1.Name, shelf2.Name},
		}
		batchGetShelvesResponse, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetShelvesResponse.Shelves, 2)
	})

	t.Run("WildcardName_Rejected", func(t *testing.T) {
		t.Parallel()
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{organizationParent + "/shelves/-"},
		}
		_, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent_ChildParentMismatch", func(t *testing.T) {
		t.Parallel()
		otherOrganizationParent := getOrganizationParent()
		otherShelf := createTestShelf(t, otherOrganizationParent, "Other Org Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: organizationParent,
			Names:  []string{otherShelf.Name},
		}
		_, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent_CrossParent", func(t *testing.T) {
		t.Parallel()
		otherOrganizationParent := getOrganizationParent()
		otherShelf := createTestShelf(t, otherOrganizationParent, "Other Org Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		batchGetShelvesRequest := &libraryservicepb.BatchGetShelvesRequest{
			Parent: "organizations/-",
			Names:  []string{shelf1.Name, otherShelf.Name},
		}
		batchGetShelvesResponse, err := libraryServiceClient.BatchGetShelves(ctx, batchGetShelvesRequest)
		require.NoError(t, err)
		require.Len(t, batchGetShelvesResponse.Shelves, 2)
		require.Equal(t, shelf1.Name, batchGetShelvesResponse.Shelves[0].Name)
		require.Equal(t, otherShelf.Name, batchGetShelvesResponse.Shelves[1].Name)
	})
}

func TestBatchGetBooks(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Batch Book Author")
	shelf := createTestShelf(t, organizationParent, "Batch Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book1 := createTestBook(t, shelf.Name, author.Name, "Batch Book 1")
	book2 := createTestBook(t, shelf.Name, author.Name, "Batch Book 2")
	book3 := createTestBook(t, shelf.Name, author.Name, "Batch Book 3")

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{book1.Name, book2.Name, book3.Name},
		}
		batchGetBooksResponse, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBooksResponse.Books, 3)
	})

	t.Run("SingleResource", func(t *testing.T) {
		t.Parallel()
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{book2.Name},
		}
		batchGetBooksResponse, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBooksResponse.Books, 1)
		require.Equal(t, book2.Name, batchGetBooksResponse.Books[0].Name)
	})

	t.Run("PreservesOrder", func(t *testing.T) {
		t.Parallel()
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{book3.Name, book1.Name, book2.Name},
		}
		batchGetBooksResponse, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBooksResponse.Books, 3)
		require.Equal(t, book3.Name, batchGetBooksResponse.Books[0].Name)
		require.Equal(t, book1.Name, batchGetBooksResponse.Books[1].Name)
		require.Equal(t, book2.Name, batchGetBooksResponse.Books[2].Name)
	})

	t.Run("MatchesIndividualGet", func(t *testing.T) {
		t.Parallel()
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{book1.Name, book3.Name},
		}
		batchGetBooksResponse, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBooksResponse.Books, 2)

		gotBook1 := getBook(t, book1.Name)
		gotBook3 := getBook(t, book3.Name)
		grpcrequire.Equal(t, gotBook1, batchGetBooksResponse.Books[0])
		grpcrequire.Equal(t, gotBook3, batchGetBooksResponse.Books[1])
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{book1.Name, shelf.Name + "/books/nonexistent-batch"},
		}
		_, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("HardDeletedNotFound", func(t *testing.T) {
		t.Parallel()
		deletedBook := createTestBook(t, shelf.Name, author.Name, "Batch HardDel Book")
		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: deletedBook.Name}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{deletedBook.Name},
		}
		_, err = libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Protovalidation_EmptyNames", func(t *testing.T) {
		t.Parallel()
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{},
		}
		_, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_DuplicateNames", func(t *testing.T) {
		t.Parallel()
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{book1.Name, book1.Name},
		}
		_, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent", func(t *testing.T) {
		t.Parallel()
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: "organizations/-/shelves/-",
			Names:  []string{book1.Name, book2.Name},
		}
		batchGetBooksResponse, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBooksResponse.Books, 2)
	})

	t.Run("WildcardName_Rejected", func(t *testing.T) {
		t.Parallel()
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{shelf.Name + "/books/-"},
		}
		_, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent_ChildParentMismatch", func(t *testing.T) {
		t.Parallel()
		otherShelf := createTestShelf(t, organizationParent, "Other Shelf For Books", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)
		otherBook := createTestBook(t, otherShelf.Name, author.Name, "Other Shelf Book")
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{otherBook.Name},
		}
		_, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent_CrossParent", func(t *testing.T) {
		t.Parallel()
		otherShelf := createTestShelf(t, organizationParent, "Other Shelf For Books", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)
		otherBook := createTestBook(t, otherShelf.Name, author.Name, "Other Shelf Book")
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: "organizations/-/shelves/-",
			Names:  []string{book1.Name, otherBook.Name},
		}
		batchGetBooksResponse, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBooksResponse.Books, 2)
		require.Equal(t, book1.Name, batchGetBooksResponse.Books[0].Name)
		require.Equal(t, otherBook.Name, batchGetBooksResponse.Books[1].Name)
	})

	t.Run("Protovalidation_EmptyNameEntry", func(t *testing.T) {
		t.Parallel()
		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{""},
		}
		_, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

}

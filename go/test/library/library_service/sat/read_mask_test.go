package sat

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/malonaz/core/go/grpc/middleware"
	grpcrequire "github.com/malonaz/core/go/grpc/require"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestReadMask_Author_Get(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := validAuthor()
	author.Labels = map[string]string{"env": "prod"}
	createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
		Parent: organizationParent,
		Author: author,
	}
	createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)

	t.Run("SingleField", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "display_name")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, createdAuthor.DisplayName, gotAuthor.DisplayName)
		require.Empty(t, gotAuthor.Name)
		require.Empty(t, gotAuthor.Biography)
		require.Empty(t, gotAuthor.EmailAddress)
		require.Nil(t, gotAuthor.Metadata)
		require.Nil(t, gotAuthor.CreateTime)
	})

	t.Run("MultipleFields", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,display_name,email_address")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, createdAuthor.Name, gotAuthor.Name)
		require.Equal(t, createdAuthor.DisplayName, gotAuthor.DisplayName)
		require.Equal(t, createdAuthor.EmailAddress, gotAuthor.EmailAddress)
		require.Empty(t, gotAuthor.Biography)
		require.Empty(t, gotAuthor.PhoneNumber)
		require.Nil(t, gotAuthor.Metadata)
	})

	t.Run("NestedField", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,metadata.country")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, createdAuthor.Name, gotAuthor.Name)
		require.Empty(t, gotAuthor.DisplayName)
		require.NotNil(t, gotAuthor.Metadata)
		require.Equal(t, "UK", gotAuthor.Metadata.Country)
		require.Empty(t, gotAuthor.Metadata.EmailAddresses)
	})

	t.Run("WildcardReturnsAll", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "*")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, createdAuthor.Name, gotAuthor.Name)
		require.Equal(t, createdAuthor.DisplayName, gotAuthor.DisplayName)
		require.Equal(t, createdAuthor.Biography, gotAuthor.Biography)
		require.NotNil(t, gotAuthor.Metadata)
		require.NotNil(t, gotAuthor.CreateTime)
	})

	t.Run("NoReadMaskReturnsAll", func(t *testing.T) {
		t.Parallel()
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, createdAuthor.Name, gotAuthor.Name)
		require.Equal(t, createdAuthor.DisplayName, gotAuthor.DisplayName)
		require.NotNil(t, gotAuthor.Metadata)
		require.NotNil(t, gotAuthor.CreateTime)
	})

	t.Run("LabelsField", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,labels")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, createdAuthor.Name, gotAuthor.Name)
		require.Equal(t, "prod", gotAuthor.Labels["env"])
		require.Empty(t, gotAuthor.DisplayName)
	})

	t.Run("TimestampFields", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,create_time,update_time")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, createdAuthor.Name, gotAuthor.Name)
		require.NotNil(t, gotAuthor.CreateTime)
		require.NotNil(t, gotAuthor.UpdateTime)
		require.Empty(t, gotAuthor.DisplayName)
		require.Nil(t, gotAuthor.Metadata)
	})

	t.Run("RepeatedFields", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,email_addresses,phone_numbers")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, createdAuthor.Name, gotAuthor.Name)
		require.Equal(t, createdAuthor.EmailAddresses, gotAuthor.EmailAddresses)
		require.Equal(t, createdAuthor.PhoneNumbers, gotAuthor.PhoneNumbers)
		require.Empty(t, gotAuthor.DisplayName)
		require.Empty(t, gotAuthor.EmailAddress)
	})
}

func TestReadMask_Author_List(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	createTestAuthor(t, organizationParent, "ReadMask List Author A")
	createTestAuthor(t, organizationParent, "ReadMask List Author B")

	t.Run("AppliedToAllItems", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,display_name")
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(readMaskCtx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 2)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.Name)
			require.NotEmpty(t, author.DisplayName)
			require.Empty(t, author.Biography)
			require.Empty(t, author.EmailAddress)
			require.Nil(t, author.Metadata)
			require.Nil(t, author.CreateTime)
		}
	})

	t.Run("NestedFieldOnList", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,metadata.country")
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(readMaskCtx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 2)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.Name)
			require.Empty(t, author.DisplayName)
			require.NotNil(t, author.Metadata)
		}
	})

	t.Run("WildcardOnList", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "*")
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(readMaskCtx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 2)
		for _, author := range listAuthorsResponse.Authors {
			require.NotEmpty(t, author.Name)
			require.NotEmpty(t, author.DisplayName)
			require.NotNil(t, author.Metadata)
			require.NotNil(t, author.CreateTime)
		}
	})
}

func TestReadMask_Book_Get(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "ReadMask Book Author")
	shelf := createTestShelf(t, organizationParent, "ReadMask Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "ReadMask Book Title")

	t.Run("SingleField", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "title")
		getBookRequest := &libraryservicepb.GetBookRequest{
			Name: book.Name,
		}
		gotBook, err := libraryServiceClient.GetBook(readMaskCtx, getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "ReadMask Book Title", gotBook.Title)
		require.Empty(t, gotBook.Name)
		require.Empty(t, gotBook.Author)
		require.Nil(t, gotBook.Metadata)
	})

	t.Run("NameAndNestedField", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,metadata.language")
		getBookRequest := &libraryservicepb.GetBookRequest{
			Name: book.Name,
		}
		gotBook, err := libraryServiceClient.GetBook(readMaskCtx, getBookRequest)
		require.NoError(t, err)
		require.Equal(t, book.Name, gotBook.Name)
		require.Empty(t, gotBook.Title)
		require.NotNil(t, gotBook.Metadata)
		require.Equal(t, "en", gotBook.Metadata.Language)
		require.Empty(t, gotBook.Metadata.Summary)
	})
}

func TestReadMask_Book_List(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "ReadMask BookList Author")
	shelf := createTestShelf(t, organizationParent, "ReadMask BookList Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	createTestBook(t, shelf.Name, author.Name, "ReadMask BookList A")
	createTestBook(t, shelf.Name, author.Name, "ReadMask BookList B")

	t.Run("AppliedToAllItems", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,title")
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(readMaskCtx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 2)
		for _, b := range listBooksResponse.Books {
			require.NotEmpty(t, b.Name)
			require.NotEmpty(t, b.Title)
			require.Empty(t, b.Author)
			require.Empty(t, b.Isbn)
			require.Nil(t, b.Metadata)
			require.Nil(t, b.CreateTime)
		}
	})
}

func TestReadMask_Shelf_Get(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "ReadMask Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

	t.Run("SingleField", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "display_name")
		getShelfRequest := &libraryservicepb.GetShelfRequest{
			Name: shelf.Name,
		}
		gotShelf, err := libraryServiceClient.GetShelf(readMaskCtx, getShelfRequest)
		require.NoError(t, err)
		require.Equal(t, "ReadMask Shelf", gotShelf.DisplayName)
		require.Empty(t, gotShelf.Name)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_UNSPECIFIED, gotShelf.Genre)
		require.Nil(t, gotShelf.Metadata)
	})

	t.Run("NestedMetadata", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,metadata.capacity")
		getShelfRequest := &libraryservicepb.GetShelfRequest{
			Name: shelf.Name,
		}
		gotShelf, err := libraryServiceClient.GetShelf(readMaskCtx, getShelfRequest)
		require.NoError(t, err)
		require.Equal(t, shelf.Name, gotShelf.Name)
		require.Empty(t, gotShelf.DisplayName)
		require.NotNil(t, gotShelf.Metadata)
		require.Equal(t, int32(100), gotShelf.Metadata.Capacity)
	})

	t.Run("EnumField", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,genre")
		getShelfRequest := &libraryservicepb.GetShelfRequest{
			Name: shelf.Name,
		}
		gotShelf, err := libraryServiceClient.GetShelf(readMaskCtx, getShelfRequest)
		require.NoError(t, err)
		require.Equal(t, shelf.Name, gotShelf.Name)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_HISTORY, gotShelf.Genre)
		require.Empty(t, gotShelf.DisplayName)
	})
}

func TestReadMask_Strict(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "ReadMask Strict Author")

	t.Run("ValidPaths", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMaskStrict(ctx, "name,display_name")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, author.Name, gotAuthor.Name)
		require.Equal(t, author.DisplayName, gotAuthor.DisplayName)
		require.Empty(t, gotAuthor.Biography)
	})

	t.Run("InvalidPathReturnsError", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMaskStrict(ctx, "name,nonexistent_field")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		_, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("InvalidNestedPathReturnsError", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMaskStrict(ctx, "metadata.nonexistent")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		_, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("NonStrictIgnoresInvalidPath", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,nonexistent_field")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(readMaskCtx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, author.Name, gotAuthor.Name)
		require.Empty(t, gotAuthor.DisplayName)
	})
}

func TestReadMask_Strict_List(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	createTestAuthor(t, organizationParent, "ReadMask StrictList Author")

	t.Run("ValidPathsOnList", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMaskStrict(ctx, "name,display_name")
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(readMaskCtx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
		require.NotEmpty(t, listAuthorsResponse.Authors[0].Name)
		require.NotEmpty(t, listAuthorsResponse.Authors[0].DisplayName)
		require.Empty(t, listAuthorsResponse.Authors[0].Biography)
	})

	t.Run("InvalidPathOnListReturnsError", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMaskStrict(ctx, "name,bad_field")
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
		}
		_, err := libraryServiceClient.ListAuthors(readMaskCtx, listAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestReadMask_WithPagination(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	for i := range 3 {
		createTestAuthor(t, organizationParent, fmt.Sprintf("ReadMask Paginate %02d", i))
	}

	readMaskCtx := middleware.WithReadMask(ctx, "name,display_name")
	var allAuthors []*librarypb.Author
	pageToken := ""
	for {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:    organizationParent,
			PageSize:  1,
			PageToken: pageToken,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(readMaskCtx, listAuthorsRequest)
		require.NoError(t, err)
		allAuthors = append(allAuthors, listAuthorsResponse.Authors...)
		if listAuthorsResponse.NextPageToken == "" {
			break
		}
		pageToken = listAuthorsResponse.NextPageToken
	}
	require.Len(t, allAuthors, 3)
	for _, author := range allAuthors {
		require.NotEmpty(t, author.Name)
		require.NotEmpty(t, author.DisplayName)
		require.Empty(t, author.Biography)
		require.Empty(t, author.EmailAddress)
		require.Nil(t, author.Metadata)
	}
}

func TestReadMask_ColumnNameReplacement(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	createShelfRequest := &libraryservicepb.CreateShelfRequest{
		Parent: organizationParent,
		Shelf: &librarypb.Shelf{
			DisplayName:     "ReadMask ColName Shelf",
			Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
			ExternalId:      "ext-readmask-123",
			CorrelationId_2: "corr-readmask-456",
			Metadata:        &librarypb.ShelfMetadata{Capacity: 75},
		},
	}
	createdShelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
	require.NoError(t, err)

	t.Run("ExternalId", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,external_id")
		getShelfRequest := &libraryservicepb.GetShelfRequest{
			Name: createdShelf.Name,
		}
		gotShelf, err := libraryServiceClient.GetShelf(readMaskCtx, getShelfRequest)
		require.NoError(t, err)
		require.Equal(t, createdShelf.Name, gotShelf.Name)
		require.Equal(t, "ext-readmask-123", gotShelf.ExternalId)
		require.Empty(t, gotShelf.DisplayName)
		require.Empty(t, gotShelf.CorrelationId_2)
	})

	t.Run("CorrelationId2", func(t *testing.T) {
		t.Parallel()
		readMaskCtx := middleware.WithReadMask(ctx, "name,correlation_id_2")
		getShelfRequest := &libraryservicepb.GetShelfRequest{
			Name: createdShelf.Name,
		}
		gotShelf, err := libraryServiceClient.GetShelf(readMaskCtx, getShelfRequest)
		require.NoError(t, err)
		require.Equal(t, createdShelf.Name, gotShelf.Name)
		require.Equal(t, "corr-readmask-456", gotShelf.CorrelationId_2)
		require.Empty(t, gotShelf.ExternalId)
	})
}

package sat

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/test/library/library_service"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func hookContext() context.Context {
	return metadata.AppendToOutgoingContext(ctx, library_service.MetadataKeyEnableHook, "true")
}

func TestHook_AuthorResponseHook(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("NotTriggeredWithoutHeader", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Hook No Header Author")
		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: author.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(ctx, getAuthorRequest)
		require.NoError(t, err)
		require.NotEqual(t, "true", gotAuthor.Labels["hook/author-response"])
	})

	t.Run("TriggeredWithHeader", func(t *testing.T) {
		t.Parallel()
		hookCtx := hookContext()
		author := validAuthor()
		author.DisplayName = "Hook Header Author"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(hookCtx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "true", createdAuthor.Labels["hook/author-response"])

		getAuthorRequest := &libraryservicepb.GetAuthorRequest{
			Name: createdAuthor.Name,
		}
		gotAuthor, err := libraryServiceClient.GetAuthor(hookCtx, getAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "true", gotAuthor.Labels["hook/author-response"])
	})

	t.Run("AppliedOnListResponse", func(t *testing.T) {
		t.Parallel()
		hookCtx := hookContext()
		author := validAuthor()
		author.DisplayName = "Hook List Author Unique 9988"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(hookCtx, createAuthorRequest)
		require.NoError(t, err)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "Hook List Author Unique 9988"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(hookCtx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
		require.Equal(t, "true", listAuthorsResponse.Authors[0].Labels["hook/author-response"])
	})
}

func TestHook_BookRequestHook(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Hook Book Author")
	shelf := createTestShelf(t, organizationParent, "Hook Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("NotTriggeredWithoutHeader", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Hook No Header Book")
		require.NotEqual(t, "true", book.Labels["hook/book-request"])
	})

	t.Run("TriggeredOnCreate", func(t *testing.T) {
		t.Parallel()
		hookCtx := hookContext()
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:    "Hook Request Book",
				Author:   author.Name,
				Metadata: &librarypb.BookMetadata{},
			},
		}
		createdBook, err := libraryServiceClient.CreateBook(hookCtx, createBookRequest)
		require.NoError(t, err)
		require.Equal(t, "true", createdBook.Labels["hook/book-request"])
		require.Equal(t, "true", createdBook.Labels["hook/book-response"])
	})
}

func TestHook_BookResponseHook(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Hook BookResp Author")
	shelf := createTestShelf(t, organizationParent, "Hook BookResp Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("TriggeredOnGet", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Hook BookResp Get")
		hookCtx := hookContext()
		getBookRequest := &libraryservicepb.GetBookRequest{
			Name: book.Name,
		}
		gotBook, err := libraryServiceClient.GetBook(hookCtx, getBookRequest)
		require.NoError(t, err)
		require.Equal(t, "true", gotBook.Labels["hook/book-response"])
	})

	t.Run("TriggeredOnList", func(t *testing.T) {
		t.Parallel()
		hookCtx := hookContext()
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:    "Hook BookResp List Unique 7766",
				Author:   author.Name,
				Metadata: &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(hookCtx, createBookRequest)
		require.NoError(t, err)

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `title = "Hook BookResp List Unique 7766"`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(hookCtx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 1)
		require.Equal(t, "true", listBooksResponse.Books[0].Labels["hook/book-response"])
	})
}

func TestHook_ShelfMetadataRequestHook(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("TriggeredOnCreateShelf", func(t *testing.T) {
		t.Parallel()
		hookCtx := hookContext()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName: "Hook ShelfMeta Shelf",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Metadata:    &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		createdShelf, err := libraryServiceClient.CreateShelf(hookCtx, createShelfRequest)
		require.NoError(t, err)
		require.Equal(t, "hello", createdShelf.Metadata.Dummy)
	})

	t.Run("NotTriggeredWithoutHeader", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Hook ShelfMeta NoHeader", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		require.Empty(t, shelf.Metadata.Dummy)
	})

	t.Run("NotTriggeredOnNonCreateMethods", func(t *testing.T) {
		t.Parallel()
		hookCtx := hookContext()
		shelf := createTestShelf(t, organizationParent, "Hook ShelfMeta GetTest", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		getShelfRequest := &libraryservicepb.GetShelfRequest{
			Name: shelf.Name,
		}
		gotShelf, err := libraryServiceClient.GetShelf(hookCtx, getShelfRequest)
		require.NoError(t, err)
		require.Empty(t, gotShelf.Metadata.Dummy)
	})
}

func TestHook_ValidationStillApplies(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	hookCtx := hookContext()

	t.Run("InvalidBookStillRejected", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Hook Validation Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Metadata: &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(hookCtx, createBookRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

// go/test/library/library_service/sat/middleware_hooks_test.go
// Add this test function:

func TestHook_ShelfNoteRequestHook(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("TriggeredOnRepeatedField", func(t *testing.T) {
		t.Parallel()
		hookCtx := hookContext()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName: "Hook ShelfNote Repeated",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Metadata: &librarypb.ShelfMetadata{
					Capacity: 10,
					Notes: []*librarypb.ShelfNote{
						{Content: "note1"},
						{Content: "note2"},
					},
				},
			},
		}
		createdShelf, err := libraryServiceClient.CreateShelf(hookCtx, createShelfRequest)
		require.NoError(t, err)
		require.Len(t, createdShelf.Metadata.Notes, 2)
		require.Equal(t, "note1 [hooked]", createdShelf.Metadata.Notes[0].Content)
		require.Equal(t, "note2 [hooked]", createdShelf.Metadata.Notes[1].Content)
	})

	t.Run("TriggeredOnMapField", func(t *testing.T) {
		t.Parallel()
		hookCtx := hookContext()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName: "Hook ShelfNote Map",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Metadata: &librarypb.ShelfMetadata{
					Capacity: 10,
					AuthorToNote: map[string]*librarypb.ShelfNote{
						"author-a": {Content: "mapnote1"},
						"author-b": {Content: "mapnote2"},
					},
				},
			},
		}
		createdShelf, err := libraryServiceClient.CreateShelf(hookCtx, createShelfRequest)
		require.NoError(t, err)
		require.Equal(t, "mapnote1 [hooked]", createdShelf.Metadata.AuthorToNote["author-a"].Content)
		require.Equal(t, "mapnote2 [hooked]", createdShelf.Metadata.AuthorToNote["author-b"].Content)
	})

	t.Run("NotTriggeredWithoutHeader", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName: "Hook ShelfNote NoHeader",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Metadata: &librarypb.ShelfMetadata{
					Capacity: 10,
					Notes:    []*librarypb.ShelfNote{{Content: "unchanged"}},
				},
			},
		}
		createdShelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)
		require.Equal(t, "unchanged", createdShelf.Metadata.Notes[0].Content)
	})
}

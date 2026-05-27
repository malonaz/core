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

func TestEtag_CreatePersistsCorrectEtag(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Author", func(t *testing.T) {
		t.Parallel()
		createdAuthor := createTestAuthor(t, organizationParent, "Etag Create Author")
		require.NotEmpty(t, createdAuthor.Etag)

		gotAuthor := getAuthor(t, createdAuthor.Name)
		require.Equal(t, createdAuthor.Etag, gotAuthor.Etag)

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:      createdAuthor.Name,
				Biography: "Updated with correct etag.",
				Etag:      createdAuthor.Etag,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "Updated with correct etag.", updatedAuthor.Biography)
	})

	t.Run("Book", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Etag Create Book Author")
		shelf := createTestShelf(t, organizationParent, "Etag Create Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createdBook := createTestBook(t, shelf.Name, author.Name, "Etag Create Book")
		require.NotEmpty(t, createdBook.Etag)

		gotBook := getBook(t, createdBook.Name)
		require.Equal(t, createdBook.Etag, gotBook.Etag)

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:  createdBook.Name,
				Title: "Etag Create Book Updated",
				Etag:  createdBook.Etag,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
		}
		updatedBook, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		require.NoError(t, err)
		require.Equal(t, "Etag Create Book Updated", updatedBook.Title)
	})
}

func TestEtag_StaleEtagRejected(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Author", func(t *testing.T) {
		t.Parallel()
		createdAuthor := createTestAuthor(t, organizationParent, "Etag Stale Author")
		staleEtag := createdAuthor.Etag

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:      createdAuthor.Name,
				Biography: "Advance etag.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)

		updateAuthorRequest = &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:      createdAuthor.Name,
				Biography: "Should fail with stale etag.",
				Etag:      staleEtag,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		_, err = libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("Book", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Etag Stale Book Author")
		shelf := createTestShelf(t, organizationParent, "Etag Stale Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createdBook := createTestBook(t, shelf.Name, author.Name, "Etag Stale Book")
		staleEtag := createdBook.Etag

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:  createdBook.Name,
				Title: "Advance etag.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
		}
		_, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		require.NoError(t, err)

		updateBookRequest = &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:  createdBook.Name,
				Title: "Should fail.",
				Etag:  staleEtag,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
		}
		_, err = libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})
}

func TestEtag_UpdateReturnsNewEtagAndChains(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	createdAuthor := createTestAuthor(t, organizationParent, "Etag Chain Author")

	updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
		Author: &librarypb.Author{
			Name:      createdAuthor.Name,
			Biography: "First change.",
			Etag:      createdAuthor.Etag,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
	}
	firstUpdate, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
	require.NoError(t, err)
	require.NotEqual(t, createdAuthor.Etag, firstUpdate.Etag)

	gotAuthor := getAuthor(t, createdAuthor.Name)
	require.Equal(t, firstUpdate.Etag, gotAuthor.Etag)

	updateAuthorRequest = &libraryservicepb.UpdateAuthorRequest{
		Author: &librarypb.Author{
			Name:      createdAuthor.Name,
			Biography: "Second change.",
			Etag:      firstUpdate.Etag,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
	}
	secondUpdate, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
	require.NoError(t, err)
	require.NotEqual(t, firstUpdate.Etag, secondUpdate.Etag)
}

func TestEtag_DeleteWithCorrectEtag(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Author", func(t *testing.T) {
		t.Parallel()
		createdAuthor := createTestAuthor(t, organizationParent, "Etag Del Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: createdAuthor.Name,
			Etag: createdAuthor.Etag,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)
	})

	t.Run("Book", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Etag Del Book Author")
		shelf := createTestShelf(t, organizationParent, "Etag Del Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createdBook := createTestBook(t, shelf.Name, author.Name, "Etag Del Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name: createdBook.Name,
			Etag: createdBook.Etag,
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)
	})
}

func TestEtag_DeleteWithStaleEtag(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Author", func(t *testing.T) {
		t.Parallel()
		createdAuthor := createTestAuthor(t, organizationParent, "Etag StaleDel Author")
		staleEtag := createdAuthor.Etag

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:      createdAuthor.Name,
				Biography: "Advance etag.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		_, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: createdAuthor.Name,
			Etag: staleEtag,
		}
		_, err = libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("Book", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Etag StaleDel Book Author")
		shelf := createTestShelf(t, organizationParent, "Etag StaleDel Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createdBook := createTestBook(t, shelf.Name, author.Name, "Etag StaleDel Book")
		staleEtag := createdBook.Etag

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:  createdBook.Name,
				Title: "Advance etag.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
		}
		_, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		require.NoError(t, err)

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name: createdBook.Name,
			Etag: staleEtag,
		}
		_, err = libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})
}

func TestEtag_NoEtagBypassesCheck(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Update", func(t *testing.T) {
		t.Parallel()
		createdAuthor := createTestAuthor(t, organizationParent, "Etag NoEtag Update Author")

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author: &librarypb.Author{
				Name:      createdAuthor.Name,
				Biography: "No etag provided.",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"biography"}},
		}
		updatedAuthor, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "No etag provided.", updatedAuthor.Biography)
	})

	t.Run("Delete", func(t *testing.T) {
		t.Parallel()
		createdAuthor := createTestAuthor(t, organizationParent, "Etag NoEtag Delete Author")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{
			Name: createdAuthor.Name,
		}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)
	})
}

func TestEtag_ChainedBookUpdates(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Etag ChainBook Author")
	shelf := createTestShelf(t, organizationParent, "Etag ChainBook Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "Etag ChainBook")

	currentEtag := book.Etag
	for i := range 5 {
		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:            book.Name,
				PublicationYear: int32(2000 + i),
				Etag:            currentEtag,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"publication_year"}},
		}
		updatedBook, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		require.NoError(t, err)
		require.NotEqual(t, currentEtag, updatedBook.Etag)
		currentEtag = updatedBook.Etag
	}

	gotBook := getBook(t, book.Name)
	require.Equal(t, currentEtag, gotBook.Etag)
	require.Equal(t, int32(2004), gotBook.PublicationYear)
}

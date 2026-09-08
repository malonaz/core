package sat

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

func importableBook(authorName, title string) *librarypb.Book {
	return &librarypb.Book{
		Title:           title,
		Author:          authorName,
		Isbn:            "978-0553293357",
		PublicationYear: 1951,
		PageCount:       244,
		Duration:        durationpb.New(100 * time.Second),
		Labels:          map[string]string{"genre": "scifi"},
		Metadata: &librarypb.BookMetadata{
			Summary:  "Imported in bulk.",
			Language: "en",
		},
	}
}

func importBooks(t *testing.T, parent string, books ...*librarypb.Book) *libraryservicepb.ImportBooksResponse {
	t.Helper()
	response, err := libraryServiceClient.ImportBooks(ctx, &libraryservicepb.ImportBooksRequest{
		Parent: parent,
		Source: &libraryservicepb.ImportBooksRequest_InlineSource_{
			InlineSource: &libraryservicepb.ImportBooksRequest_InlineSource{Books: books},
		},
	})
	require.NoError(t, err)
	return response
}

func TestImport_Books(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Import Author")
	shelf := createTestShelf(t, organizationParent, "Import Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		before := time.Now().UTC()
		books := []*librarypb.Book{
			importableBook(author.Name, "Foundation"),
			importableBook(author.Name, "Foundation and Empire"),
			importableBook(author.Name, "Second Foundation"),
		}
		response := importBooks(t, shelf.Name, books...)
		after := time.Now().UTC()

		require.Len(t, response.Books, 3)
		names := map[string]bool{}
		for i, book := range response.Books {
			require.True(t, strings.HasPrefix(book.Name, shelf.Name+"/books/"), "name %q", book.Name)
			require.False(t, names[book.Name], "duplicate name %q", book.Name)
			names[book.Name] = true
			require.NotEmpty(t, book.Etag)
			require.Equal(t, books[i].Title, book.Title)

			createTime := book.CreateTime.AsTime()
			require.True(t, !createTime.Before(before), "create_time %v should be >= before %v", createTime, before)
			require.True(t, !createTime.After(after), "create_time %v should be <= after %v", createTime, after)
			require.Equal(t, createTime, book.UpdateTime.AsTime())
		}

		// Every imported book is readable, joined columns included.
		for _, imported := range response.Books {
			book, err := libraryServiceClient.GetBook(ctx, &libraryservicepb.GetBookRequest{Name: imported.Name})
			require.NoError(t, err)
			require.Equal(t, imported.Title, book.Title)
			require.Equal(t, imported.Etag, book.Etag)
			require.Equal(t, imported.CreateTime.AsTime(), book.CreateTime.AsTime())
			require.Equal(t, map[string]string{"genre": "scifi"}, book.Labels)
			require.Equal(t, "Imported in bulk.", book.Metadata.Summary)
			require.Equal(t, 100*time.Second, book.Duration.AsDuration())
			// shelf_external_id / shelf_genre come from the parent shelf join.
			require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_FICTION, book.ShelfGenre)

			// The book's singleton child is imported alongside it.
			review, err := libraryServiceClient.GetBookReview(ctx, &libraryservicepb.GetBookReviewRequest{
				Name: imported.Name + "/review",
			})
			require.NoError(t, err)
			require.NotEmpty(t, review.Etag)
		}
	})

	t.Run("PreservesProvidedNames", func(t *testing.T) {
		t.Parallel()
		book := importableBook(author.Name, "Nightfall")
		book.Name = shelf.Name + "/books/nightfall"

		response := importBooks(t, shelf.Name, book)
		require.Len(t, response.Books, 1)
		require.Equal(t, shelf.Name+"/books/nightfall", response.Books[0].Name)

		got, err := libraryServiceClient.GetBook(ctx, &libraryservicepb.GetBookRequest{Name: shelf.Name + "/books/nightfall"})
		require.NoError(t, err)
		require.Equal(t, "Nightfall", got.Title)
	})

	t.Run("AlreadyExists", func(t *testing.T) {
		t.Parallel()
		book := importableBook(author.Name, "The Gods Themselves")
		book.Name = shelf.Name + "/books/the-gods-themselves"
		importBooks(t, shelf.Name, book)

		_, err := libraryServiceClient.ImportBooks(ctx, &libraryservicepb.ImportBooksRequest{
			Parent: shelf.Name,
			Source: &libraryservicepb.ImportBooksRequest_InlineSource_{
				InlineSource: &libraryservicepb.ImportBooksRequest_InlineSource{Books: []*librarypb.Book{book}},
			},
		})
		grpcrequire.Error(t, codes.AlreadyExists, err)
	})

	t.Run("DuplicateWithinBatchRollsBack", func(t *testing.T) {
		t.Parallel()
		first := importableBook(author.Name, "Prelude")
		first.Name = shelf.Name + "/books/prelude"
		second := importableBook(author.Name, "Prelude Again")
		second.Name = shelf.Name + "/books/prelude"

		_, err := libraryServiceClient.ImportBooks(ctx, &libraryservicepb.ImportBooksRequest{
			Parent: shelf.Name,
			Source: &libraryservicepb.ImportBooksRequest_InlineSource_{
				InlineSource: &libraryservicepb.ImportBooksRequest_InlineSource{Books: []*librarypb.Book{first, second}},
			},
		})
		grpcrequire.Error(t, codes.AlreadyExists, err)

		// The whole batch is one COPY, so nothing landed.
		_, err = libraryServiceClient.GetBook(ctx, &libraryservicepb.GetBookRequest{Name: shelf.Name + "/books/prelude"})
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("ValidateOnly", func(t *testing.T) {
		t.Parallel()
		book := importableBook(author.Name, "Robots of Dawn")
		book.Name = shelf.Name + "/books/robots-of-dawn"

		response, err := libraryServiceClient.ImportBooks(ctx, &libraryservicepb.ImportBooksRequest{
			Parent: shelf.Name,
			Source: &libraryservicepb.ImportBooksRequest_InlineSource_{
				InlineSource: &libraryservicepb.ImportBooksRequest_InlineSource{Books: []*librarypb.Book{book}},
			},
			ValidateOnly: true,
		})
		require.NoError(t, err)
		require.Len(t, response.Books, 1)
		require.NotEmpty(t, response.Books[0].Etag)

		_, err = libraryServiceClient.GetBook(ctx, &libraryservicepb.GetBookRequest{Name: shelf.Name + "/books/robots-of-dawn"})
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("NameOutsideParent", func(t *testing.T) {
		t.Parallel()
		otherShelf := createTestShelf(t, organizationParent, "Other Import Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := importableBook(author.Name, "Misfiled")
		book.Name = otherShelf.Name + "/books/misfiled"

		_, err := libraryServiceClient.ImportBooks(ctx, &libraryservicepb.ImportBooksRequest{
			Parent: shelf.Name,
			Source: &libraryservicepb.ImportBooksRequest_InlineSource_{
				InlineSource: &libraryservicepb.ImportBooksRequest_InlineSource{Books: []*librarypb.Book{book}},
			},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("MissingSource", func(t *testing.T) {
		t.Parallel()
		_, err := libraryServiceClient.ImportBooks(ctx, &libraryservicepb.ImportBooksRequest{Parent: shelf.Name})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent", func(t *testing.T) {
		t.Parallel()
		_, err := libraryServiceClient.ImportBooks(ctx, &libraryservicepb.ImportBooksRequest{
			Parent: organizationParent + "/shelves/-",
			Source: &libraryservicepb.ImportBooksRequest_InlineSource_{
				InlineSource: &libraryservicepb.ImportBooksRequest_InlineSource{
					Books: []*librarypb.Book{importableBook(author.Name, "Wildcard")},
				},
			},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Bulk", func(t *testing.T) {
		t.Parallel()
		bulkShelf := createTestShelf(t, organizationParent, "Bulk Import Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		books := make([]*librarypb.Book, 500)
		for i := range books {
			books[i] = importableBook(author.Name, fmt.Sprintf("Bulk Book %d", i))
		}
		response := importBooks(t, bulkShelf.Name, books...)
		require.Len(t, response.Books, 500)

		listed, err := libraryServiceClient.ListBooks(ctx, &libraryservicepb.ListBooksRequest{
			Parent:   bulkShelf.Name,
			PageSize: 1000,
		})
		require.NoError(t, err)
		require.Len(t, listed.Books, 500)
	})
}

// TestImport_Notes covers a multi-pattern resource: the parent decides which
// pattern the batch is named under.
func TestImport_Notes(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	importNotes := func(t *testing.T, parent string, notes ...*librarypb.Note) *libraryservicepb.ImportNotesResponse {
		t.Helper()
		response, err := libraryServiceClient.ImportNotes(ctx, &libraryservicepb.ImportNotesRequest{
			Parent: parent,
			Source: &libraryservicepb.ImportNotesRequest_InlineSource_{
				InlineSource: &libraryservicepb.ImportNotesRequest_InlineSource{Notes: notes},
			},
		})
		require.NoError(t, err)
		return response
	}

	newNote := func(displayName string) *librarypb.Note {
		return &librarypb.Note{DisplayName: displayName, Content: "Imported note."}
	}

	t.Run("OrganizationParent", func(t *testing.T) {
		t.Parallel()
		response := importNotes(t, organizationParent, newNote("Org note A"), newNote("Org note B"))
		require.Len(t, response.Notes, 2)
		for _, note := range response.Notes {
			require.True(t, strings.HasPrefix(note.Name, organizationParent+"/notes/"), "name %q", note.Name)
			got := getNote(t, note.Name)
			require.Equal(t, note.DisplayName, got.DisplayName)
		}
	})

	t.Run("AuthorParent", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Note Import Author")
		response := importNotes(t, author.Name, newNote("Author note"))
		require.Len(t, response.Notes, 1)
		require.True(t, strings.HasPrefix(response.Notes[0].Name, author.Name+"/notes/"), "name %q", response.Notes[0].Name)
		require.Equal(t, "Author note", getNote(t, response.Notes[0].Name).DisplayName)
	})

	t.Run("ShelfParent", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Note Import Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		note := newNote("Shelf note")
		note.Name = shelf.Name + "/notes/shelf-note"
		response := importNotes(t, shelf.Name, note)
		require.Len(t, response.Notes, 1)
		require.Equal(t, shelf.Name+"/notes/shelf-note", response.Notes[0].Name)
		require.Equal(t, "Shelf note", getNote(t, response.Notes[0].Name).DisplayName)
	})

	t.Run("NameFromAnotherPattern", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Mismatched Note Author")
		note := newNote("Mismatched")
		note.Name = author.Name + "/notes/mismatched"

		_, err := libraryServiceClient.ImportNotes(ctx, &libraryservicepb.ImportNotesRequest{
			Parent: organizationParent,
			Source: &libraryservicepb.ImportNotesRequest_InlineSource_{
				InlineSource: &libraryservicepb.ImportNotesRequest_InlineSource{Notes: []*librarypb.Note{note}},
			},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

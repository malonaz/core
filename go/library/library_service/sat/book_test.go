package sat

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/malonaz/core/go/grpc/middleware"
	grpcrequire "github.com/malonaz/core/go/grpc/require"

	libraryservicepb "github.com/malonaz/core/genproto/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/library/v1"
)

func createTestBook(t *testing.T, shelfName, authorName, title string) *librarypb.Book {
	t.Helper()
	createBookRequest := &libraryservicepb.CreateBookRequest{
		Parent: shelfName,
		Book: &librarypb.Book{
			Title:           title,
			Author:          authorName,
			Isbn:            "978-0553293357",
			PublicationYear: 2000,
			PageCount:       200,
			Metadata: &librarypb.BookMetadata{
				Summary:  "A test book.",
				Language: "en",
			},
		},
	}
	book, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
	require.NoError(t, err)
	return book
}

func TestBookCreate(t *testing.T) {
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Book Create Author")
	shelf := createTestShelf(t, organizationParent, "Book Create Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("Success", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:           "Foundation",
				Author:          author.Name,
				Isbn:            "978-0553293357",
				PublicationYear: 1951,
				PageCount:       244,
				Labels:          map[string]string{"genre": "scifi"},
				Metadata: &librarypb.BookMetadata{
					Summary:  "The first novel in Asimov's masterwork.",
					Language: "en",
				},
			},
		}
		createdBook, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)
		require.NotEmpty(t, createdBook.Name)
		require.NotNil(t, createdBook.CreateTime)
		require.NotNil(t, createdBook.UpdateTime)
		require.Equal(t, "Foundation", createdBook.Title)
		require.Equal(t, author.Name, createdBook.Author)
		require.NotEmpty(t, createdBook.Etag)
	})

	t.Run("Protovalidation_MissingTitle", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Author:   author.Name,
				Metadata: &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingAuthor", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:    "Orphan Book",
				Metadata: &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_NegativePageCount", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:     "Negative Pages",
				Author:    author.Name,
				PageCount: -5,
				Metadata:  &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingParent", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Book: &librarypb.Book{
				Title:    "No Parent Book",
				Author:   author.Name,
				Metadata: &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestBookGet(t *testing.T) {
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Book Get Author")
	shelf := createTestShelf(t, organizationParent, "Book Get Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "Get Test Book")

	t.Run("Success", func(t *testing.T) {
		getBookRequest := &libraryservicepb.GetBookRequest{
			Name: book.Name,
		}
		gotBook, err := libraryServiceClient.GetBook(ctx, getBookRequest)
		require.NoError(t, err)
		require.Equal(t, book.Name, gotBook.Name)
		require.Equal(t, "Get Test Book", gotBook.Title)
	})

	t.Run("NotFound", func(t *testing.T) {
		getBookRequest := &libraryservicepb.GetBookRequest{
			Name: shelf.Name + "/books/nonexistent-book",
		}
		_, err := libraryServiceClient.GetBook(ctx, getBookRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestBookUpdate(t *testing.T) {
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Book Update Author")
	shelf := createTestShelf(t, organizationParent, "Book Update Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("AllowedFields", func(t *testing.T) {
		book := createTestBook(t, shelf.Name, author.Name, "Update Allowed Book")

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:            book.Name,
				Title:           "Updated Title",
				PublicationYear: 2025,
				PageCount:       300,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title", "publication_year", "page_count"}},
		}
		updatedBook, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		require.NoError(t, err)
		require.Equal(t, "Updated Title", updatedBook.Title)
		require.Equal(t, int32(2025), updatedBook.PublicationYear)
		require.Equal(t, int32(300), updatedBook.PageCount)
		require.Equal(t, author.Name, updatedBook.Author)
	})

	t.Run("UpdateTimeChanges", func(t *testing.T) {
		book := createTestBook(t, shelf.Name, author.Name, "Update Time Book")

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:  book.Name,
				Title: "Time Check",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
		}
		updatedBook, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		require.NoError(t, err)
		require.True(t, updatedBook.UpdateTime.AsTime().After(book.UpdateTime.AsTime()) ||
			updatedBook.UpdateTime.AsTime().Equal(book.UpdateTime.AsTime()))
	})

	t.Run("EtagChanges", func(t *testing.T) {
		book := createTestBook(t, shelf.Name, author.Name, "Etag Book")
		originalEtag := book.Etag

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:  book.Name,
				Title: "Etag Changed",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
		}
		updatedBook, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		require.NoError(t, err)
		require.NotEqual(t, originalEtag, updatedBook.Etag)
	})

	t.Run("MetadataPartialUpdate", func(t *testing.T) {
		book := createTestBook(t, shelf.Name, author.Name, "Metadata Update Book")

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name: book.Name,
				Metadata: &librarypb.BookMetadata{
					Summary: "Updated summary only.",
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata.summary"}},
		}
		updatedBook, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		require.NoError(t, err)
		require.Equal(t, "Updated summary only.", updatedBook.Metadata.Summary)
		require.Equal(t, "en", updatedBook.Metadata.Language)
	})

	t.Run("UnauthorizedField_Name", func(t *testing.T) {
		book := createTestBook(t, shelf.Name, author.Name, "Unauthorized Name Book")

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name: book.Name,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		}
		_, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("UnauthorizedField_CreateTime", func(t *testing.T) {
		book := createTestBook(t, shelf.Name, author.Name, "Unauthorized CT Book")

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name: book.Name,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"create_time"}},
		}
		_, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingName", func(t *testing.T) {
		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Title: "No Name",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
		}
		_, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestBookDelete(t *testing.T) {
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Book Delete Author")
	shelf := createTestShelf(t, organizationParent, "Book Delete Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("HardDelete", func(t *testing.T) {
		book := createTestBook(t, shelf.Name, author.Name, "Hard Delete Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name: book.Name,
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		getBookRequest := &libraryservicepb.GetBookRequest{
			Name: book.Name,
		}
		_, err = libraryServiceClient.GetBook(ctx, getBookRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("DeleteWithMatchingEtag", func(t *testing.T) {
		book := createTestBook(t, shelf.Name, author.Name, "Etag Delete Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name: book.Name,
			Etag: book.Etag,
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)
	})

	t.Run("DeleteWithWrongEtag", func(t *testing.T) {
		book := createTestBook(t, shelf.Name, author.Name, "Wrong Etag Delete Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name: book.Name,
			Etag: `"wrong-etag"`,
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})

	t.Run("AllowMissing", func(t *testing.T) {
		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name:         shelf.Name + "/books/nonexistent-book-for-delete",
			AllowMissing: true,
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)
	})

	t.Run("NotFound", func(t *testing.T) {
		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name: shelf.Name + "/books/nonexistent-book-err",
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestBookList(t *testing.T) {
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Book List Author")
	shelf := createTestShelf(t, organizationParent, "Book List Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("BasicList", func(t *testing.T) {
		createTestBook(t, shelf.Name, author.Name, "List Book A")
		createTestBook(t, shelf.Name, author.Name, "List Book B")

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listBooksResponse.Books), 2)
	})

	t.Run("FilterByTitle", func(t *testing.T) {
		createTestBook(t, shelf.Name, author.Name, "Unique Title XYZ123")

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `title = "Unique Title XYZ123"`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 1)
		require.Equal(t, "Unique Title XYZ123", listBooksResponse.Books[0].Title)
	})

	t.Run("FilterByAuthorReference", func(t *testing.T) {
		secondAuthor := createTestAuthor(t, organizationParent, "Second List Author")
		createTestBook(t, shelf.Name, secondAuthor.Name, "Second Author Book")

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: fmt.Sprintf(`author = "%s"`, secondAuthor.Name),
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listBooksResponse.Books), 1)
		for _, book := range listBooksResponse.Books {
			require.Equal(t, secondAuthor.Name, book.Author)
		}
	})

	t.Run("FilterByPublicationYear_Comparison", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:           "Old Book",
				Author:          author.Name,
				PublicationYear: 1800,
				Metadata:        &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `publication_year < 1900`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listBooksResponse.Books), 1)
		for _, book := range listBooksResponse.Books {
			require.Less(t, book.PublicationYear, int32(1900))
		}
	})

	t.Run("FilterByISBN", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:    "ISBN Book",
				Author:   author.Name,
				Isbn:     "978-unique-isbn",
				Metadata: &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `isbn = "978-unique-isbn"`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 1)
	})

	t.Run("FilterByMetadataLanguage", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:    "French Book",
				Author:   author.Name,
				Metadata: &librarypb.BookMetadata{Language: "fr-unique"},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `metadata.language = "fr-unique"`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 1)
	})

	t.Run("FilterByLabelsHasKey", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:    "Labeled Book",
				Author:   author.Name,
				Labels:   map[string]string{"unique-book-label": "yes"},
				Metadata: &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `labels:"unique-book-label"`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listBooksResponse.Books), 1)
	})

	t.Run("FilterByLabelsKeyValue", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:    "Label Value Book",
				Author:   author.Name,
				Labels:   map[string]string{"status": "unique-archived"},
				Metadata: &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `labels.status = "unique-archived"`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 1)
	})

	t.Run("FilterWithWildcardString", func(t *testing.T) {
		createTestBook(t, shelf.Name, author.Name, "Wildcard Prefix Book ZZZ")

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `title = "Wildcard Prefix*"`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listBooksResponse.Books), 1)
	})

	t.Run("FilterWithANDandOR", func(t *testing.T) {
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:           "Complex Filter A",
				Author:          author.Name,
				PublicationYear: 2020,
				Metadata:        &librarypb.BookMetadata{Language: "en"},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)

		createBookRequest2 := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:           "Complex Filter B",
				Author:          author.Name,
				PublicationYear: 2021,
				Metadata:        &librarypb.BookMetadata{Language: "en"},
			},
		}
		_, err = libraryServiceClient.CreateBook(ctx, createBookRequest2)
		require.NoError(t, err)

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `(title = "Complex Filter A" OR title = "Complex Filter B") AND publication_year >= 2020`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 2)
	})

	t.Run("FilterWithNOT", func(t *testing.T) {
		createTestBook(t, shelf.Name, author.Name, "NOT Filter Exclude Me")

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `NOT title = "NOT Filter Exclude Me"`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		for _, book := range listBooksResponse.Books {
			require.NotEqual(t, "NOT Filter Exclude Me", book.Title)
		}
	})

	t.Run("FilterPresenceCheck", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `title:*`,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(listBooksResponse.Books), 1)
	})

	t.Run("FilterNotAllowed_PageCount", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `page_count > 100`,
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("FilterNotAllowed_Etag", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `etag = "something"`,
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("FilterInvalidSyntax", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `title = `,
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("OrderByAllowed_Title", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "title asc",
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
	})

	t.Run("OrderByAllowed_PublicationYear", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "publication_year desc",
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
	})

	t.Run("OrderByAllowed_MultipleFields", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "title asc, create_time desc",
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
	})

	t.Run("OrderByNotAllowed_Author", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "author asc",
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("OrderByNotAllowed_PageCount", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "page_count asc",
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("OrderByInvalidSyntax", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:  shelf.Name,
			OrderBy: "title ascending",
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Pagination", func(t *testing.T) {
		paginationShelf := createTestShelf(t, organizationParent, "Pagination Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		for i := range 3 {
			createTestBook(t, paginationShelf.Name, author.Name, fmt.Sprintf("Paginated Book %d", i))
		}

		var allBooks []*librarypb.Book
		pageToken := ""
		for {
			listBooksRequest := &libraryservicepb.ListBooksRequest{
				Parent:    paginationShelf.Name,
				PageSize:  1,
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
		require.Len(t, allBooks, 3)
	})

	t.Run("DefaultOrdering", func(t *testing.T) {
		defaultShelf := createTestShelf(t, organizationParent, "Default Order Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBook(t, defaultShelf.Name, author.Name, "First Created")
		createTestBook(t, defaultShelf.Name, author.Name, "Second Created")

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: defaultShelf.Name,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 2)
		require.True(t,
			listBooksResponse.Books[0].CreateTime.AsTime().After(listBooksResponse.Books[1].CreateTime.AsTime()) ||
				listBooksResponse.Books[0].CreateTime.AsTime().Equal(listBooksResponse.Books[1].CreateTime.AsTime()),
		)
	})

	t.Run("Protovalidation_MissingParent", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_PageSizeTooLarge", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent:   shelf.Name,
			PageSize: 1001,
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestBookFieldMask(t *testing.T) {
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Book Field Mask Author")
	shelf := createTestShelf(t, organizationParent, "Book Field Mask Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "Field Mask Book")

	t.Run("ReturnsOnlyRequestedFields", func(t *testing.T) {
		ctxWithFieldMask := middleware.WithFieldMask(ctx, "name,title")
		getBookRequest := &libraryservicepb.GetBookRequest{
			Name: book.Name,
		}
		maskedBook, err := libraryServiceClient.GetBook(ctxWithFieldMask, getBookRequest)
		require.NoError(t, err)
		require.Equal(t, book.Name, maskedBook.Name)
		require.Equal(t, "Field Mask Book", maskedBook.Title)
		require.Empty(t, maskedBook.Author)
		require.Empty(t, maskedBook.Isbn)
		require.Equal(t, int32(0), maskedBook.PublicationYear)
		require.Nil(t, maskedBook.Metadata)
	})

	t.Run("NestedFieldMask", func(t *testing.T) {
		ctxWithFieldMask := middleware.WithFieldMask(ctx, "name,metadata.language")
		getBookRequest := &libraryservicepb.GetBookRequest{
			Name: book.Name,
		}
		maskedBook, err := libraryServiceClient.GetBook(ctxWithFieldMask, getBookRequest)
		require.NoError(t, err)
		require.Equal(t, book.Name, maskedBook.Name)
		require.Empty(t, maskedBook.Title)
		require.NotNil(t, maskedBook.Metadata)
		require.Equal(t, "en", maskedBook.Metadata.Language)
		require.Empty(t, maskedBook.Metadata.Summary)
	})
}

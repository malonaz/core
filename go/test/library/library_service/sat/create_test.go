package sat

import (
	"strings"
	"sync"
	"testing"
	"time"

	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestCreate_Author(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		before := time.Now().UTC()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: validAuthor(),
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.NotEmpty(t, createdAuthor.Name)
		require.True(t, strings.HasPrefix(createdAuthor.Name, organizationParent+"/authors/"))
		require.Equal(t, "George Orwell", createdAuthor.DisplayName)
		require.Equal(t, "English novelist and essayist.", createdAuthor.Biography)
		require.Equal(t, "george@example.com", createdAuthor.EmailAddress)
		require.Equal(t, "+14155551234", createdAuthor.PhoneNumber)
		require.Equal(t, []string{"mytest@gmail.com", "mytest2@gmail.com"}, createdAuthor.EmailAddresses)
		require.Equal(t, []string{"+33610102030", "+12247704567"}, createdAuthor.PhoneNumbers)
		require.Equal(t, "fiction", createdAuthor.Labels["genre"])
		require.Equal(t, "UK", createdAuthor.Metadata.Country)
		require.NotEmpty(t, createdAuthor.Etag)
		require.Nil(t, createdAuthor.DeleteTime)

		createTime := createdAuthor.CreateTime.AsTime()
		require.True(t, !createTime.Before(before), "create_time %v should be >= before %v", createTime, before)
		require.True(t, !createTime.After(after), "create_time %v should be <= after %v", createTime, after)

		updateTime := createdAuthor.UpdateTime.AsTime()
		require.True(t, !updateTime.Before(before), "update_time %v should be >= before %v", updateTime, before)
		require.True(t, !updateTime.After(after), "update_time %v should be <= after %v", updateTime, after)
	})

	t.Run("WithCustomID", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent:   organizationParent,
			AuthorId: "custom-author-id",
			Author:   validAuthor(),
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, organizationParent+"/authors/custom-author-id", createdAuthor.Name)
	})

	t.Run("GetMatchesCreateResponse", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: validAuthor(),
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		got := getAuthor(t, createdAuthor.Name)
		grpcrequire.Equal(t, createdAuthor, got)
	})

	t.Run("WithoutLabels", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.Labels = nil
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Empty(t, createdAuthor.Labels)
	})

	t.Run("WithoutOptionalFields", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: &librarypb.Author{
				DisplayName:    "Minimal Author",
				EmailAddress:   "minimal@example.com",
				EmailAddresses: []string{"minimal@example.com"},
				Metadata:       &librarypb.AuthorMetadata{},
			},
		}
		createdAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.Empty(t, createdAuthor.Biography)
		require.Empty(t, createdAuthor.PhoneNumber)
		require.Empty(t, createdAuthor.PhoneNumbers)
		require.Empty(t, createdAuthor.Labels)
		require.Empty(t, createdAuthor.Metadata.Country)
	})

	t.Run("Protovalidation_MissingParent", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Author: validAuthor(),
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingAuthor", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingDisplayName", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.DisplayName = ""
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingEmail", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.EmailAddress = ""
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_InvalidEmail", func(t *testing.T) {
		t.Parallel()
		author := validAuthor()
		author.EmailAddress = "not-an-email"
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: author,
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_InvalidAuthorID", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent:   organizationParent,
			AuthorId: "INVALID_UPPERCASE",
			Author:   validAuthor(),
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestCreate_Shelf(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		before := time.Now().UTC()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Science Fiction Classics",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION,
				Labels:          map[string]string{"floor": "2"},
				Metadata:        &librarypb.ShelfMetadata{Capacity: 100},
				CorrelationId_2: "hello",
			},
		}
		createdShelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.NotEmpty(t, createdShelf.Name)
		require.True(t, strings.HasPrefix(createdShelf.Name, organizationParent+"/shelves/"))
		require.Equal(t, "Science Fiction Classics", createdShelf.DisplayName)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION, createdShelf.Genre)
		require.Equal(t, int32(100), createdShelf.Metadata.Capacity)
		require.Equal(t, "2", createdShelf.Labels["floor"])
		require.Nil(t, createdShelf.DeleteTime)

		createTime := createdShelf.CreateTime.AsTime()
		require.True(t, !createTime.Before(before), "create_time %v should be >= before %v", createTime, before)
		require.True(t, !createTime.After(after), "create_time %v should be <= after %v", createTime, after)

		updateTime := createdShelf.UpdateTime.AsTime()
		require.True(t, !updateTime.Before(before), "update_time %v should be >= before %v", updateTime, before)
		require.True(t, !updateTime.After(after), "update_time %v should be <= after %v", updateTime, after)
	})

	t.Run("GetMatchesCreateResponse", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "GetMatch Create Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
				CorrelationId_2: "hello",
			},
		}
		createdShelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		got := getShelf(t, createdShelf.Name)
		grpcrequire.Equal(t, createdShelf, got)
	})

	t.Run("WithColumnRenameFields", func(t *testing.T) {
		t.Parallel()
		before := time.Now().UTC()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "ColName Create Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_HISTORY,
				ExternalId:      "ext-create-123",
				CorrelationId_2: "corr-create-456",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 75},
			},
		}
		createdShelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		after := time.Now().UTC()

		require.NoError(t, err)
		require.Equal(t, "ext-create-123", createdShelf.ExternalId)
		require.Equal(t, "corr-create-456", createdShelf.CorrelationId_2)

		createTime := createdShelf.CreateTime.AsTime()
		require.True(t, !createTime.Before(before))
		require.True(t, !createTime.After(after))

		got := getShelf(t, createdShelf.Name)
		grpcrequire.Equal(t, createdShelf, got)
	})

	t.Run("Protovalidation_GenreUnspecified", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Bad Genre Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_UNSPECIFIED,
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingDisplayName", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_MissingParent", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Shelf: &librarypb.Shelf{
				DisplayName:     "No Parent Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				CorrelationId_2: "hello",
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestCreate_Book(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Create Book Author")
	shelf := createTestShelf(t, organizationParent, "Create Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		before := time.Now().UTC()
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
		after := time.Now().UTC()

		require.NoError(t, err)
		require.NotEmpty(t, createdBook.Name)
		require.True(t, strings.HasPrefix(createdBook.Name, shelf.Name+"/books/"))
		require.Equal(t, "Foundation", createdBook.Title)
		require.Equal(t, author.Name, createdBook.Author)
		require.Equal(t, "978-0553293357", createdBook.Isbn)
		require.Equal(t, int32(1951), createdBook.PublicationYear)
		require.Equal(t, int32(244), createdBook.PageCount)
		require.Equal(t, "scifi", createdBook.Labels["genre"])
		require.Equal(t, "The first novel in Asimov's masterwork.", createdBook.Metadata.Summary)
		require.Equal(t, "en", createdBook.Metadata.Language)
		require.NotEmpty(t, createdBook.Etag)

		createTime := createdBook.CreateTime.AsTime()
		require.True(t, !createTime.Before(before), "create_time %v should be >= before %v", createTime, before)
		require.True(t, !createTime.After(after), "create_time %v should be <= after %v", createTime, after)

		updateTime := createdBook.UpdateTime.AsTime()
		require.True(t, !updateTime.Before(before), "update_time %v should be >= before %v", updateTime, before)
		require.True(t, !updateTime.After(after), "update_time %v should be <= after %v", updateTime, after)
	})

	t.Run("GetMatchesCreateResponse", func(t *testing.T) {
		t.Parallel()
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:    "GetMatch Create Book",
				Author:   author.Name,
				Metadata: &librarypb.BookMetadata{Summary: "test", Language: "en"},
			},
		}
		createdBook, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)

		got := getBook(t, createdBook.Name)
		grpcrequire.Equal(t, createdBook, got)
	})

	t.Run("MinimalFields", func(t *testing.T) {
		t.Parallel()
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:    "Minimal Book",
				Author:   author.Name,
				Metadata: &librarypb.BookMetadata{},
			},
		}
		createdBook, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)
		require.Empty(t, createdBook.Isbn)
		require.Equal(t, int32(0), createdBook.PublicationYear)
		require.Equal(t, int32(0), createdBook.PageCount)
		require.Empty(t, createdBook.Labels)
	})

	t.Run("Protovalidation_MissingTitle", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent: shelf.Name,
			Book: &librarypb.Book{
				Title:    "No Author Book",
				Metadata: &librarypb.BookMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_NegativePageCount", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()
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

func TestCreate_RequestIdempotency(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("SameRequestID_NoResourceID", func(t *testing.T) {
		t.Parallel()
		requestID := uuid.MustNewV7().String()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent:    organizationParent,
			RequestId: requestID,
			Author:    validAuthor(),
		}
		firstAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		secondAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, firstAuthor, secondAuthor)
	})

	t.Run("SameRequestID_SameResourceID", func(t *testing.T) {
		t.Parallel()
		requestID := uuid.MustNewV7().String()
		authorID := "idempotent-" + uuid.MustNewV7().String()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent:    organizationParent,
			RequestId: requestID,
			AuthorId:  authorID,
			Author:    validAuthor(),
		}
		firstAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		secondAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, firstAuthor, secondAuthor)
	})

	t.Run("SameRequestID_DifferentResourceID", func(t *testing.T) {
		t.Parallel()
		requestID := uuid.MustNewV7().String()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent:    organizationParent,
			RequestId: requestID,
			AuthorId:  "idempotent-a-" + uuid.MustNewV7().String(),
			Author:    validAuthor(),
		}
		firstAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		createAuthorRequest.AuthorId = "idempotent-b-" + uuid.MustNewV7().String()
		secondAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, firstAuthor, secondAuthor)
	})

	t.Run("DifferentRequestID_SameResourceID", func(t *testing.T) {
		t.Parallel()
		authorID := "idempotent-dup-" + uuid.MustNewV7().String()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent:    organizationParent,
			RequestId: uuid.MustNewV7().String(),
			AuthorId:  authorID,
			Author:    validAuthor(),
		}
		_, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		createAuthorRequest.RequestId = uuid.MustNewV7().String()
		_, err = libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		grpcrequire.Error(t, codes.AlreadyExists, err)
	})

	t.Run("DifferentRequestID_NoResourceID", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent:    organizationParent,
			RequestId: uuid.MustNewV7().String(),
			Author:    validAuthor(),
		}
		firstAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		createAuthorRequest.RequestId = uuid.MustNewV7().String()
		secondAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.NotEqual(t, firstAuthor.Name, secondAuthor.Name)
	})

	t.Run("NoRequestID_NotIdempotent", func(t *testing.T) {
		t.Parallel()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent: organizationParent,
			Author: validAuthor(),
		}
		firstAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)

		secondAuthor, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
		require.NoError(t, err)
		require.NotEqual(t, firstAuthor.Name, secondAuthor.Name)
	})

	t.Run("Shelf_SameRequestID", func(t *testing.T) {
		t.Parallel()
		requestID := uuid.MustNewV7().String()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent:    organizationParent,
			RequestId: requestID,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Idempotent Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		firstShelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		secondShelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, firstShelf, secondShelf)
	})

	t.Run("Book_SameRequestID", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Idempotent Book Author")
		shelf := createTestShelf(t, organizationParent, "Idempotent Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		requestID := uuid.MustNewV7().String()
		createBookRequest := &libraryservicepb.CreateBookRequest{
			Parent:    shelf.Name,
			RequestId: requestID,
			Book: &librarypb.Book{
				Title:    "Idempotent Book",
				Author:   author.Name,
				Metadata: &librarypb.BookMetadata{},
			},
		}
		firstBook, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)

		secondBook, err := libraryServiceClient.CreateBook(ctx, createBookRequest)
		require.NoError(t, err)
		grpcrequire.Equal(t, firstBook, secondBook)
	})

	t.Run("SameRequestID_ConcurrentCalls", func(t *testing.T) {
		t.Parallel()
		requestID := uuid.MustNewV7().String()
		createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
			Parent:    organizationParent,
			RequestId: requestID,
			Author:    validAuthor(),
		}

		var wg sync.WaitGroup
		authors := make([]*librarypb.Author, 5)
		errs := make([]error, 5)
		for i := range 5 {
			wg.Go(func() {
				authors[i], errs[i] = libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
			})
		}
		wg.Wait()

		var successAuthor *librarypb.Author
		for i := range 5 {
			require.NoError(t, errs[i])
			if successAuthor == nil {
				successAuthor = authors[i]
			}
			grpcrequire.Equal(t, successAuthor, authors[i])
		}
	})
}

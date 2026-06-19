package sat

/*
import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestJoin_ShelfExternalId(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("PopulatedOnCreate", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Join ExtId Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				ExternalId:      "ext-join-001",
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		shelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		author := createTestAuthor(t, organizationParent, "Join ExtId Author")
		book := createTestBook(t, shelf.Name, author.Name, "Join ExtId Book")
		require.Equal(t, "ext-join-001", book.ShelfExternalId)
	})

	t.Run("PopulatedOnGet", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Join ExtId Get Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_HISTORY,
				ExternalId:      "ext-join-get",
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		shelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		author := createTestAuthor(t, organizationParent, "Join ExtId Get Author")
		book := createTestBook(t, shelf.Name, author.Name, "Join ExtId Get Book")

		gotBook := getBook(t, book.Name)
		require.Equal(t, "ext-join-get", gotBook.ShelfExternalId)
	})

	t.Run("EmptyWhenShelfHasNoExternalId", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Join Empty ExtId Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		author := createTestAuthor(t, organizationParent, "Join Empty ExtId Author")
		book := createTestBook(t, shelf.Name, author.Name, "Join Empty ExtId Book")
		require.Empty(t, book.ShelfExternalId)

		gotBook := getBook(t, book.Name)
		require.Empty(t, gotBook.ShelfExternalId)
	})

	t.Run("ReflectsShelfUpdate", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Join ExtId Update Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY,
				ExternalId:      "ext-before-update",
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		shelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		author := createTestAuthor(t, organizationParent, "Join ExtId Update Author")
		book := createTestBook(t, shelf.Name, author.Name, "Join ExtId Update Book")
		require.Equal(t, "ext-before-update", book.ShelfExternalId)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:       shelf.Name,
				ExternalId: "ext-after-update",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"external_id"}},
		}
		_, err = libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		require.NoError(t, err)

		gotBook := getBook(t, book.Name)
		require.Equal(t, "ext-after-update", gotBook.ShelfExternalId)
	})

	t.Run("PopulatedOnList", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Join ExtId List Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION,
				ExternalId:      "ext-join-list",
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		shelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		author := createTestAuthor(t, organizationParent, "Join ExtId List Author")
		createTestBook(t, shelf.Name, author.Name, "Join ExtId List Book A")
		createTestBook(t, shelf.Name, author.Name, "Join ExtId List Book B")

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 2)
		for _, book := range listBooksResponse.Books {
			require.Equal(t, "ext-join-list", book.ShelfExternalId)
		}
	})

	t.Run("PopulatedOnBatchGet", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Join ExtId Batch Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				ExternalId:      "ext-join-batch",
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		shelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		author := createTestAuthor(t, organizationParent, "Join ExtId Batch Author")
		book1 := createTestBook(t, shelf.Name, author.Name, "Join ExtId Batch Book 1")
		book2 := createTestBook(t, shelf.Name, author.Name, "Join ExtId Batch Book 2")

		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{book1.Name, book2.Name},
		}
		batchGetBooksResponse, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBooksResponse.Books, 2)
		for _, book := range batchGetBooksResponse.Books {
			require.Equal(t, "ext-join-batch", book.ShelfExternalId)
		}
	})
}

func TestJoin_ShelfGenre(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("PopulatedOnCreate", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Join Genre Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)
		author := createTestAuthor(t, organizationParent, "Join Genre Author")
		book := createTestBook(t, shelf.Name, author.Name, "Join Genre Book")
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_HISTORY, book.ShelfGenre)
	})

	t.Run("PopulatedOnGet", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Join Genre Get Shelf", librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION)
		author := createTestAuthor(t, organizationParent, "Join Genre Get Author")
		book := createTestBook(t, shelf.Name, author.Name, "Join Genre Get Book")

		gotBook := getBook(t, book.Name)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION, gotBook.ShelfGenre)
	})

	t.Run("ReflectsShelfGenreUpdate", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Join Genre Update Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		author := createTestAuthor(t, organizationParent, "Join Genre Update Author")
		book := createTestBook(t, shelf.Name, author.Name, "Join Genre Update Book")
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_FICTION, book.ShelfGenre)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:  shelf.Name,
				Genre: librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"genre"}},
		}
		_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		require.NoError(t, err)

		gotBook := getBook(t, book.Name)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY, gotBook.ShelfGenre)
	})

	t.Run("PopulatedOnList", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Join Genre List Shelf", librarypb.ShelfGenre_SHELF_GENRE_NON_FICTION)
		author := createTestAuthor(t, organizationParent, "Join Genre List Author")
		createTestBook(t, shelf.Name, author.Name, "Join Genre List Book A")
		createTestBook(t, shelf.Name, author.Name, "Join Genre List Book B")

		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		require.Len(t, listBooksResponse.Books, 2)
		for _, book := range listBooksResponse.Books {
			require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_NON_FICTION, book.ShelfGenre)
		}
	})

	t.Run("PopulatedOnBatchGet", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Join Genre Batch Shelf", librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY)
		author := createTestAuthor(t, organizationParent, "Join Genre Batch Author")
		book1 := createTestBook(t, shelf.Name, author.Name, "Join Genre Batch Book 1")
		book2 := createTestBook(t, shelf.Name, author.Name, "Join Genre Batch Book 2")

		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: shelf.Name,
			Names:  []string{book1.Name, book2.Name},
		}
		batchGetBooksResponse, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBooksResponse.Books, 2)
		for _, book := range batchGetBooksResponse.Books {
			require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY, book.ShelfGenre)
		}
	})
}

func TestJoin_BothFieldsTogether(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("BothPopulated", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Join Both Shelf",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_HISTORY,
				ExternalId:      "ext-both-123",
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		shelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		author := createTestAuthor(t, organizationParent, "Join Both Author")
		book := createTestBook(t, shelf.Name, author.Name, "Join Both Book")
		require.Equal(t, "ext-both-123", book.ShelfExternalId)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_HISTORY, book.ShelfGenre)

		gotBook := getBook(t, book.Name)
		require.Equal(t, "ext-both-123", gotBook.ShelfExternalId)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_HISTORY, gotBook.ShelfGenre)
	})

	t.Run("ExternalIdEmptyGenrePopulated", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Join Mixed Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		author := createTestAuthor(t, organizationParent, "Join Mixed Author")
		book := createTestBook(t, shelf.Name, author.Name, "Join Mixed Book")
		require.Empty(t, book.ShelfExternalId)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_FICTION, book.ShelfGenre)
	})

	t.Run("DifferentShelvesHaveDifferentValues", func(t *testing.T) {
		t.Parallel()
		createShelfRequestA := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Join Diff Shelf A",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				ExternalId:      "ext-diff-a",
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		shelfA, err := libraryServiceClient.CreateShelf(ctx, createShelfRequestA)
		require.NoError(t, err)

		createShelfRequestB := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Join Diff Shelf B",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY,
				ExternalId:      "ext-diff-b",
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		shelfB, err := libraryServiceClient.CreateShelf(ctx, createShelfRequestB)
		require.NoError(t, err)

		author := createTestAuthor(t, organizationParent, "Join Diff Author")

		createBookRequestA := &libraryservicepb.CreateBookRequest{
			Parent: shelfA.Name,
			Book: &librarypb.Book{
				Title:    "Join Diff Book A",
				Author:   author.Name,
				Duration: durationpb.New(100 * time.Second),
				Metadata: &librarypb.BookMetadata{},
			},
		}
		bookA, err := libraryServiceClient.CreateBook(ctx, createBookRequestA)
		require.NoError(t, err)

		createBookRequestB := &libraryservicepb.CreateBookRequest{
			Parent: shelfB.Name,
			Book: &librarypb.Book{
				Title:    "Join Diff Book B",
				Author:   author.Name,
				Duration: durationpb.New(100 * time.Second),
				Metadata: &librarypb.BookMetadata{},
			},
		}
		bookB, err := libraryServiceClient.CreateBook(ctx, createBookRequestB)
		require.NoError(t, err)

		gotBookA := getBook(t, bookA.Name)
		require.Equal(t, "ext-diff-a", gotBookA.ShelfExternalId)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_FICTION, gotBookA.ShelfGenre)

		gotBookB := getBook(t, bookB.Name)
		require.Equal(t, "ext-diff-b", gotBookB.ShelfExternalId)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY, gotBookB.ShelfGenre)
	})

	t.Run("CrossShelfBatchGet", func(t *testing.T) {
		t.Parallel()
		createShelfRequestA := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Join Cross Shelf A",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_HISTORY,
				ExternalId:      "ext-cross-a",
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		shelfA, err := libraryServiceClient.CreateShelf(ctx, createShelfRequestA)
		require.NoError(t, err)

		createShelfRequestB := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     "Join Cross Shelf B",
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION,
				ExternalId:      "ext-cross-b",
				CorrelationId_2: "hello",
				Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		shelfB, err := libraryServiceClient.CreateShelf(ctx, createShelfRequestB)
		require.NoError(t, err)

		author := createTestAuthor(t, organizationParent, "Join Cross Author")
		bookA := createTestBook(t, shelfA.Name, author.Name, "Join Cross Book A")
		bookB := createTestBook(t, shelfB.Name, author.Name, "Join Cross Book B")

		batchGetBooksRequest := &libraryservicepb.BatchGetBooksRequest{
			Parent: "organizations/-/shelves/-",
			Names:  []string{bookA.Name, bookB.Name},
		}
		batchGetBooksResponse, err := libraryServiceClient.BatchGetBooks(ctx, batchGetBooksRequest)
		require.NoError(t, err)
		require.Len(t, batchGetBooksResponse.Books, 2)

		require.Equal(t, "ext-cross-a", batchGetBooksResponse.Books[0].ShelfExternalId)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_HISTORY, batchGetBooksResponse.Books[0].ShelfGenre)

		require.Equal(t, "ext-cross-b", batchGetBooksResponse.Books[1].ShelfExternalId)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION, batchGetBooksResponse.Books[1].ShelfGenre)
	})
}

// Add to the end of go/test/library/library_service/sat/join_test.go

func TestJoin_FilterByShelfGenre(t *testing.T) {
  t.Parallel()
  organizationParent := getOrganizationParent()
  author := createTestAuthor(t, organizationParent, "Join Filter Genre Author")

  shelfFiction := createTestShelf(t, organizationParent, "Join Filter Fiction Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
  shelfHistory := createTestShelf(t, organizationParent, "Join Filter History Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

  createTestBook(t, shelfFiction.Name, author.Name, "Join Filter Fiction Book")
  createTestBook(t, shelfHistory.Name, author.Name, "Join Filter History Book")

  t.Run("ExactMatch", func(t *testing.T) {
    t.Parallel()
    listBooksRequest := &libraryservicepb.ListBooksRequest{
      Parent: "organizations/-/shelves/-",
      Filter: `shelf_genre = SHELF_GENRE_FICTION`,
    }
    listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
    require.NoError(t, err)
    for _, book := range listBooksResponse.Books {
      require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_FICTION, book.ShelfGenre)
    }
  })

  t.Run("NotEqual", func(t *testing.T) {
    t.Parallel()
    listBooksRequest := &libraryservicepb.ListBooksRequest{
      Parent: "organizations/-/shelves/-",
      Filter: `shelf_genre != SHELF_GENRE_FICTION`,
    }
    listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
    require.NoError(t, err)
    for _, book := range listBooksResponse.Books {
      require.NotEqual(t, librarypb.ShelfGenre_SHELF_GENRE_FICTION, book.ShelfGenre)
    }
  })
}

func TestJoin_FilterByShelfExternalId(t *testing.T) {
  t.Parallel()
  organizationParent := getOrganizationParent()
  author := createTestAuthor(t, organizationParent, "Join Filter ExtId Author")

  createShelfRequest := &libraryservicepb.CreateShelfRequest{
    Parent: organizationParent,
    Shelf: &librarypb.Shelf{
      DisplayName:     "Join Filter ExtId Shelf",
      Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
      ExternalId:      "ext-filter-unique-99",
      CorrelationId_2: "hello",
      Metadata:        &librarypb.ShelfMetadata{Capacity: 50},
    },
  }
  shelf, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
  require.NoError(t, err)

  createTestBook(t, shelf.Name, author.Name, "Join Filter ExtId Book")

  t.Run("ExactMatch", func(t *testing.T) {
    t.Parallel()
    listBooksRequest := &libraryservicepb.ListBooksRequest{
      Parent: shelf.Name,
      Filter: `shelf_external_id = "ext-filter-unique-99"`,
    }
    listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
    require.NoError(t, err)
    require.Len(t, listBooksResponse.Books, 1)
    require.Equal(t, "ext-filter-unique-99", listBooksResponse.Books[0].ShelfExternalId)
  })

  t.Run("WildcardMatch", func(t *testing.T) {
    t.Parallel()
    listBooksRequest := &libraryservicepb.ListBooksRequest{
      Parent: shelf.Name,
      Filter: `shelf_external_id = "ext-filter-unique*"`,
    }
    listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
    require.NoError(t, err)
    require.Len(t, listBooksResponse.Books, 1)
  })

  t.Run("Presence", func(t *testing.T) {
    t.Parallel()
    listBooksRequest := &libraryservicepb.ListBooksRequest{
      Parent: shelf.Name,
      Filter: `shelf_external_id:*`,
    }
    listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
    require.NoError(t, err)
    require.Len(t, listBooksResponse.Books, 1)
  })

  t.Run("NoMatch", func(t *testing.T) {
    t.Parallel()
    listBooksRequest := &libraryservicepb.ListBooksRequest{
      Parent: shelf.Name,
      Filter: `shelf_external_id = "nonexistent-ext-id"`,
    }
    listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
    require.NoError(t, err)
    require.Empty(t, listBooksResponse.Books)
  })
}
*/

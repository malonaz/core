package sat

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/malonaz/core/go/aip"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/nats"
	"github.com/malonaz/core/go/uuid"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

const (
	natsEventCheckInterval = 10 * time.Millisecond
	natsEventCheckTimeout  = 100 * time.Millisecond
)

type shelfCreatedEvent struct {
	Shelf *librarypb.Shelf
}

type shelfUpdatedEvent struct {
	Shelf      *librarypb.Shelf
	OldShelf   *librarypb.Shelf
	UpdateMask *fieldmaskpb.FieldMask
}

type shelfDeletedEvent struct {
	Shelf *librarypb.Shelf
}

type authorCreatedEvent struct {
	Author *librarypb.Author
}

type authorUpdatedEvent struct {
	Author     *librarypb.Author
	OldAuthor  *librarypb.Author
	UpdateMask *fieldmaskpb.FieldMask
}

type authorDeletedEvent struct {
	Author *librarypb.Author
}

type bookCreatedEvent struct {
	Book *librarypb.Book
}

type bookUpdatedEvent struct {
	Book       *librarypb.Book
	OldBook    *librarypb.Book
	UpdateMask *fieldmaskpb.FieldMask
}

type bookDeletedEvent struct {
	Book *librarypb.Book
}

func TestNatsEvents_Shelf(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	consumerSuffix := uuid.MustNewV7().String()

	shelfStream := libraryservicepb.GetLibraryServiceShelfStream()

	var mu sync.Mutex
	shelfNameToCreatedEvents := map[string][]*shelfCreatedEvent{}
	shelfNameToUpdatedEvents := map[string][]*shelfUpdatedEvent{}
	shelfNameToDeletedEvents := map[string][]*shelfDeletedEvent{}

	natsClient, err := satEnvironment.GetNatsClient(ctx)
	require.NoError(t, err)

	createdProcessor := nats.NewProcessor(natsClient, &nats.ProcessorConfig{
		Subjects:     []*nats.Subject{shelfStream.Subject("created")},
		ConsumerName: "test-shelf-created-" + consumerSuffix,
	}, func(_ context.Context, messages []*nats.Message[*aippb.ResourceCreatedEvent]) error {
		mu.Lock()
		defer mu.Unlock()
		for _, message := range messages {
			shelf, err := aip.ParseResourceCreatedEvent[*librarypb.Shelf](message.Payload)
			if err != nil {
				panic(err)
			}
			shelfNameToCreatedEvents[shelf.Name] = append(shelfNameToCreatedEvents[shelf.Name], &shelfCreatedEvent{
				Shelf: shelf,
			})
		}
		return nil
	})
	require.NoError(t, createdProcessor.Start(ctx))

	updatedProcessor := nats.NewProcessor(natsClient, &nats.ProcessorConfig{
		Subjects:     []*nats.Subject{shelfStream.Subject("updated")},
		ConsumerName: "test-shelf-updated-" + consumerSuffix,
	}, func(_ context.Context, messages []*nats.Message[*aippb.ResourceUpdatedEvent]) error {
		mu.Lock()
		defer mu.Unlock()
		for _, message := range messages {
			shelf, oldShelf, updateMask, err := aip.ParseResourceUpdatedEvent[*librarypb.Shelf](message.Payload)
			if err != nil {
				panic(err)
			}
			shelfNameToUpdatedEvents[shelf.Name] = append(shelfNameToUpdatedEvents[shelf.Name], &shelfUpdatedEvent{
				Shelf:      shelf,
				OldShelf:   oldShelf,
				UpdateMask: updateMask,
			})
		}
		return nil
	})
	require.NoError(t, updatedProcessor.Start(ctx))

	deletedProcessor := nats.NewProcessor(natsClient, &nats.ProcessorConfig{
		Subjects:     []*nats.Subject{shelfStream.Subject("deleted")},
		ConsumerName: "test-shelf-deleted-" + consumerSuffix,
	}, func(_ context.Context, messages []*nats.Message[*aippb.ResourceDeletedEvent]) error {
		mu.Lock()
		defer mu.Unlock()
		for _, message := range messages {
			shelf, err := aip.ParseResourceDeletedEvent[*librarypb.Shelf](message.Payload)
			if err != nil {
				panic(err)
			}
			shelfNameToDeletedEvents[shelf.Name] = append(shelfNameToDeletedEvents[shelf.Name], &shelfDeletedEvent{
				Shelf: shelf,
			})
		}
		return nil
	})
	require.NoError(t, deletedProcessor.Start(ctx))

	t.Run("CreatedEvent", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats Created Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToCreatedEvents[shelf.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		events := shelfNameToCreatedEvents[shelf.Name]
		require.Len(t, events, 1)
		require.Equal(t, shelf.Name, events[0].Shelf.Name)
		require.Equal(t, "Nats Created Shelf", events[0].Shelf.DisplayName)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_FICTION, events[0].Shelf.Genre)
	})

	t.Run("CreatedEvent_MatchesGet", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats Created MatchGet Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToCreatedEvents[shelf.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		gotShelf := getShelf(t, shelf.Name)

		mu.Lock()
		defer mu.Unlock()
		grpcrequire.Equal(t, gotShelf, shelfNameToCreatedEvents[shelf.Name][0].Shelf)
	})

	t.Run("ValidateOnlyCreate_NoEvent", func(t *testing.T) {
		t.Parallel()
		shelfID := "validate-only-" + uuid.MustNewV7().String()
		expectedName := organizationParent + "/shelves/" + shelfID
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent:       organizationParent,
			ShelfId:      shelfID,
			ValidateOnly: true,
			Shelf: &librarypb.Shelf{
				DisplayName: "ValidateOnly Shelf",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Metadata:    &librarypb.ShelfMetadata{Capacity: 50},
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)

		require.Never(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToCreatedEvents[expectedName]) > 0
		}, natsEventCheckTimeout, natsEventCheckInterval)
	})

	t.Run("FailedCreate_NoEvent", func(t *testing.T) {
		t.Parallel()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName: "Bad Genre Shelf",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_UNSPECIFIED,
				Metadata:    &librarypb.ShelfMetadata{},
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)

		canary := createTestShelf(t, organizationParent, "FailedCreate Canary Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToCreatedEvents[canary.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, shelfNameToCreatedEvents[canary.Name], 1)
	})

	t.Run("UpdatedEvent", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats Updated Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:        shelf.Name,
				DisplayName: "Nats Updated Shelf Changed",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToUpdatedEvents[shelf.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		events := shelfNameToUpdatedEvents[shelf.Name]
		require.Len(t, events, 1)
		require.Equal(t, "Nats Updated Shelf Changed", events[0].Shelf.DisplayName)
		require.Equal(t, "Nats Updated Shelf", events[0].OldShelf.DisplayName)
		require.Equal(t, []string{"display_name"}, events[0].UpdateMask.GetPaths())
	})

	t.Run("UpdatedEvent_MultiplePaths", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats MultiPath Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:        shelf.Name,
				DisplayName: "Nats MultiPath Changed",
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "genre"}},
		}
		_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToUpdatedEvents[shelf.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		event := shelfNameToUpdatedEvents[shelf.Name][0]
		require.Equal(t, "Nats MultiPath Changed", event.Shelf.DisplayName)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY, event.Shelf.Genre)
		require.Equal(t, "Nats MultiPath Shelf", event.OldShelf.DisplayName)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_FICTION, event.OldShelf.Genre)
		require.ElementsMatch(t, []string{"display_name", "genre"}, event.UpdateMask.GetPaths())
	})

	t.Run("UpdatedEvent_NestedMetadata", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats NestedMeta Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:     shelf.Name,
				Metadata: &librarypb.ShelfMetadata{Capacity: 999},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata.capacity"}},
		}
		_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToUpdatedEvents[shelf.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		event := shelfNameToUpdatedEvents[shelf.Name][0]
		require.Equal(t, int32(999), event.Shelf.Metadata.Capacity)
		require.Equal(t, int32(100), event.OldShelf.Metadata.Capacity)
		require.Equal(t, []string{"metadata.capacity"}, event.UpdateMask.GetPaths())
	})

	t.Run("UpdatedEvent_OldResourceMatchesPreUpdateGet", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats OldRes Shelf", librarypb.ShelfGenre_SHELF_GENRE_NON_FICTION)
		preUpdateShelf := getShelf(t, shelf.Name)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name:        shelf.Name,
				DisplayName: "Nats OldRes Changed",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToUpdatedEvents[shelf.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		grpcrequire.Equal(t, preUpdateShelf, shelfNameToUpdatedEvents[shelf.Name][0].OldShelf)
	})

	t.Run("MultipleUpdates_MultipleEvents", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats MultiUpdate Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		for i := range 3 {
			updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
				Shelf: &librarypb.Shelf{
					Name:  shelf.Name,
					Genre: librarypb.ShelfGenre(i + 1),
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"genre"}},
			}
			_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
			require.NoError(t, err)
		}

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToUpdatedEvents[shelf.Name]) >= 3
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, shelfNameToUpdatedEvents[shelf.Name], 3)
	})

	t.Run("FailedUpdate_NoEvent", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats FailedUpdate Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		updateShelfRequest := &libraryservicepb.UpdateShelfRequest{
			Shelf: &librarypb.Shelf{
				Name: shelf.Name,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		}
		_, err := libraryServiceClient.UpdateShelf(ctx, updateShelfRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)

		require.Never(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToUpdatedEvents[shelf.Name]) > 0
		}, natsEventCheckTimeout, natsEventCheckInterval)
	})

	t.Run("DeletedEvent", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats Deleted Shelf", librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToDeletedEvents[shelf.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		events := shelfNameToDeletedEvents[shelf.Name]
		require.Len(t, events, 1)
		require.Equal(t, shelf.Name, events[0].Shelf.Name)
		require.NotNil(t, events[0].Shelf.DeleteTime)
	})

	t.Run("FailedDelete_NotFound_NoEvent", func(t *testing.T) {
		t.Parallel()
		nonexistentName := organizationParent + "/shelves/nats-nonexistent-delete"
		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{
			Name: nonexistentName,
		}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)

		require.Never(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToDeletedEvents[nonexistentName]) > 0
		}, natsEventCheckTimeout, natsEventCheckInterval)
	})

	t.Run("IdempotentDelete_EmitsEvent", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats IdempotentDel Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToDeletedEvents[shelf.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		deleteShelfRequest = &libraryservicepb.DeleteShelfRequest{
			Name:         shelf.Name,
			AllowMissing: true,
		}
		_, err = libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToDeletedEvents[shelf.Name]) >= 2
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, shelfNameToDeletedEvents[shelf.Name], 2)
	})

	t.Run("DoubleDelete_OnlyOneEvent", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Nats DoubleDel Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		_, err = libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToDeletedEvents[shelf.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		require.Never(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(shelfNameToDeletedEvents[shelf.Name]) > 1
		}, natsEventCheckTimeout, natsEventCheckInterval)
	})
}

func TestNatsEvents_Book(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	consumerSuffix := uuid.MustNewV7().String()

	bookStream := libraryservicepb.GetLibraryServiceBookStream()

	var mu sync.Mutex
	bookNameToCreatedEvents := map[string][]*bookCreatedEvent{}
	bookNameToUpdatedEvents := map[string][]*bookUpdatedEvent{}
	bookNameToDeletedEvents := map[string][]*bookDeletedEvent{}

	natsClient, err := satEnvironment.GetNatsClient(ctx)
	require.NoError(t, err)

	createdProcessor := nats.NewProcessor(natsClient, &nats.ProcessorConfig{
		Subjects:     []*nats.Subject{bookStream.Subject("created")},
		ConsumerName: "test-book-created-" + consumerSuffix,
	}, func(_ context.Context, messages []*nats.Message[*aippb.ResourceCreatedEvent]) error {
		mu.Lock()
		defer mu.Unlock()
		for _, message := range messages {
			book, err := aip.ParseResourceCreatedEvent[*librarypb.Book](message.Payload)
			if err != nil {
				panic(err)
			}
			bookNameToCreatedEvents[book.Name] = append(bookNameToCreatedEvents[book.Name], &bookCreatedEvent{
				Book: book,
			})
		}
		return nil
	})
	require.NoError(t, createdProcessor.Start(ctx))

	updatedProcessor := nats.NewProcessor(natsClient, &nats.ProcessorConfig{
		Subjects:     []*nats.Subject{bookStream.Subject("updated")},
		ConsumerName: "test-book-updated-" + consumerSuffix,
	}, func(_ context.Context, messages []*nats.Message[*aippb.ResourceUpdatedEvent]) error {
		mu.Lock()
		defer mu.Unlock()
		for _, message := range messages {
			book, oldBook, updateMask, err := aip.ParseResourceUpdatedEvent[*librarypb.Book](message.Payload)
			if err != nil {
				panic(err)
			}
			bookNameToUpdatedEvents[book.Name] = append(bookNameToUpdatedEvents[book.Name], &bookUpdatedEvent{
				Book:       book,
				OldBook:    oldBook,
				UpdateMask: updateMask,
			})
		}
		return nil
	})
	require.NoError(t, updatedProcessor.Start(ctx))

	deletedProcessor := nats.NewProcessor(natsClient, &nats.ProcessorConfig{
		Subjects:     []*nats.Subject{bookStream.Subject("deleted")},
		ConsumerName: "test-book-deleted-" + consumerSuffix,
	}, func(_ context.Context, messages []*nats.Message[*aippb.ResourceDeletedEvent]) error {
		mu.Lock()
		defer mu.Unlock()
		for _, message := range messages {
			book, err := aip.ParseResourceDeletedEvent[*librarypb.Book](message.Payload)
			if err != nil {
				panic(err)
			}
			bookNameToDeletedEvents[book.Name] = append(bookNameToDeletedEvents[book.Name], &bookDeletedEvent{
				Book: book,
			})
		}
		return nil
	})
	require.NoError(t, deletedProcessor.Start(ctx))

	author := createTestAuthor(t, organizationParent, "Nats Book Author")
	shelf := createTestShelf(t, organizationParent, "Nats Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("CreatedEvent", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Nats Created Book")

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(bookNameToCreatedEvents[book.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		events := bookNameToCreatedEvents[book.Name]
		require.Len(t, events, 1)
		require.Equal(t, book.Name, events[0].Book.Name)
		require.Equal(t, "Nats Created Book", events[0].Book.Title)
		require.Equal(t, author.Name, events[0].Book.Author)
	})

	t.Run("CreatedEvent_MatchesGet", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Nats Created MatchGet Book")

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(bookNameToCreatedEvents[book.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		gotBook := getBook(t, book.Name)

		mu.Lock()
		defer mu.Unlock()
		grpcrequire.Equal(t, gotBook, bookNameToCreatedEvents[book.Name][0].Book)
	})

	t.Run("UpdatedEvent", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Nats Updated Book")

		updateBookRequest := &libraryservicepb.UpdateBookRequest{
			Book: &librarypb.Book{
				Name:            book.Name,
				Title:           "Nats Updated Book Changed",
				PublicationYear: 2025,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title", "publication_year"}},
		}
		_, err := libraryServiceClient.UpdateBook(ctx, updateBookRequest)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(bookNameToUpdatedEvents[book.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		event := bookNameToUpdatedEvents[book.Name][0]
		require.Equal(t, "Nats Updated Book Changed", event.Book.Title)
		require.Equal(t, int32(2025), event.Book.PublicationYear)
		require.Equal(t, "Nats Updated Book", event.OldBook.Title)
		require.ElementsMatch(t, []string{"title", "publication_year"}, event.UpdateMask.GetPaths())
	})

	t.Run("DeletedEvent_HardDelete", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Nats Deleted Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(bookNameToDeletedEvents[book.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		mu.Lock()
		defer mu.Unlock()
		events := bookNameToDeletedEvents[book.Name]
		require.Len(t, events, 1)
		require.Equal(t, book.Name, events[0].Book.Name)
	})

	t.Run("FailedDelete_EtagMismatch_NoEvent", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Nats EtagFail Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{
			Name: book.Name,
			Etag: `"wrong-etag"`,
		}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		grpcrequire.Error(t, codes.Aborted, err)

		require.Never(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(bookNameToDeletedEvents[book.Name]) > 0
		}, natsEventCheckTimeout, natsEventCheckInterval)
	})

	t.Run("DeletedEvent_HardDelete_DoubleDelete_OnlyOneEvent", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Nats DoubleDel Book")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		_, err = libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		grpcrequire.Error(t, codes.NotFound, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(bookNameToDeletedEvents[book.Name]) >= 1
		}, natsEventCheckTimeout, natsEventCheckInterval)

		require.Never(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(bookNameToDeletedEvents[book.Name]) > 1
		}, natsEventCheckTimeout, natsEventCheckInterval)
	})
}

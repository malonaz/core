package sat

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	"github.com/malonaz/core/go/aip"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/malonaz/core/go/nats"
	"github.com/malonaz/core/go/uuid"
)

func batchCreateAuthorRequest(parent, displayName string) *libraryservicepb.CreateAuthorRequest {
	author := validAuthor()
	author.DisplayName = displayName
	return &libraryservicepb.CreateAuthorRequest{Parent: parent, Author: author}
}

func TestBatchCreateAuthors(t *testing.T) {
	t.Parallel()

	t.Run("Success_PreservesOrder", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		response, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent: organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{
				batchCreateAuthorRequest(organizationParent, "Batch Author C"),
				batchCreateAuthorRequest(organizationParent, "Batch Author A"),
				batchCreateAuthorRequest(organizationParent, "Batch Author B"),
			},
		})
		require.NoError(t, err)
		require.Len(t, response.Authors, 3)
		require.Equal(t, "Batch Author C", response.Authors[0].DisplayName)
		require.Equal(t, "Batch Author A", response.Authors[1].DisplayName)
		require.Equal(t, "Batch Author B", response.Authors[2].DisplayName)
		for _, author := range response.Authors {
			require.NotEmpty(t, author.Name)
			require.NotNil(t, author.CreateTime)
			require.NotEmpty(t, author.Etag)
			// Each created author matches a subsequent Get, and its singleton child exists.
			got, err := libraryServiceClient.GetAuthor(ctx, &libraryservicepb.GetAuthorRequest{Name: author.Name})
			require.NoError(t, err)
			grpcrequire.Equal(t, author, got)
			profile := getAuthorProfile(t, author.Name+"/profile")
			require.Equal(t, author.Name+"/profile", profile.Name)
		}
	})

	t.Run("SingleRequest", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		response, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:   organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{batchCreateAuthorRequest(organizationParent, "Solo")},
		})
		require.NoError(t, err)
		require.Len(t, response.Authors, 1)
	})

	t.Run("WithCustomIDs", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		first := batchCreateAuthorRequest(organizationParent, "First")
		first.AuthorId = "custom-a-" + uuid.MustNewV7().String()
		second := batchCreateAuthorRequest(organizationParent, "Second")
		second.AuthorId = "custom-b-" + uuid.MustNewV7().String()
		response, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:   organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{first, second},
		})
		require.NoError(t, err)
		require.Equal(t, organizationParent+"/authors/"+first.AuthorId, response.Authors[0].Name)
		require.Equal(t, organizationParent+"/authors/"+second.AuthorId, response.Authors[1].Name)
	})

	t.Run("WithoutBatchParent", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		response, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Requests: []*libraryservicepb.CreateAuthorRequest{
				batchCreateAuthorRequest(organizationParent, "No Batch Parent"),
			},
		})
		require.NoError(t, err)
		require.Len(t, response.Authors, 1)
	})

	t.Run("ParentMismatch", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		_, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent: organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{
				batchCreateAuthorRequest(organizationParent, "Ok"),
				batchCreateAuthorRequest(getOrganizationParent(), "Other Parent"),
			},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("WildcardParent_Rejected", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		_, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:   "organizations/-",
			Requests: []*libraryservicepb.CreateAuthorRequest{batchCreateAuthorRequest(organizationParent, "Wildcard")},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("SubRequestValidateOnly_Rejected", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		request := batchCreateAuthorRequest(organizationParent, "Validate Only")
		request.ValidateOnly = true
		_, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:   organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{request},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("ValidateOnly_CreatesNothing", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		response, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:       organizationParent,
			ValidateOnly: true,
			Requests: []*libraryservicepb.CreateAuthorRequest{
				batchCreateAuthorRequest(organizationParent, "Preview A"),
				batchCreateAuthorRequest(organizationParent, "Preview B"),
			},
		})
		require.NoError(t, err)
		require.Len(t, response.Authors, 2)
		for _, author := range response.Authors {
			require.NotEmpty(t, author.Name)
			_, err := libraryServiceClient.GetAuthor(ctx, &libraryservicepb.GetAuthorRequest{Name: author.Name})
			grpcrequire.Error(t, codes.NotFound, err)
		}
	})

	t.Run("Protovalidation_EmptyRequests", func(t *testing.T) {
		t.Parallel()
		_, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent: getOrganizationParent(),
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Protovalidation_InvalidSubRequest", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		invalid := batchCreateAuthorRequest(organizationParent, "Invalid")
		invalid.Author.EmailAddress = "not-an-email"
		_, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent: organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{
				batchCreateAuthorRequest(organizationParent, "Valid"),
				invalid,
			},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestBatchCreateAuthors_Atomicity(t *testing.T) {
	t.Parallel()

	t.Run("DuplicateIDInBatch_CreatesNothing", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		authorID := "dup-" + uuid.MustNewV7().String()
		first := batchCreateAuthorRequest(organizationParent, "First")
		first.AuthorId = authorID
		second := batchCreateAuthorRequest(organizationParent, "Second")
		second.AuthorId = authorID
		_, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:   organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{first, second},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)

		listResponse, err := libraryServiceClient.ListAuthors(ctx, &libraryservicepb.ListAuthorsRequest{Parent: organizationParent})
		require.NoError(t, err)
		require.Empty(t, listResponse.Authors)
	})

	t.Run("ExistingResource_CreatesNothing", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		existing := createTestAuthor(t, organizationParent, "Existing")

		collides := batchCreateAuthorRequest(organizationParent, "Collides")
		collides.AuthorId = existing.Name[strings.LastIndex(existing.Name, "/")+1:]
		_, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent: organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{
				batchCreateAuthorRequest(organizationParent, "Would Be New"),
				collides,
			},
		})
		grpcrequire.Error(t, codes.AlreadyExists, err)

		// Only the pre-existing author remains: the batch rolled back entirely.
		listResponse, err := libraryServiceClient.ListAuthors(ctx, &libraryservicepb.ListAuthorsRequest{Parent: organizationParent})
		require.NoError(t, err)
		require.Len(t, listResponse.Authors, 1)
		require.Equal(t, existing.Name, listResponse.Authors[0].Name)
	})
}

func TestBatchCreateAuthors_RequestIdempotency(t *testing.T) {
	t.Parallel()

	t.Run("Replay_NoResourceIDs", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		first := batchCreateAuthorRequest(organizationParent, "Replay A")
		first.RequestId = uuid.MustNewV7().String()
		second := batchCreateAuthorRequest(organizationParent, "Replay B")
		second.RequestId = uuid.MustNewV7().String()
		request := &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:   organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{first, second},
		}
		firstResponse, err := libraryServiceClient.BatchCreateAuthors(ctx, request)
		require.NoError(t, err)

		// Server-generated ids differ on replay; request ids identify the batch.
		secondResponse, err := libraryServiceClient.BatchCreateAuthors(ctx, request)
		require.NoError(t, err)
		grpcrequire.Equal(t, firstResponse, secondResponse)

		listResponse, err := libraryServiceClient.ListAuthors(ctx, &libraryservicepb.ListAuthorsRequest{Parent: organizationParent})
		require.NoError(t, err)
		require.Len(t, listResponse.Authors, 2)
	})

	t.Run("Replay_SameResourceIDs", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		first := batchCreateAuthorRequest(organizationParent, "Replay A")
		first.RequestId = uuid.MustNewV7().String()
		first.AuthorId = "replay-a-" + uuid.MustNewV7().String()
		second := batchCreateAuthorRequest(organizationParent, "Replay B")
		second.RequestId = uuid.MustNewV7().String()
		second.AuthorId = "replay-b-" + uuid.MustNewV7().String()
		request := &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:   organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{first, second},
		}
		firstResponse, err := libraryServiceClient.BatchCreateAuthors(ctx, request)
		require.NoError(t, err)

		secondResponse, err := libraryServiceClient.BatchCreateAuthors(ctx, request)
		require.NoError(t, err)
		grpcrequire.Equal(t, firstResponse, secondResponse)
	})

	t.Run("DifferentRequestID_SameResourceID", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		request := batchCreateAuthorRequest(organizationParent, "Taken")
		request.RequestId = uuid.MustNewV7().String()
		request.AuthorId = "taken-" + uuid.MustNewV7().String()
		_, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:   organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{request},
		})
		require.NoError(t, err)

		request.RequestId = uuid.MustNewV7().String()
		_, err = libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:   organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{request},
		})
		grpcrequire.Error(t, codes.AlreadyExists, err)
	})

	t.Run("DuplicateRequestIDInBatch", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		requestID := uuid.MustNewV7().String()
		first := batchCreateAuthorRequest(organizationParent, "First")
		first.RequestId = requestID
		second := batchCreateAuthorRequest(organizationParent, "Second")
		second.RequestId = requestID
		_, err := libraryServiceClient.BatchCreateAuthors(ctx, &libraryservicepb.BatchCreateAuthorsRequest{
			Parent:   organizationParent,
			Requests: []*libraryservicepb.CreateAuthorRequest{first, second},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

// TestBatchCreateNotes covers a multi-pattern resource: notes under different
// parents can be created in one batch when no batch parent is set.
func TestBatchCreateNotes(t *testing.T) {
	t.Parallel()

	newNoteRequest := func(parent, displayName string) *libraryservicepb.CreateNoteRequest {
		return &libraryservicepb.CreateNoteRequest{
			Parent: parent,
			Note:   &librarypb.Note{DisplayName: displayName, Content: "Batch note content."},
		}
	}

	t.Run("MixedParents", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Note Author")
		shelf := createTestShelf(t, organizationParent, "Note Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		response, err := libraryServiceClient.BatchCreateNotes(ctx, &libraryservicepb.BatchCreateNotesRequest{
			Requests: []*libraryservicepb.CreateNoteRequest{
				newNoteRequest(organizationParent, "Org Note"),
				newNoteRequest(author.Name, "Author Note"),
				newNoteRequest(shelf.Name, "Shelf Note"),
			},
		})
		require.NoError(t, err)
		require.Len(t, response.Notes, 3)
		require.Contains(t, response.Notes[0].Name, organizationParent+"/notes/")
		require.Contains(t, response.Notes[1].Name, author.Name+"/notes/")
		require.Contains(t, response.Notes[2].Name, shelf.Name+"/notes/")
		for _, note := range response.Notes {
			grpcrequire.Equal(t, note, getNote(t, note.Name))
		}
	})

	t.Run("BatchParent_Scoped", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		author := createTestAuthor(t, organizationParent, "Note Author")

		response, err := libraryServiceClient.BatchCreateNotes(ctx, &libraryservicepb.BatchCreateNotesRequest{
			Parent: author.Name,
			Requests: []*libraryservicepb.CreateNoteRequest{
				newNoteRequest(author.Name, "Note 1"),
				newNoteRequest(author.Name, "Note 2"),
			},
		})
		require.NoError(t, err)
		require.Len(t, response.Notes, 2)

		_, err = libraryServiceClient.BatchCreateNotes(ctx, &libraryservicepb.BatchCreateNotesRequest{
			Parent:   author.Name,
			Requests: []*libraryservicepb.CreateNoteRequest{newNoteRequest(organizationParent, "Wrong Parent")},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("InvalidSubRequestParent_ReportsIndex", func(t *testing.T) {
		t.Parallel()
		organizationParent := getOrganizationParent()
		_, err := libraryServiceClient.BatchCreateNotes(ctx, &libraryservicepb.BatchCreateNotesRequest{
			Requests: []*libraryservicepb.CreateNoteRequest{
				newNoteRequest(organizationParent, "Ok"),
				newNoteRequest("books/not-a-note-parent", "Bad"),
			},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
		require.Contains(t, err.Error(), "requests[1]")
	})
}

// TestBatchCreateShelves covers a resource with joined columns and NATS
// created events: one event is published per created shelf.
func TestBatchCreateShelves(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelfStream := librarypb.GetShelfStream()

	var mu sync.Mutex
	shelfNameToCreatedEvents := map[string]int{}
	natsClient, err := satEnvironment.GetNatsClient(ctx)
	require.NoError(t, err)
	createdProcessor := nats.NewProcessor(natsClient, &nats.ProcessorConfig{
		Subjects:     []*nats.Subject{shelfStream.GetCreatedSubject().MustGet()},
		ConsumerName: "test-shelf-batch-created-" + uuid.MustNewV7().String(),
		BatchSize:    100,
	}, func(_ context.Context, message *nats.Message[*aippb.ResourceEvent]) error {
		shelf, err := aip.ParseEventResource[*librarypb.Shelf](message.Payload)
		if err != nil {
			panic(err)
		}
		mu.Lock()
		defer mu.Unlock()
		shelfNameToCreatedEvents[shelf.Name]++
		return nil
	})
	require.NoError(t, createdProcessor.Start(ctx))
	t.Cleanup(createdProcessor.Close)

	newShelfRequest := func(displayName string) *libraryservicepb.CreateShelfRequest {
		return &libraryservicepb.CreateShelfRequest{
			Parent: organizationParent,
			Shelf: &librarypb.Shelf{
				DisplayName:     displayName,
				Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Metadata:        &librarypb.ShelfMetadata{Capacity: 10},
				CorrelationId_2: "batch",
			},
		}
	}
	response, err := libraryServiceClient.BatchCreateShelves(ctx, &libraryservicepb.BatchCreateShelvesRequest{
		Parent:   organizationParent,
		Requests: []*libraryservicepb.CreateShelfRequest{newShelfRequest("Batch Shelf 1"), newShelfRequest("Batch Shelf 2")},
	})
	require.NoError(t, err)
	require.Len(t, response.Shelves, 2)
	for _, shelf := range response.Shelves {
		got, err := libraryServiceClient.GetShelf(ctx, &libraryservicepb.GetShelfRequest{Name: shelf.Name})
		require.NoError(t, err)
		grpcrequire.Equal(t, shelf, got)
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return shelfNameToCreatedEvents[response.Shelves[0].Name] >= 1 && shelfNameToCreatedEvents[response.Shelves[1].Name] >= 1
	}, natsEventCheckTimeout, natsEventCheckInterval)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, shelfNameToCreatedEvents[response.Shelves[0].Name])
	require.Equal(t, 1, shelfNameToCreatedEvents[response.Shelves[1].Name])
}

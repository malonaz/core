package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func deleteAuthor(t *testing.T, name string) *librarypb.Author {
	t.Helper()
	deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: name}
	author, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
	require.NoError(t, err)
	return author
}

func TestUndelete_Author(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("RestoresResource", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Restore Author 71001")
		deleted := deleteAuthor(t, author.Name)
		require.NotNil(t, deleted.DeleteTime)

		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name}
		restored, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		require.NoError(t, err)
		require.Nil(t, restored.DeleteTime)
		require.Equal(t, author.DisplayName, restored.DisplayName)
		require.Equal(t, author.CreateTime.AsTime(), restored.CreateTime.AsTime())
		// Clearing the tombstone moves the etag.
		require.NotEmpty(t, restored.Etag)
		require.NotEqual(t, deleted.Etag, restored.Etag)

		got := getAuthor(t, author.Name)
		require.Nil(t, got.DeleteTime)
		require.Equal(t, restored.Etag, got.Etag)
	})

	t.Run("VisibleInListAgain", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Listed Author 71002")
		deleteAuthor(t, author.Name)

		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: organizationParent,
			Filter: `display_name = "Undelete Listed Author 71002"`,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Empty(t, listAuthorsResponse.Authors)

		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name}
		_, err = libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		require.NoError(t, err)

		listAuthorsResponse, err = libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
		require.Equal(t, author.Name, listAuthorsResponse.Authors[0].Name)
	})

	t.Run("UpdatableAgain", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Updatable Author")
		deleteAuthor(t, author.Name)

		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		require.NoError(t, err)

		updateAuthorRequest := &libraryservicepb.UpdateAuthorRequest{
			Author:     &librarypb.Author{Name: author.Name, DisplayName: "Undelete Updated Author"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		}
		updated, err := libraryServiceClient.UpdateAuthor(ctx, updateAuthorRequest)
		require.NoError(t, err)
		require.Equal(t, "Undelete Updated Author", updated.DisplayName)
	})

	t.Run("DeletableAgain", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Redeletable Author")
		deleteAuthor(t, author.Name)

		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		require.NoError(t, err)

		deleted := deleteAuthor(t, author.Name)
		require.NotNil(t, deleted.DeleteTime)
	})

	t.Run("LiveResource_AlreadyExists", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Live Author")

		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		grpcrequire.Error(t, codes.AlreadyExists, err)

		got := getAuthor(t, author.Name)
		require.Equal(t, author.Etag, got.Etag)
	})

	t.Run("TwiceIsAlreadyExists", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Twice Author")
		deleteAuthor(t, author.Name)

		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		require.NoError(t, err)
		_, err = libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		grpcrequire.Error(t, codes.AlreadyExists, err)
	})

	t.Run("NeverExisted_NotFound", func(t *testing.T) {
		t.Parallel()
		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: organizationParent + "/authors/undelete-nonexistent"}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Wildcard_InvalidArgument", func(t *testing.T) {
		t.Parallel()
		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: organizationParent + "/authors/-"}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("MalformedName_InvalidArgument", func(t *testing.T) {
		t.Parallel()
		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: "not/a/valid/author/name"}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("EmptyName_InvalidArgument", func(t *testing.T) {
		t.Parallel()
		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Etag_Matching", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Etag Author")
		deleted := deleteAuthor(t, author.Name)

		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name, Etag: deleted.Etag}
		restored, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		require.NoError(t, err)
		require.Nil(t, restored.DeleteTime)
	})

	t.Run("Etag_Mismatch_Aborted", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Stale Etag Author")
		deleteAuthor(t, author.Name)

		// The pre-delete etag is stale: deleting moved it.
		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name, Etag: author.Etag}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		grpcrequire.Error(t, codes.Aborted, err)

		got := getAuthor(t, author.Name)
		require.NotNil(t, got.DeleteTime)
	})

	t.Run("Etag_Mismatch_OnLive_AlreadyExists", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Live Stale Etag Author")

		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name, Etag: `"bogus"`}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		grpcrequire.Error(t, codes.AlreadyExists, err)
	})
}

func TestUndelete_Shelf_NoEtag(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("RestoresResource", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Undelete Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		undeleteShelfRequest := &libraryservicepb.UndeleteShelfRequest{Name: shelf.Name}
		restored, err := libraryServiceClient.UndeleteShelf(ctx, undeleteShelfRequest)
		require.NoError(t, err)
		require.Nil(t, restored.DeleteTime)
		require.Equal(t, shelf.DisplayName, restored.DisplayName)
		require.Equal(t, shelf.Genre, restored.Genre)
	})

	t.Run("LiveResource_AlreadyExists", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Undelete Live Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		undeleteShelfRequest := &libraryservicepb.UndeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.UndeleteShelf(ctx, undeleteShelfRequest)
		grpcrequire.Error(t, codes.AlreadyExists, err)
	})

	t.Run("NeverExisted_NotFound", func(t *testing.T) {
		t.Parallel()
		undeleteShelfRequest := &libraryservicepb.UndeleteShelfRequest{Name: organizationParent + "/shelves/undelete-nonexistent"}
		_, err := libraryServiceClient.UndeleteShelf(ctx, undeleteShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

// Undeleting an Author restores its AuthorProfile (lifecycle singleton) but
// leaves its Notes (collection) tombstoned.
func TestUndelete_Descendants(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("LifecycleSingletonRestored", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Profile Author")
		profileName := author.Name + "/profile"
		deleteAuthor(t, author.Name)
		require.NotNil(t, getAuthorProfile(t, profileName).DeleteTime)

		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		require.NoError(t, err)

		profile := getAuthorProfile(t, profileName)
		require.Nil(t, profile.DeleteTime)
	})

	t.Run("CollectionChildrenStayDeleted", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Notes Author")
		note := createTestNote(t, author.Name, "Undelete Cascaded Note")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name, Force: true}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)
		require.NotNil(t, getNote(t, note.Name).DeleteTime)

		undeleteAuthorRequest := &libraryservicepb.UndeleteAuthorRequest{Name: author.Name}
		_, err = libraryServiceClient.UndeleteAuthor(ctx, undeleteAuthorRequest)
		require.NoError(t, err)

		// Children stay recoverable one by one.
		require.NotNil(t, getNote(t, note.Name).DeleteTime)
		undeleteNoteRequest := &libraryservicepb.UndeleteNoteRequest{Name: note.Name}
		restoredNote, err := libraryServiceClient.UndeleteNote(ctx, undeleteNoteRequest)
		require.NoError(t, err)
		require.Nil(t, restoredNote.DeleteTime)
		require.Len(t, listNotes(t, author.Name, ""), 1)
	})

	t.Run("ChildUndeleteUnderDeletedParent", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Undelete Orphan Note Author")
		note := createTestNote(t, author.Name, "Undelete Orphan Note")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name, Force: true}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		// Codegen does not police the parent's state: that is a service decision.
		undeleteNoteRequest := &libraryservicepb.UndeleteNoteRequest{Name: note.Name}
		restoredNote, err := libraryServiceClient.UndeleteNote(ctx, undeleteNoteRequest)
		require.NoError(t, err)
		require.Nil(t, restoredNote.DeleteTime)
		require.NotNil(t, getAuthor(t, author.Name).DeleteTime)
	})
}

func TestUndelete_Note_MultiPattern(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Undelete MultiPattern Author")
	shelf := createTestShelf(t, organizationParent, "Undelete MultiPattern Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	for _, parent := range []struct {
		name   string
		parent string
	}{
		{"UnderOrganization", organizationParent},
		{"UnderAuthor", author.Name},
		{"UnderShelf", shelf.Name},
	} {
		t.Run(parent.name, func(t *testing.T) {
			t.Parallel()
			note := createTestNote(t, parent.parent, "Undelete Note "+parent.name)
			deleteNote(t, note.Name)
			require.NotNil(t, getNote(t, note.Name).DeleteTime)

			undeleteNoteRequest := &libraryservicepb.UndeleteNoteRequest{Name: note.Name}
			restored, err := libraryServiceClient.UndeleteNote(ctx, undeleteNoteRequest)
			require.NoError(t, err)
			require.Nil(t, restored.DeleteTime)
			require.Equal(t, note.Name, restored.Name)
			require.Equal(t, restored.Etag, getNote(t, note.Name).Etag)

			_, err = libraryServiceClient.UndeleteNote(ctx, undeleteNoteRequest)
			grpcrequire.Error(t, codes.AlreadyExists, err)
		})
	}

	t.Run("NeverExisted_NotFound", func(t *testing.T) {
		t.Parallel()
		undeleteNoteRequest := &libraryservicepb.UndeleteNoteRequest{Name: author.Name + "/notes/undelete-nonexistent"}
		_, err := libraryServiceClient.UndeleteNote(ctx, undeleteNoteRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})

	t.Run("Etag_Mismatch_Aborted", func(t *testing.T) {
		t.Parallel()
		note := createTestNote(t, shelf.Name, "Undelete Stale Etag Note")
		deleteNote(t, note.Name)

		undeleteNoteRequest := &libraryservicepb.UndeleteNoteRequest{Name: note.Name, Etag: note.Etag}
		_, err := libraryServiceClient.UndeleteNote(ctx, undeleteNoteRequest)
		grpcrequire.Error(t, codes.Aborted, err)
	})
}

func TestUndelete_Bookmark_OtherService(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Undelete Bookmark Author")
	shelf := createTestShelf(t, organizationParent, "Undelete Bookmark Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "Undelete Bookmark Book")

	bookmark := createTestBookmark(t, book.Name, "Undelete Bookmark")
	deleteBookmark(t, bookmark.Name)

	undeleteBookmarkRequest := &libraryservicepb.UndeleteBookmarkRequest{Name: bookmark.Name}
	restored, err := bookmarkServiceClient.UndeleteBookmark(ctx, undeleteBookmarkRequest)
	require.NoError(t, err)
	require.Nil(t, restored.DeleteTime)
	require.Equal(t, bookmark.DisplayName, restored.DisplayName)

	_, err = bookmarkServiceClient.UndeleteBookmark(ctx, undeleteBookmarkRequest)
	grpcrequire.Error(t, codes.AlreadyExists, err)
}

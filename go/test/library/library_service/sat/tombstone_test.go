package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

// A tombstone is immutable: a redundant Delete must not rewrite it, so the
// etag handed out by the first Delete keeps working for Undelete.

func getBookmark(t *testing.T, name string) *librarypb.Bookmark {
	t.Helper()
	bookmark, err := bookmarkServiceClient.GetBookmark(ctx, &libraryservicepb.GetBookmarkRequest{Name: name})
	require.NoError(t, err)
	return bookmark
}

// Bookmark has no descendants: its soft delete is a single statement outside
// any transaction, the path where a stray write would have been committed.
func TestTombstone_Bookmark(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Tombstone Bookmark Author")
	shelf := createTestShelf(t, organizationParent, "Tombstone Bookmark Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	book := createTestBook(t, shelf.Name, author.Name, "Tombstone Bookmark Book")

	t.Run("RedundantDeleteKeepsEtag", func(t *testing.T) {
		t.Parallel()
		bookmark := createTestBookmark(t, book.Name, "Tombstone Redundant Delete")
		deleted, err := bookmarkServiceClient.DeleteBookmark(ctx, &libraryservicepb.DeleteBookmarkRequest{Name: bookmark.Name})
		require.NoError(t, err)

		_, err = bookmarkServiceClient.DeleteBookmark(ctx, &libraryservicepb.DeleteBookmarkRequest{Name: bookmark.Name})
		grpcrequire.Error(t, codes.NotFound, err)

		tombstone := getBookmark(t, bookmark.Name)
		require.Equal(t, deleted.Etag, tombstone.Etag)
		require.Equal(t, deleted.DeleteTime.AsTime(), tombstone.DeleteTime.AsTime())
	})

	t.Run("AllowMissingKeepsEtag", func(t *testing.T) {
		t.Parallel()
		bookmark := createTestBookmark(t, book.Name, "Tombstone AllowMissing")
		deleted, err := bookmarkServiceClient.DeleteBookmark(ctx, &libraryservicepb.DeleteBookmarkRequest{Name: bookmark.Name})
		require.NoError(t, err)

		again, err := bookmarkServiceClient.DeleteBookmark(ctx, &libraryservicepb.DeleteBookmarkRequest{Name: bookmark.Name, AllowMissing: true})
		require.NoError(t, err)
		require.Equal(t, deleted.Etag, again.Etag)
		require.Equal(t, deleted.Etag, getBookmark(t, bookmark.Name).Etag)
	})

	t.Run("UndeleteWithTombstoneEtagAfterRedundantDelete", func(t *testing.T) {
		t.Parallel()
		bookmark := createTestBookmark(t, book.Name, "Tombstone Undelete")
		deleted, err := bookmarkServiceClient.DeleteBookmark(ctx, &libraryservicepb.DeleteBookmarkRequest{Name: bookmark.Name})
		require.NoError(t, err)
		_, err = bookmarkServiceClient.DeleteBookmark(ctx, &libraryservicepb.DeleteBookmarkRequest{Name: bookmark.Name})
		grpcrequire.Error(t, codes.NotFound, err)

		restored, err := bookmarkServiceClient.UndeleteBookmark(ctx, &libraryservicepb.UndeleteBookmarkRequest{Name: bookmark.Name, Etag: deleted.Etag})
		require.NoError(t, err)
		require.Nil(t, restored.DeleteTime)
	})

	// The row's state is explained before the client's etag: a stale etag on a
	// tombstone is still "already deleted", mirroring Undelete on a live row.
	t.Run("StaleEtagOnTombstone_NotFound", func(t *testing.T) {
		t.Parallel()
		bookmark := createTestBookmark(t, book.Name, "Tombstone Stale Etag")
		_, err := bookmarkServiceClient.DeleteBookmark(ctx, &libraryservicepb.DeleteBookmarkRequest{Name: bookmark.Name})
		require.NoError(t, err)

		_, err = bookmarkServiceClient.DeleteBookmark(ctx, &libraryservicepb.DeleteBookmarkRequest{Name: bookmark.Name, Etag: `"stale"`})
		grpcrequire.Error(t, codes.NotFound, err)

		deleted, err := bookmarkServiceClient.DeleteBookmark(ctx, &libraryservicepb.DeleteBookmarkRequest{Name: bookmark.Name, Etag: `"stale"`, AllowMissing: true})
		require.NoError(t, err)
		require.Equal(t, getBookmark(t, bookmark.Name).Etag, deleted.Etag)
	})

	t.Run("StaleEtagOnLive_Aborted", func(t *testing.T) {
		t.Parallel()
		bookmark := createTestBookmark(t, book.Name, "Tombstone Live Stale Etag")
		_, err := bookmarkServiceClient.DeleteBookmark(ctx, &libraryservicepb.DeleteBookmarkRequest{Name: bookmark.Name, Etag: `"stale"`})
		grpcrequire.Error(t, codes.Aborted, err)
		require.Nil(t, getBookmark(t, bookmark.Name).DeleteTime)
	})
}

// Author has descendants: its soft delete runs in a transaction with the
// cascade, the other code path of the same statement.
func TestTombstone_Author(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("RedundantDeleteKeepsEtag", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Tombstone Author Redundant")
		deleted, err := libraryServiceClient.DeleteAuthor(ctx, &libraryservicepb.DeleteAuthorRequest{Name: author.Name})
		require.NoError(t, err)

		_, err = libraryServiceClient.DeleteAuthor(ctx, &libraryservicepb.DeleteAuthorRequest{Name: author.Name})
		grpcrequire.Error(t, codes.NotFound, err)
		_, err = libraryServiceClient.DeleteAuthor(ctx, &libraryservicepb.DeleteAuthorRequest{Name: author.Name, Etag: `"stale"`})
		grpcrequire.Error(t, codes.NotFound, err)

		require.Equal(t, deleted.Etag, getAuthor(t, author.Name).Etag)

		restored, err := libraryServiceClient.UndeleteAuthor(ctx, &libraryservicepb.UndeleteAuthorRequest{Name: author.Name, Etag: deleted.Etag})
		require.NoError(t, err)
		require.Nil(t, restored.DeleteTime)
	})

	// The tombstoned profile rides on its parent and is never touched by a
	// redundant parent delete either.
	t.Run("RedundantDeleteKeepsSingletonTombstone", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Tombstone Author Singleton")
		_, err := libraryServiceClient.DeleteAuthor(ctx, &libraryservicepb.DeleteAuthorRequest{Name: author.Name})
		require.NoError(t, err)
		profile, err := libraryServiceClient.GetAuthorProfile(ctx, &libraryservicepb.GetAuthorProfileRequest{Name: author.Name + "/profile"})
		require.NoError(t, err)
		require.NotNil(t, profile.DeleteTime)

		_, err = libraryServiceClient.DeleteAuthor(ctx, &libraryservicepb.DeleteAuthorRequest{Name: author.Name})
		grpcrequire.Error(t, codes.NotFound, err)

		profileAgain, err := libraryServiceClient.GetAuthorProfile(ctx, &libraryservicepb.GetAuthorProfileRequest{Name: author.Name + "/profile"})
		require.NoError(t, err)
		require.Equal(t, profile.Etag, profileAgain.Etag)
		require.Equal(t, profile.DeleteTime.AsTime(), profileAgain.DeleteTime.AsTime())
	})
}

// Note is multi-pattern: its soft delete builds the WHERE clause at runtime.
func TestTombstone_Note(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	shelf := createTestShelf(t, organizationParent, "Tombstone Note Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	for _, parent := range []struct{ name, value string }{
		{"OrganizationNote", organizationParent},
		{"ShelfNote", shelf.Name},
	} {
		t.Run(parent.name, func(t *testing.T) {
			t.Parallel()
			note := createTestNote(t, parent.value, "Tombstone Note")
			deleted, err := libraryServiceClient.DeleteNote(ctx, &libraryservicepb.DeleteNoteRequest{Name: note.Name})
			require.NoError(t, err)

			_, err = libraryServiceClient.DeleteNote(ctx, &libraryservicepb.DeleteNoteRequest{Name: note.Name})
			grpcrequire.Error(t, codes.NotFound, err)
			_, err = libraryServiceClient.DeleteNote(ctx, &libraryservicepb.DeleteNoteRequest{Name: note.Name, Etag: `"stale"`})
			grpcrequire.Error(t, codes.NotFound, err)

			got, err := libraryServiceClient.GetNote(ctx, &libraryservicepb.GetNoteRequest{Name: note.Name})
			require.NoError(t, err)
			require.Equal(t, deleted.Etag, got.Etag)

			restored, err := libraryServiceClient.UndeleteNote(ctx, &libraryservicepb.UndeleteNoteRequest{Name: note.Name, Etag: deleted.Etag})
			require.NoError(t, err)
			require.Nil(t, restored.DeleteTime)
		})
	}
}

package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

// The library tree exercised here (soft = has delete_time):
//
//	Shelf (soft) ─┬─ Book (hard) ─┬─ BookReview (hard singleton)
//	              │               └─ Bookmark (soft)
//	              ├─ Note (soft)
//	              └─ ShelfTag (silent: no message, no table)
//	Author (soft) ─┬─ AuthorProfile (soft singleton)
//	               └─ Note (soft)

func createTestBookmark(t *testing.T, bookName, displayName string) *librarypb.Bookmark {
	t.Helper()
	createBookmarkRequest := &libraryservicepb.CreateBookmarkRequest{
		Parent:   bookName,
		Bookmark: &librarypb.Bookmark{PageNumber: 1, DisplayName: displayName},
	}
	bookmark, err := bookmarkServiceClient.CreateBookmark(ctx, createBookmarkRequest)
	require.NoError(t, err)
	return bookmark
}

func deleteBookmark(t *testing.T, name string) {
	t.Helper()
	deleteBookmarkRequest := &libraryservicepb.DeleteBookmarkRequest{Name: name}
	_, err := bookmarkServiceClient.DeleteBookmark(ctx, deleteBookmarkRequest)
	require.NoError(t, err)
}

func deleteNote(t *testing.T, name string) {
	t.Helper()
	deleteNoteRequest := &libraryservicepb.DeleteNoteRequest{Name: name}
	_, err := libraryServiceClient.DeleteNote(ctx, deleteNoteRequest)
	require.NoError(t, err)
}

func requireBookGone(t *testing.T, name string) {
	t.Helper()
	getBookRequest := &libraryservicepb.GetBookRequest{Name: name}
	_, err := libraryServiceClient.GetBook(ctx, getBookRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func requireBookReviewGone(t *testing.T, name string) {
	t.Helper()
	getBookReviewRequest := &libraryservicepb.GetBookReviewRequest{Name: name}
	_, err := libraryServiceClient.GetBookReview(ctx, getBookReviewRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func requireBookmarkGone(t *testing.T, name string) {
	t.Helper()
	getBookmarkRequest := &libraryservicepb.GetBookmarkRequest{Name: name}
	_, err := bookmarkServiceClient.GetBookmark(ctx, getBookmarkRequest)
	grpcrequire.Error(t, codes.NotFound, err)
}

func TestCascadeDelete_Shelf(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Cascade Shelf Author")

	t.Run("NoChildrenDeletesWithoutForce", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Cascade Empty Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		deletedShelf, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)
		require.NotNil(t, deletedShelf.DeleteTime)
	})

	t.Run("LiveBookBlocksDelete", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Cascade Blocked By Book", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, author.Name, "Cascade Blocking Book")

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.FailedPrecondition, err)

		// The rejected delete left nothing behind.
		require.Nil(t, getShelf(t, shelf.Name).DeleteTime)
		require.Equal(t, book.Name, getBook(t, book.Name).Name)
	})

	t.Run("LiveNoteBlocksDelete", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Cascade Blocked By Note", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestNote(t, shelf.Name, "Cascade Blocking Note")

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.FailedPrecondition, err)
	})

	t.Run("SoftDeletedNoteDoesNotBlock", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Cascade Tombstoned Note", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		note := createTestNote(t, shelf.Name, "Cascade Tombstoned Note")
		deleteNote(t, note.Name)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)
	})

	t.Run("ForceCascadesWholeSubtree", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Cascade Forced Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		book := createTestBook(t, shelf.Name, author.Name, "Cascade Forced Book")
		bookmark := createTestBookmark(t, book.Name, "Cascade Forced Bookmark")
		note := createTestNote(t, shelf.Name, "Cascade Forced Note")
		reviewName := book.Name + "/review"
		getBookReview(t, reviewName)

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name, Force: true}
		deletedShelf, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)
		require.NotNil(t, deletedShelf.DeleteTime)

		// Hard-deletable descendants are gone: the book, its singleton review
		// and its bookmarks (soft-deletable, but they cannot outlive the book).
		requireBookGone(t, book.Name)
		requireBookReviewGone(t, reviewName)
		requireBookmarkGone(t, bookmark.Name)

		// Soft-deletable descendants are tombstoned alongside the shelf.
		gotNote := getNote(t, note.Name)
		require.NotNil(t, gotNote.DeleteTime)
		require.Equal(t, deletedShelf.DeleteTime.AsTime(), gotNote.DeleteTime.AsTime())
	})

	t.Run("ForceIsScopedToTheShelf", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Cascade Scoped Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		otherShelf := createTestShelf(t, organizationParent, "Cascade Other Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBook(t, shelf.Name, author.Name, "Cascade Scoped Book")
		otherBook := createTestBook(t, otherShelf.Name, author.Name, "Cascade Other Book")
		otherNote := createTestNote(t, otherShelf.Name, "Cascade Other Note")
		authorNote := createTestNote(t, author.Name, "Cascade Author Note")

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name, Force: true}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		require.Nil(t, getShelf(t, otherShelf.Name).DeleteTime)
		require.Equal(t, otherBook.Name, getBook(t, otherBook.Name).Name)
		require.Nil(t, getNote(t, otherNote.Name).DeleteTime)
		require.Nil(t, getNote(t, authorNote.Name).DeleteTime)
	})

	t.Run("ForceOnAlreadyDeletedIsNotFound", func(t *testing.T) {
		t.Parallel()
		shelf := createTestShelf(t, organizationParent, "Cascade Twice Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
		createTestBook(t, shelf.Name, author.Name, "Cascade Twice Book")

		deleteShelfRequest := &libraryservicepb.DeleteShelfRequest{Name: shelf.Name, Force: true}
		_, err := libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		require.NoError(t, err)

		_, err = libraryServiceClient.DeleteShelf(ctx, deleteShelfRequest)
		grpcrequire.Error(t, codes.NotFound, err)
	})
}

func TestCascadeDelete_Book(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()
	author := createTestAuthor(t, organizationParent, "Cascade Book Author")
	shelf := createTestShelf(t, organizationParent, "Cascade Book Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	t.Run("LiveBookmarkBlocksDelete", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Cascade Blocked Book")
		createTestBookmark(t, book.Name, "Cascade Blocking Bookmark")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		grpcrequire.Error(t, codes.FailedPrecondition, err)

		// The book and its singleton review survived the rejected delete.
		require.Equal(t, book.Name, getBook(t, book.Name).Name)
		getBookReview(t, book.Name+"/review")
	})

	t.Run("SoftDeletedBookmarkIsPurgedWithHardDeletedBook", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Cascade Purge Book")
		bookmark := createTestBookmark(t, book.Name, "Cascade Purged Bookmark")
		deleteBookmark(t, bookmark.Name)

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		requireBookmarkGone(t, bookmark.Name)
	})

	t.Run("ForceHardDeletesSoftDeletableBookmarks", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Cascade Forced Book")
		bookmark := createTestBookmark(t, book.Name, "Cascade Forced Bookmark")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name, Force: true}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		require.NoError(t, err)

		requireBookmarkGone(t, bookmark.Name)
		requireBookReviewGone(t, book.Name+"/review")
	})

	t.Run("EtagMismatchStillAbortsWithForce", func(t *testing.T) {
		t.Parallel()
		book := createTestBook(t, shelf.Name, author.Name, "Cascade Etag Book")
		createTestBookmark(t, book.Name, "Cascade Etag Bookmark")

		deleteBookRequest := &libraryservicepb.DeleteBookRequest{Name: book.Name, Force: true, Etag: `"wrong-etag"`}
		_, err := libraryServiceClient.DeleteBook(ctx, deleteBookRequest)
		grpcrequire.Error(t, codes.Aborted, err)
		require.Equal(t, book.Name, getBook(t, book.Name).Name)
	})
}

func TestCascadeDelete_Author(t *testing.T) {
	t.Parallel()
	organizationParent := getOrganizationParent()

	t.Run("SingletonProfileNeverBlocks", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Cascade Profile Author")
		getAuthorProfile(t, author.Name+"/profile")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
		deletedAuthor, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		profile := getAuthorProfile(t, author.Name+"/profile")
		require.NotNil(t, profile.DeleteTime)
		require.Equal(t, deletedAuthor.DeleteTime.AsTime(), profile.DeleteTime.AsTime())
	})

	t.Run("LiveNoteBlocksDelete", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Cascade Blocked Author")
		createTestNote(t, author.Name, "Cascade Blocking Author Note")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		grpcrequire.Error(t, codes.FailedPrecondition, err)

		// Neither the author nor its singleton profile were touched.
		require.Nil(t, getAuthor(t, author.Name).DeleteTime)
		require.Nil(t, getAuthorProfile(t, author.Name+"/profile").DeleteTime)
	})

	t.Run("ForceSoftDeletesNotesAndProfile", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Cascade Forced Author")
		note := createTestNote(t, author.Name, "Cascade Forced Author Note")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name, Force: true}
		deletedAuthor, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		gotNote := getNote(t, note.Name)
		require.NotNil(t, gotNote.DeleteTime)
		require.Equal(t, deletedAuthor.DeleteTime.AsTime(), gotNote.DeleteTime.AsTime())
		require.NotNil(t, getAuthorProfile(t, author.Name+"/profile").DeleteTime)

		listNotesRequest := &libraryservicepb.ListNotesRequest{Parent: author.Name}
		listNotesResponse, err := libraryServiceClient.ListNotes(ctx, listNotesRequest)
		require.NoError(t, err)
		require.Empty(t, listNotesResponse.Notes)
	})

	t.Run("ForcedCascadeKeepsNoteEtag", func(t *testing.T) {
		t.Parallel()
		author := createTestAuthor(t, organizationParent, "Cascade Etag Author")
		note := createTestNote(t, author.Name, "Cascade Etag Author Note")

		deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: author.Name, Force: true}
		_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
		require.NoError(t, err)

		// Cascaded tombstones only stamp delete_time; a stale client etag on
		// the note still matches, as it does for singleton children.
		require.Equal(t, note.Etag, getNote(t, note.Name).Etag)
	})
}

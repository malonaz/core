package aip

import (
	"testing"

	"github.com/stretchr/testify/require"

	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestRn_SinglePattern(t *testing.T) {
	book, err := librarypb.ParseBookRn("organizations/o1/shelves/s1/books/b1")
	require.NoError(t, err)
	require.Equal(t, "b1", book.ID())
	require.Equal(t, "organizations/o1/shelves/s1", book.Parent())
	require.Equal(t, librarypb.BookRnType, book.Type())
	require.Equal(t, librarypb.BookRnPattern, book.Pattern())
	require.Equal(t, "s1", book.ShelfRn().Shelf)
	require.Equal(t, book.String(), book.ShelfRn().BookRn("b1").String())

	require.True(t, librarypb.MatchBookRn("organizations/o1/shelves/s1/books/b1"))
	require.False(t, librarypb.MatchBookRn("organizations/o1/shelves/s1"))

	_, err = librarypb.ParseBookRn("organizations/o1/shelves/s1")
	require.Error(t, err)

	fromParent, err := librarypb.NewBookRn("organizations/o1/shelves/s1", "b1")
	require.NoError(t, err)
	require.Equal(t, book, fromParent)
	_, err = librarypb.NewBookRn("organizations/o1", "b1")
	require.Error(t, err)

	var rn Rn = book
	require.Equal(t, "organizations/o1/shelves/s1/books/b1", rn.String())
	require.True(t, (&librarypb.BookRn{Organization: Wildcard, Shelf: "s1", Book: "b1"}).ContainsWildcard())
}

func TestRn_MultiPattern(t *testing.T) {
	for name, want := range map[string]librarypb.NoteRn{
		"organizations/o1/notes/n1":            &librarypb.OrganizationNoteRn{Organization: "o1", Note: "n1"},
		"organizations/o1/authors/a1/notes/n1": &librarypb.AuthorNoteRn{Organization: "o1", Author: "a1", Note: "n1"},
		"organizations/o1/shelves/s1/notes/n1": &librarypb.ShelfNoteRn{Organization: "o1", Shelf: "s1", Note: "n1"},
	} {
		got, err := librarypb.ParseNoteRn(name)
		require.NoError(t, err, name)
		require.Equal(t, want, got)
		require.Equal(t, "n1", got.ID())
		require.Equal(t, name, got.String())
		require.True(t, librarypb.MatchNoteRn(name))

		// The same name built from its parent.
		fromParent, err := librarypb.NewNoteRn(got.Parent(), got.ID())
		require.NoError(t, err)
		require.Equal(t, want, fromParent)
	}
	_, err := librarypb.ParseNoteRn("organizations/o1/books/b1/notes/n1")
	require.Error(t, err)
	_, err = librarypb.NewNoteRn("organizations/o1/books/b1", "n1")
	require.Error(t, err)

	author, err := librarypb.ParseAuthorRn("organizations/o1/authors/a1")
	require.NoError(t, err)
	require.Equal(t, "organizations/o1/authors/a1/notes/n1", author.AuthorNoteRn("n1").String())
	require.Equal(t, author, author.AuthorNoteRn("n1").AuthorRn())
}

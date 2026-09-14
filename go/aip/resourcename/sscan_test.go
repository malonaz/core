package resourcename

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScan(t *testing.T) {
	t.Parallel()
	t.Run("no variables", func(t *testing.T) {
		t.Parallel()
		require.NoError(
			t,
			Sscan(
				"publishers",
				"publishers",
			),
		)
	})

	t.Run("single variable", func(t *testing.T) {
		t.Parallel()
		var publisher string
		require.NoError(
			t,
			Sscan(
				"publishers/foo",
				"publishers/{publisher}",
				&publisher,
			),
		)
		require.Equal(t, "foo", publisher)
	})

	t.Run("two variables", func(t *testing.T) {
		t.Parallel()
		var publisher, book string
		require.NoError(
			t,
			Sscan(
				"publishers/foo/books/bar",
				"publishers/{publisher}/books/{book}",
				&publisher,
				&book,
			),
		)
		require.Equal(t, "foo", publisher)
		require.Equal(t, "bar", book)
	})

	t.Run("two variables singleton", func(t *testing.T) {
		t.Parallel()
		var publisher, book string
		require.NoError(
			t,
			Sscan(
				"publishers/foo/books/bar/settings",
				"publishers/{publisher}/books/{book}/settings",
				&publisher,
				&book,
			),
		)
		require.Equal(t, "foo", publisher)
		require.Equal(t, "bar", book)
	})

	t.Run("two variables singleton", func(t *testing.T) {
		t.Parallel()
		var publisher, book string
		require.NoError(
			t,
			Sscan(
				"publishers/foo/books/bar/settings",
				"publishers/{publisher}/books/{book}/settings",
				&publisher,
				&book,
			),
		)
		require.Equal(t, "foo", publisher)
		require.Equal(t, "bar", book)
	})

	t.Run("trailing segments", func(t *testing.T) {
		t.Parallel()
		var publisher, book string
		require.ErrorContains(
			t,
			Sscan(
				"publishers/foo/books/bar/settings",
				"publishers/{publisher}/books/{book}",
				&publisher,
				&book,
			),
			"trailing",
		)
	})

	t.Run("too few variables", func(t *testing.T) {
		t.Parallel()
		var publisher string
		require.ErrorContains(
			t,
			Sscan(
				"publishers/foo/books/bar/settings",
				"publishers/{publisher}/books/{book}",
				&publisher,
			),
			"too few variables",
		)
	})

	t.Run("too many variables", func(t *testing.T) {
		t.Parallel()
		var publisher, book, extra string
		require.ErrorContains(
			t,
			Sscan(
				"publishers/foo/books/bar",
				"publishers/{publisher}/books/{book}",
				&publisher,
				&book,
				&extra,
			),
			"too many variables",
		)
	})
}

//nolint:gochecknoglobals
var benchmarkScanSink string

func BenchmarkScan(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var publisher, book string
		if err := Sscan(
			"publishers/foo/books/bar",
			"publishers/{publisher}/books/{book}",
			&publisher,
			&book,
		); err != nil {
			b.Fatal(err)
		}
		benchmarkScanSink = publisher
	}
}

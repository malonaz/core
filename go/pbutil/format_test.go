package pbutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestTrimEnumPrefix(t *testing.T) {
	tests := []struct {
		name     string
		enum     librarypb.ShelfGenre
		expected string
	}{
		{"Unspecified", librarypb.ShelfGenre_SHELF_GENRE_UNSPECIFIED, "UNSPECIFIED"},
		{"Fiction", librarypb.ShelfGenre_SHELF_GENRE_FICTION, "FICTION"},
		{"History", librarypb.ShelfGenre_SHELF_GENRE_HISTORY, "HISTORY"},
		{"Biography", librarypb.ShelfGenre_SHELF_GENRE_BIOGRAPHY, "BIOGRAPHY"},
		{"ScienceFiction", librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION, "SCIENCE_FICTION"},
		{"NonFiction", librarypb.ShelfGenre_SHELF_GENRE_NON_FICTION, "NON_FICTION"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := TrimEnumPrefix(tc.enum)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestFormatEnumKebab(t *testing.T) {
	tests := []struct {
		name     string
		enum     librarypb.ShelfGenre
		expected string
	}{
		{"Fiction", librarypb.ShelfGenre_SHELF_GENRE_FICTION, "fiction"},
		{"ScienceFiction", librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION, "science-fiction"},
		{"NonFiction", librarypb.ShelfGenre_SHELF_GENRE_NON_FICTION, "non-fiction"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, FormatEnumKebab(tc.enum))
		})
	}
}

func TestFormatEnumSnake(t *testing.T) {
	tests := []struct {
		name     string
		enum     librarypb.ShelfGenre
		expected string
	}{
		{"Fiction", librarypb.ShelfGenre_SHELF_GENRE_FICTION, "fiction"},
		{"ScienceFiction", librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION, "science_fiction"},
		{"NonFiction", librarypb.ShelfGenre_SHELF_GENRE_NON_FICTION, "non_fiction"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, FormatEnumSnake(tc.enum))
		})
	}
}

func TestFormatEnumTitle(t *testing.T) {
	tests := []struct {
		name     string
		enum     librarypb.ShelfGenre
		expected string
	}{
		{"Fiction", librarypb.ShelfGenre_SHELF_GENRE_FICTION, "Fiction"},
		{"ScienceFiction", librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION, "Science Fiction"},
		{"NonFiction", librarypb.ShelfGenre_SHELF_GENRE_NON_FICTION, "Non Fiction"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, FormatEnumTitle(tc.enum))
		})
	}
}

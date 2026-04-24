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

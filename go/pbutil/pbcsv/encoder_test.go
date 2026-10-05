package pbcsv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestEncoder(t *testing.T) {
	encoder := NewEncoderFor[*librarypb.Shelf]()

	require.Equal(t, []string{
		"name", "create_time", "update_time", "delete_time", "display_name", "genre", "external_id",
		"correlation_id_2", "duration", "labels",
		"metadata.capacity", "metadata.notes", "metadata.author_to_note",
		"metadata.open", "metadata.theme", "metadata.location.room", "metadata.location.floor",
		"best_book", "best_book_page_count", "latest_book", "latest_book_title", "secondary_genre",
		"shelf_number", "featured",
		"latest_draft_book", "extra.note", "extra.rank", "total_page_count", "book_count",
		"total_price", "last_book_create_time", "opened_date", "inventory_time",
		"uninventoried_book_count", "oldest_uninventoried_book",
	}, encoder.Header())

	t.Run("set", func(t *testing.T) {
		shelf := &librarypb.Shelf{
			Name:        "organizations/o/shelves/s",
			CreateTime:  timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
			UpdateTime:  timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 500_000_000, time.UTC)),
			DisplayName: "Shelf",
			Genre:       librarypb.ShelfGenre_SHELF_GENRE_HISTORY,
			Duration:    durationpb.New(90 * time.Second),
			Labels:      map[string]string{"b": "2", "a": "1"},
			Metadata: &librarypb.ShelfMetadata{
				Capacity:     10,
				Dummy:        "excluded",
				Notes:        []*librarypb.ShelfNote{{Content: "first"}},
				AuthorToNote: map[string]*librarypb.ShelfNote{"x": {Content: "second"}},
				Open:         true,
				Location:     &librarypb.ShelfLocation{Room: "attic", Floor: 2},
			},
			TotalPrice: &decimal.Decimal{Value: "12.50"},
			OpenedDate: &date.Date{Year: 2026, Month: 3, Day: 4},
		}
		record, err := encoder.Record(shelf)
		require.NoError(t, err)
		require.Equal(t, []string{
			"organizations/o/shelves/s", "2026-01-02T03:04:05Z", "2026-01-02T03:04:05.5Z", "", "Shelf",
			"SHELF_GENRE_HISTORY", "", "", "1m30s", `{"a":"1","b":"2"}`,
			"10", `[{"content":"first"}]`, `{"x":{"content":"second"}}`,
			"true", "SHELF_GENRE_UNSPECIFIED", "attic", "2",
			"", "0", "", "", "SHELF_GENRE_UNSPECIFIED", "0", "false",
			"", "", "", "0", "0",
			"12.50", "", "2026-03-04", "",
			"0", "",
		}, record)
	})

	t.Run("unset parent leaves its columns empty", func(t *testing.T) {
		record, err := encoder.Record(&librarypb.Shelf{})
		require.NoError(t, err)
		require.Equal(t, []string{
			"", "", "", "", "", "SHELF_GENRE_UNSPECIFIED", "", "", "", "",
			"", "", "", "", "", "", "",
			"", "0", "", "", "SHELF_GENRE_UNSPECIFIED", "0", "false",
			"", "", "", "0", "0",
			"", "", "", "",
			"0", "",
		}, record)
	})
}

package sat

import (
	"testing"

	"github.com/stretchr/testify/require"
	datepb "google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/grpc/codes"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	grpcrequire "github.com/malonaz/core/go/grpc/require"
)

// google.type.Date columns, end to end: a DATE is a calendar day with no
// instant, stored as pgtype.Date (go/postgres/date.go), filtered against its
// "YYYY-MM-DD" literal (go/aip/transpiler/postgres/date.go), and ordered as a
// day. Shelf.opened_date is the nullable column, Bookmark.placed_date the
// NOT NULL one.

// testDate is the day every fixture bookmark was placed.
var testDate = &datepb.Date{Year: 2025, Month: 9, Day: 1}

func date(year, month, day int32) *datepb.Date {
	return &datepb.Date{Year: year, Month: month, Day: day}
}

func TestDate_RoundTrip(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()

	t.Run("Nullable", func(t *testing.T) {
		t.Parallel()
		opened := createNullShelf(t, parent, "opened", func(s *librarypb.Shelf) { s.OpenedDate = date(2024, 2, 29) })
		grpcrequire.Equal(t, date(2024, 2, 29), opened.OpenedDate)
		grpcrequire.Equal(t, opened, getShelf(t, opened.Name))

		unset := createNullShelf(t, parent, "unset", nil)
		require.Nil(t, unset.OpenedDate)
		grpcrequire.Equal(t, unset, getShelf(t, unset.Name))
	})

	t.Run("NotNull", func(t *testing.T) {
		t.Parallel()
		shelf := createNullShelf(t, parent, "shelf", nil)
		author := createTestAuthor(t, parent, "Date Author")
		book := createNullBook(t, shelf.Name, author.Name, "book", nil)
		bookmark := createTestBookmark(t, book.Name, "placed")
		grpcrequire.Equal(t, testDate, bookmark.PlacedDate)
		grpcrequire.Equal(t, bookmark, getBookmark(t, bookmark.Name))
	})

	t.Run("NotNullRequired", func(t *testing.T) {
		t.Parallel()
		shelf := createNullShelf(t, parent, "shelf-required", nil)
		author := createTestAuthor(t, parent, "Date Required Author")
		book := createNullBook(t, shelf.Name, author.Name, "book", nil)
		_, err := bookmarkServiceClient.CreateBookmark(ctx, &libraryservicepb.CreateBookmarkRequest{
			Parent:   book.Name,
			Bookmark: &librarypb.Bookmark{PageNumber: 1, DisplayName: "unplaced"},
		})
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestDate_Update(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()

	t.Run("Set", func(t *testing.T) {
		t.Parallel()
		original := createNullShelf(t, parent, "set", nil)
		updated := updateShelf(t, &librarypb.Shelf{Name: original.Name, OpenedDate: date(2025, 9, 1)}, []string{"opened_date"})
		grpcrequire.Equal(t, date(2025, 9, 1), updated.OpenedDate)
		grpcrequire.Equal(t, updated, getShelf(t, original.Name))
	})

	t.Run("Clear", func(t *testing.T) {
		t.Parallel()
		original := createNullShelf(t, parent, "clear", func(s *librarypb.Shelf) { s.OpenedDate = date(2025, 9, 1) })
		updated := updateShelf(t, &librarypb.Shelf{Name: original.Name}, []string{"opened_date"})
		require.Nil(t, updated.OpenedDate)
		grpcrequire.Equal(t, updated, getShelf(t, original.Name))
	})
}

func TestDate_Filter(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createNullShelf(t, parent, "unset", nil)
	createNullShelf(t, parent, "aug", func(s *librarypb.Shelf) { s.OpenedDate = date(2025, 8, 31) })
	createNullShelf(t, parent, "sep", func(s *librarypb.Shelf) { s.OpenedDate = date(2025, 9, 1) })
	createNullShelf(t, parent, "oct", func(s *librarypb.Shelf) { s.OpenedDate = date(2025, 10, 1) })
	list := shelfNames(parent, false)

	// NULL is absent: it differs from every day and orders with none.
	runNullCases(t, list, []nullCase{
		{"Equals", `opened_date = "2025-09-01"`, []string{"sep"}},
		{"NotEquals", `opened_date != "2025-09-01"`, []string{"aug", "oct", "unset"}},
		{"LessThan", `opened_date < "2025-09-01"`, []string{"aug"}},
		{"LessThanOrEqual", `opened_date <= "2025-09-01"`, []string{"aug", "sep"}},
		{"GreaterThan", `opened_date > "2025-09-01"`, []string{"oct"}},
		{"GreaterThanOrEqual", `opened_date >= "2025-09-01"`, []string{"oct", "sep"}},
		{"Range", `opened_date >= "2025-09-01" AND opened_date < "2025-10-01"`, []string{"sep"}},
		{"Presence", `opened_date:*`, []string{"aug", "oct", "sep"}},
		{"NotPresence", `NOT opened_date:*`, []string{"unset"}},
		{"NotEqualsIsNotEquals", `NOT opened_date = "2025-09-01"`, []string{"aug", "oct", "unset"}},
	})

	t.Run("Complement", func(t *testing.T) {
		t.Parallel()
		all := []string{"aug", "oct", "sep", "unset"}
		requireComplement(t, list, all, `opened_date < "2025-09-01"`, `NOT opened_date < "2025-09-01"`)
		requireComplement(t, list, all, `opened_date:*`, `NOT opened_date:*`)
	})

	// A malformed literal is the caller's error, not Postgres's.
	for name, filter := range map[string]string{
		"Malformed":   `opened_date = "2025--0-01"`,
		"Timestamp":   `opened_date = "2025-09-01T00:00:00Z"`,
		"Wildcard":    `opened_date = "2025-*"`,
		"NonExistent": `opened_date = "2025-02-30"`,
		"Number":      `opened_date = 20250901`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := libraryServiceClient.ListShelves(ctx, &libraryservicepb.ListShelvesRequest{Parent: parent, Filter: filter})
			grpcrequire.Error(t, codes.InvalidArgument, err)
		})
	}
}

func TestDate_OrderBy(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	shelf := createNullShelf(t, parent, "shelf", nil)
	author := createTestAuthor(t, parent, "Date Order Author")
	book := createNullBook(t, shelf.Name, author.Name, "book", nil)
	// Created out of day order, and on days whose string order differs from
	// their day order ("2025-10-01" < "2025-9-01" as text would).
	for _, placed := range []struct {
		name string
		date *datepb.Date
	}{
		{"oct", date(2025, 10, 1)},
		{"jan", date(2026, 1, 1)},
		{"sep", date(2025, 9, 1)},
	} {
		_, err := bookmarkServiceClient.CreateBookmark(ctx, &libraryservicepb.CreateBookmarkRequest{
			Parent:   book.Name,
			Bookmark: &librarypb.Bookmark{PageNumber: 1, DisplayName: placed.name, PlacedDate: placed.date},
		})
		require.NoError(t, err)
	}

	names := func(orderBy string) []string {
		response, err := bookmarkServiceClient.ListBookmarks(ctx, &libraryservicepb.ListBookmarksRequest{Parent: book.Name, OrderBy: orderBy})
		require.NoError(t, err)
		names := make([]string, 0, len(response.Bookmarks))
		for _, bookmark := range response.Bookmarks {
			names = append(names, bookmark.DisplayName)
		}
		return names
	}
	require.Equal(t, []string{"sep", "oct", "jan"}, names("placed_date asc"))
	require.Equal(t, []string{"jan", "oct", "sep"}, names("placed_date desc"))
}

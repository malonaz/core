package sat

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

// AIP-160 unset-field (NULL) semantics, end to end. Every group creates rows
// with a field unset next to rows with it set, then asserts the operator
// matrix by display name. See go/aip/transpiler/postgres/null.go.

type nullCase struct {
	name   string
	filter string
	want   []string
}

// lister returns the display names of the rows matching filter.
type lister func(t *testing.T, filter string) []string

func runNullCases(t *testing.T, list lister, cases []nullCase) {
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			require.ElementsMatch(t, c.want, list(t, c.filter))
		})
	}
}

// requireComplement asserts that negated selects exactly the rows filter does
// not: together they partition all.
func requireComplement(t *testing.T, list lister, all []string, filter, negated string) {
	t.Helper()
	require.ElementsMatch(t, all, append(list(t, filter), list(t, negated)...))
}

func createNullShelf(t *testing.T, parent, displayName string, mutate func(*librarypb.Shelf)) *librarypb.Shelf {
	t.Helper()
	shelf := &librarypb.Shelf{
		DisplayName:     displayName,
		Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
		Metadata:        &librarypb.ShelfMetadata{},
		CorrelationId_2: "null-semantics",
	}
	if mutate != nil {
		mutate(shelf)
	}
	created, err := libraryServiceClient.CreateShelf(ctx, &libraryservicepb.CreateShelfRequest{Parent: parent, Shelf: shelf})
	require.NoError(t, err)
	return created
}

func createNullBook(t *testing.T, shelfName, authorName, title string, mutate func(*librarypb.Book)) *librarypb.Book {
	t.Helper()
	book := &librarypb.Book{
		Title:     title,
		Author:    authorName,
		Isbn:      "978-0553293357",
		PageCount: 200,
		Duration:  durationpb.New(100 * time.Second),
		Metadata:  &librarypb.BookMetadata{},
	}
	if mutate != nil {
		mutate(book)
	}
	created, err := libraryServiceClient.CreateBook(ctx, &libraryservicepb.CreateBookRequest{Parent: shelfName, Book: book})
	require.NoError(t, err)
	return created
}

func shelfNames(parent string, showDeleted bool) lister {
	return func(t *testing.T, filter string) []string {
		t.Helper()
		response, err := libraryServiceClient.ListShelves(ctx, &libraryservicepb.ListShelvesRequest{
			Parent:      parent,
			Filter:      filter,
			ShowDeleted: showDeleted,
		})
		require.NoError(t, err)
		names := make([]string, 0, len(response.Shelves))
		for _, shelf := range response.Shelves {
			names = append(names, shelf.DisplayName)
		}
		slices.Sort(names)
		return names
	}
}

func bookTitles(shelfName string) lister {
	return func(t *testing.T, filter string) []string {
		t.Helper()
		response, err := libraryServiceClient.ListBooks(ctx, &libraryservicepb.ListBooksRequest{Parent: shelfName, Filter: filter})
		require.NoError(t, err)
		titles := make([]string, 0, len(response.Books))
		for _, book := range response.Books {
			titles = append(titles, book.Title)
		}
		slices.Sort(titles)
		return titles
	}
}

func searchedBookTitles(shelfName, query string) lister {
	return func(t *testing.T, filter string) []string {
		t.Helper()
		response, err := libraryServiceClient.SearchBooks(ctx, &libraryservicepb.SearchBooksRequest{
			Parent: shelfName,
			Query:  query,
			Filter: filter,
		})
		require.NoError(t, err)
		titles := make([]string, 0, len(response.Books))
		for _, book := range response.Books {
			titles = append(titles, book.Title)
		}
		slices.Sort(titles)
		return titles
	}
}

func TestFilterNull_NullableEnum(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createNullShelf(t, parent, "unset", nil)
	createNullShelf(t, parent, "fiction", func(s *librarypb.Shelf) { s.SecondaryGenre = librarypb.ShelfGenre_SHELF_GENRE_FICTION })
	createNullShelf(t, parent, "history", func(s *librarypb.Shelf) { s.SecondaryGenre = librarypb.ShelfGenre_SHELF_GENRE_HISTORY })
	list := shelfNames(parent, false)

	runNullCases(t, list, []nullCase{
		{"EqualsUnspecified", `secondary_genre = SHELF_GENRE_UNSPECIFIED`, []string{"unset"}},
		{"EqualsValue", `secondary_genre = SHELF_GENRE_FICTION`, []string{"fiction"}},
		{"NotEqualsValue", `secondary_genre != SHELF_GENRE_FICTION`, []string{"history", "unset"}},
		{"NotEqualsUnspecified", `secondary_genre != SHELF_GENRE_UNSPECIFIED`, []string{"fiction", "history"}},
		{"Presence", `secondary_genre:*`, []string{"fiction", "history"}},
		{"NotPresence", `NOT secondary_genre:*`, []string{"unset"}},
		{"NotEquals", `NOT secondary_genre = SHELF_GENRE_FICTION`, []string{"history", "unset"}},
		{"NotNotEquals", `NOT secondary_genre != SHELF_GENRE_FICTION`, []string{"fiction"}},
	})

	t.Run("UnspecifiedEquivalentToNotPresent", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, list(t, `secondary_genre = SHELF_GENRE_UNSPECIFIED`), list(t, `NOT secondary_genre:*`))
	})
}

func TestFilterNull_NullableInt(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createNullShelf(t, parent, "unset", nil)
	createNullShelf(t, parent, "five", func(s *librarypb.Shelf) { s.ShelfNumber = 5 })
	createNullShelf(t, parent, "ten", func(s *librarypb.Shelf) { s.ShelfNumber = 10 })

	runNullCases(t, shelfNames(parent, false), []nullCase{
		{"EqualsZero", `shelf_number = 0`, []string{"unset"}},
		{"NotEqualsZero", `shelf_number != 0`, []string{"five", "ten"}},
		{"NotEqualsValue", `shelf_number != 5`, []string{"ten", "unset"}},
		{"LessThan", `shelf_number < 5`, []string{"unset"}},
		{"LessThanOrEqualZero", `shelf_number <= 0`, []string{"unset"}},
		{"GreaterThanZero", `shelf_number > 0`, []string{"five", "ten"}},
		{"GreaterThanOrEqualZero", `shelf_number >= 0`, []string{"five", "ten", "unset"}},
		{"LessThanZero", `shelf_number < 0`, nil},
		{"GreaterThanNegative", `shelf_number > -1`, []string{"five", "ten", "unset"}},
		{"Presence", `shelf_number:*`, []string{"five", "ten"}},
		{"NotGreaterThanZero", `NOT shelf_number > 0`, []string{"unset"}},
	})
}

func TestFilterNull_NonNullableInt(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	author := createTestAuthor(t, parent, "NonNullableInt Author")
	shelf := createNullShelf(t, parent, "shelf", nil)
	createNullBook(t, shelf.Name, author.Name, "unset", nil)
	// Stored as 0 in a NOT NULL column: must be indistinguishable from unset.
	createNullBook(t, shelf.Name, author.Name, "zero", func(b *librarypb.Book) { b.PublicationYear = 0 })
	createNullBook(t, shelf.Name, author.Name, "y1999", func(b *librarypb.Book) { b.PublicationYear = 1999 })
	createNullBook(t, shelf.Name, author.Name, "y2010", func(b *librarypb.Book) { b.PublicationYear = 2010 })

	runNullCases(t, bookTitles(shelf.Name), []nullCase{
		{"EqualsZero", `publication_year = 0`, []string{"unset", "zero"}},
		{"NotEqualsZero", `publication_year != 0`, []string{"y1999", "y2010"}},
		{"NotEqualsValue", `publication_year != 1999`, []string{"unset", "y2010", "zero"}},
		{"LessThan", `publication_year < 1999`, []string{"unset", "zero"}},
		{"LessThanOrEqualZero", `publication_year <= 0`, []string{"unset", "zero"}},
		{"GreaterThanZero", `publication_year > 0`, []string{"y1999", "y2010"}},
		{"GreaterThanOrEqualZero", `publication_year >= 0`, []string{"unset", "y1999", "y2010", "zero"}},
		{"LessThanZero", `publication_year < 0`, nil},
		{"GreaterThanNegative", `publication_year > -1`, []string{"unset", "y1999", "y2010", "zero"}},
		{"Presence", `publication_year:*`, []string{"y1999", "y2010"}},
		{"NotGreaterThanZero", `NOT publication_year > 0`, []string{"unset", "zero"}},
	})
}

func TestFilterNull_NullableBool(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createNullShelf(t, parent, "unset", nil)
	// An explicit false is the zero value: stored NULL, same as unset.
	createNullShelf(t, parent, "false", func(s *librarypb.Shelf) { s.Featured = false })
	createNullShelf(t, parent, "true", func(s *librarypb.Shelf) { s.Featured = true })

	runNullCases(t, shelfNames(parent, false), []nullCase{
		{"EqualsFalse", `featured = false`, []string{"false", "unset"}},
		{"EqualsTrue", `featured = true`, []string{"true"}},
		{"NotEqualsTrue", `featured != true`, []string{"false", "unset"}},
		{"NotEqualsFalse", `featured != false`, []string{"true"}},
		{"Presence", `featured:*`, []string{"true"}},
		{"NotEqualsTrueNegated", `NOT featured = true`, []string{"false", "unset"}},
	})
}

func TestFilterNull_NullableString(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createNullShelf(t, parent, "unset", nil)
	createNullShelf(t, parent, "x-alpha", func(s *librarypb.Shelf) { s.ExternalId = "x-alpha" })
	createNullShelf(t, parent, "y-beta", func(s *librarypb.Shelf) { s.ExternalId = "y-beta" })

	runNullCases(t, shelfNames(parent, false), []nullCase{
		{"EqualsEmpty", `external_id = ""`, []string{"unset"}},
		{"NotEqualsEmpty", `external_id != ""`, []string{"x-alpha", "y-beta"}},
		{"NotEqualsValue", `external_id != "x-alpha"`, []string{"unset", "y-beta"}},
		{"Wildcard", `external_id = "x*"`, []string{"x-alpha"}},
		{"NotWildcard", `NOT external_id = "x*"`, []string{"unset", "y-beta"}},
		{"Presence", `external_id:*`, []string{"x-alpha", "y-beta"}},
		{"LessThan", `external_id < "x"`, []string{"unset"}},
	})
}

func TestFilterNull_Timestamp(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createNullShelf(t, parent, "live", nil)
	for _, name := range []string{"gone-a", "gone-b"} {
		shelf := createNullShelf(t, parent, name, nil)
		_, err := libraryServiceClient.DeleteShelf(ctx, &libraryservicepb.DeleteShelfRequest{Name: shelf.Name})
		require.NoError(t, err)
	}
	// Read back so the literal carries exactly the stored (microsecond) value.
	response, err := libraryServiceClient.ListShelves(ctx, &libraryservicepb.ListShelvesRequest{
		Parent:      parent,
		Filter:      `display_name = "gone-a"`,
		ShowDeleted: true,
	})
	require.NoError(t, err)
	require.Len(t, response.Shelves, 1)
	deleteTime := response.Shelves[0].DeleteTime.AsTime().UTC().Format(time.RFC3339Nano)

	runNullCases(t, shelfNames(parent, true), []nullCase{
		{"Equals", `delete_time = timestamp("` + deleteTime + `")`, []string{"gone-a"}},
		{"NotEquals", `delete_time != timestamp("` + deleteTime + `")`, []string{"gone-b", "live"}},
		{"LessThan", `delete_time < timestamp("2100-01-01T00:00:00Z")`, []string{"gone-a", "gone-b"}},
		{"LessThanString", `delete_time < "2100-01-01T00:00:00Z"`, []string{"gone-a", "gone-b"}},
		{"GreaterThan", `delete_time > timestamp("2000-01-01T00:00:00Z")`, []string{"gone-a", "gone-b"}},
		{"Presence", `delete_time:*`, []string{"gone-a", "gone-b"}},
		{"NotPresence", `NOT delete_time:*`, []string{"live"}},
		{"NotEqualsNegated", `NOT delete_time = timestamp("` + deleteTime + `")`, []string{"gone-b", "live"}},
		{"NotLessThan", `NOT delete_time < timestamp("2100-01-01T00:00:00Z")`, []string{"live"}},
	})
}

func TestFilterNull_NullableDuration(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createNullShelf(t, parent, "unset", nil)
	createNullShelf(t, parent, "1h", func(s *librarypb.Shelf) { s.Duration = durationpb.New(time.Hour) })
	createNullShelf(t, parent, "2h", func(s *librarypb.Shelf) { s.Duration = durationpb.New(2 * time.Hour) })

	runNullCases(t, shelfNames(parent, false), []nullCase{
		{"NotEquals", `duration != duration("1h")`, []string{"2h", "unset"}},
		{"EqualsZero", `duration = duration("0s")`, nil},
		{"LessThan", `duration < duration("1h")`, nil},
		{"LessThanOrEqual", `duration <= duration("1h")`, []string{"1h"}},
		{"Presence", `duration:*`, []string{"1h", "2h"}},
		{"NotPresence", `NOT duration:*`, []string{"unset"}},
		{"NotEqualsNegated", `NOT duration = duration("1h")`, []string{"2h", "unset"}},
	})
}

func TestFilterNull_Traversal_JSONB(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	// unset: every leaf and nested message missing; extra message absent.
	createNullShelf(t, parent, "unset", nil)
	// empty: nested messages present but empty.
	createNullShelf(t, parent, "empty", func(s *librarypb.Shelf) {
		s.Metadata.Location = &librarypb.ShelfLocation{}
		s.Extra = &librarypb.ShelfExtra{}
	})
	createNullShelf(t, parent, "set", func(s *librarypb.Shelf) {
		s.Metadata = &librarypb.ShelfMetadata{
			Capacity: 5,
			Dummy:    "x",
			Open:     true,
			Theme:    librarypb.ShelfGenre_SHELF_GENRE_FICTION,
			Location: &librarypb.ShelfLocation{Room: "x", Floor: 3},
		}
		s.Extra = &librarypb.ShelfExtra{Note: "x", Rank: 5}
	})

	runNullCases(t, shelfNames(parent, false), []nullCase{
		// Leaves under an always-present message: missing key is zero.
		{"IntEqualsZero", `metadata.capacity = 0`, []string{"empty", "unset"}},
		{"IntNotEquals", `metadata.capacity != 5`, []string{"empty", "unset"}},
		{"IntLessThan", `metadata.capacity < 5`, []string{"empty", "unset"}},
		{"IntPresence", `metadata.capacity:*`, []string{"set"}},
		{"StringNotEquals", `metadata.dummy != "x"`, []string{"empty", "unset"}},
		{"StringEqualsEmpty", `metadata.dummy = ""`, []string{"empty", "unset"}},
		{"StringPresence", `metadata.dummy:*`, []string{"set"}},
		{"BoolEqualsFalse", `metadata.open = false`, []string{"empty", "unset"}},
		{"BoolNotEqualsTrue", `metadata.open != true`, []string{"empty", "unset"}},
		{"BoolPresence", `metadata.open:*`, []string{"set"}},
		{"EnumEqualsUnspecified", `metadata.theme = SHELF_GENRE_UNSPECIFIED`, []string{"empty", "unset"}},
		{"EnumNotEquals", `metadata.theme != SHELF_GENRE_FICTION`, []string{"empty", "unset"}},
		{"EnumPresence", `metadata.theme:*`, []string{"set"}},
		// Leaves under a nested message: skipped when the message is unset.
		{"NestedStringNotEquals", `metadata.location.room != "x"`, []string{"empty"}},
		{"NestedStringEqualsEmpty", `metadata.location.room = ""`, []string{"empty"}},
		{"NestedIntLessThan", `metadata.location.floor < 3`, []string{"empty"}},
		{"NestedStringPresence", `metadata.location.room:*`, []string{"set"}},
		{"NestedMessagePresence", `metadata.location:*`, []string{"empty", "set"}},
		// Leaves under a nullable top-level message: same skip.
		{"ExtraStringNotEquals", `extra.note != "x"`, []string{"empty"}},
		{"ExtraStringEqualsEmpty", `extra.note = ""`, []string{"empty"}},
		{"ExtraIntEqualsZero", `extra.rank = 0`, []string{"empty"}},
		{"ExtraIntLessThan", `extra.rank < 5`, []string{"empty"}},
		{"ExtraStringPresence", `extra.note:*`, []string{"set"}},
		{"ExtraMessagePresence", `extra:*`, []string{"empty", "set"}},
		// NOT is "not true": it does admit the skipped entry.
		{"ExtraNotEquals", `NOT extra.note = "x"`, []string{"empty", "unset"}},
	})
}

func TestFilterNull_Map(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createNullShelf(t, parent, "nolabels", nil)
	createNullShelf(t, parent, "other", func(s *librarypb.Shelf) { s.Labels = map[string]string{"other": "z"} })
	createNullShelf(t, parent, "a", func(s *librarypb.Shelf) { s.Labels = map[string]string{"k": "a"} })
	createNullShelf(t, parent, "b", func(s *librarypb.Shelf) { s.Labels = map[string]string{"k": "b"} })

	runNullCases(t, shelfNames(parent, false), []nullCase{
		{"KeyEquals", `labels.k = "a"`, []string{"a"}},
		{"KeyNotEquals", `labels.k != "a"`, []string{"b", "nolabels", "other"}},
		{"KeyEqualsEmpty", `labels.k = ""`, []string{"nolabels", "other"}},
		{"KeyPresence", `labels.k:*`, []string{"a", "b"}},
		{"NotKeyPresence", `NOT labels.k:*`, []string{"nolabels", "other"}},
		{"MapPresence", `labels:*`, []string{"a", "b", "other"}},
		{"NotMapPresence", `NOT labels:*`, []string{"nolabels"}},
	})
}

func TestFilterNull_Joins(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	author := createTestAuthor(t, parent, "Joins Author")
	createNullShelf(t, parent, "nobooks", nil)
	shelf := createNullShelf(t, parent, "withbook", nil)
	book := createNullBook(t, shelf.Name, author.Name, "Joined Book", nil)
	setShelfBestBook(t, shelf.Name, book.Name)

	t.Run("Shelf", func(t *testing.T) {
		t.Parallel()
		runNullCases(t, shelfNames(parent, false), []nullCase{
			{"QueryJoinStringNotEquals", `latest_book_title != "x"`, []string{"nobooks", "withbook"}},
			{"QueryJoinStringEqualsEmpty", `latest_book_title = ""`, []string{"nobooks"}},
			{"QueryJoinStringPresence", `latest_book_title:*`, []string{"withbook"}},
			{"QueryJoinReferencePresence", `latest_book:*`, []string{"withbook"}},
			{"ReferenceJoinIntLessThan", `best_book_page_count < 10`, []string{"nobooks"}},
			{"ReferenceJoinIntEqualsZero", `best_book_page_count = 0`, []string{"nobooks"}},
			{"ReferenceJoinIntGreaterThanZero", `best_book_page_count > 0`, []string{"withbook"}},
			{"ReferenceJoinIntPresence", `best_book_page_count:*`, []string{"withbook"}},
		})
	})

	// A book always joins its shelf; the joined external_id is what can be NULL.
	t.Run("Book", func(t *testing.T) {
		t.Parallel()
		runNullCases(t, bookTitles(shelf.Name), []nullCase{
			{"ParentJoinStringNotEquals", `shelf_external_id != "x"`, []string{"Joined Book"}},
			{"ParentJoinStringEqualsEmpty", `shelf_external_id = ""`, []string{"Joined Book"}},
			{"ParentJoinStringPresence", `shelf_external_id:*`, nil},
			{"ParentJoinEnumNotEquals", `shelf_genre != SHELF_GENRE_HISTORY`, []string{"Joined Book"}},
		})
	})
}

func TestFilterNull_NotComposition(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createNullShelf(t, parent, "r1", nil)
	createNullShelf(t, parent, "r2", func(s *librarypb.Shelf) { s.SecondaryGenre = librarypb.ShelfGenre_SHELF_GENRE_FICTION })
	createNullShelf(t, parent, "r3", func(s *librarypb.Shelf) { s.ShelfNumber = 5 })
	createNullShelf(t, parent, "r4", func(s *librarypb.Shelf) {
		s.SecondaryGenre = librarypb.ShelfGenre_SHELF_GENRE_FICTION
		s.ShelfNumber = 5
	})
	createNullShelf(t, parent, "r5", func(s *librarypb.Shelf) {
		s.SecondaryGenre = librarypb.ShelfGenre_SHELF_GENRE_HISTORY
		s.ShelfNumber = 10
	})
	list := shelfNames(parent, false)
	all := []string{"r1", "r2", "r3", "r4", "r5"}

	runNullCases(t, list, []nullCase{
		{"NotAnd", `NOT (secondary_genre = SHELF_GENRE_FICTION AND shelf_number = 5)`, []string{"r1", "r2", "r3", "r5"}},
		{"NotOr", `NOT (secondary_genre = SHELF_GENRE_FICTION OR shelf_number = 5)`, []string{"r1", "r5"}},
		{"NotNot", `NOT (NOT secondary_genre = SHELF_GENRE_FICTION)`, []string{"r2", "r4"}},
		{"Minus", `-secondary_genre = SHELF_GENRE_FICTION`, []string{"r1", "r3", "r5"}},
		{"NotAndPresence", `NOT (secondary_genre:* AND shelf_number:*)`, []string{"r1", "r2", "r3"}},
	})

	for _, c := range []struct{ name, filter, negated string }{
		{"AndComplement", `secondary_genre = SHELF_GENRE_FICTION AND shelf_number = 5`, `NOT (secondary_genre = SHELF_GENRE_FICTION AND shelf_number = 5)`},
		{"OrComplement", `secondary_genre = SHELF_GENRE_FICTION OR shelf_number = 5`, `NOT (secondary_genre = SHELF_GENRE_FICTION OR shelf_number = 5)`},
		{"NotNotComplement", `NOT secondary_genre = SHELF_GENRE_FICTION`, `NOT (NOT secondary_genre = SHELF_GENRE_FICTION)`},
		{"MinusComplement", `secondary_genre = SHELF_GENRE_FICTION`, `-secondary_genre = SHELF_GENRE_FICTION`},
		{"NotEqualsComplement", `shelf_number != 5`, `NOT shelf_number != 5`},
		{"GreaterThanComplement", `shelf_number > 5`, `NOT shelf_number > 5`},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			requireComplement(t, list, all, c.filter, c.negated)
		})
	}
}

func TestFilterNull_ColumnVsColumn(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createNullShelf(t, parent, "live", nil)
	gone := createNullShelf(t, parent, "gone", nil)
	_, err := libraryServiceClient.DeleteShelf(ctx, &libraryservicepb.DeleteShelfRequest{Name: gone.Name})
	require.NoError(t, err)

	// Plain SQL: a NULL delete_time is a non-match, never a zero.
	runNullCases(t, shelfNames(parent, true), []nullCase{
		{"GreaterThan", `delete_time > create_time`, []string{"gone"}},
		{"LessThan", `delete_time < create_time`, nil},
		{"NotGreaterThan", `NOT delete_time > create_time`, []string{"live"}},
		{"GreaterThanOrPresence", `delete_time > create_time OR NOT delete_time:*`, []string{"gone", "live"}},
	})
}

func TestFilterNull_Search(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	author := createTestAuthor(t, parent, "Search Author")
	shelf := createNullShelf(t, parent, "shelf", nil)
	createNullBook(t, shelf.Name, author.Name, "Nullsearch Plain", nil)
	for _, c := range []struct {
		title string
		label string
		color librarypb.BookmarkColor
	}{
		{"Nullsearch Red", "a", librarypb.BookmarkColor_BOOKMARK_COLOR_RED},
		{"Nullsearch Blue", "b", librarypb.BookmarkColor_BOOKMARK_COLOR_BLUE},
	} {
		book := createNullBook(t, shelf.Name, author.Name, c.title, func(b *librarypb.Book) { b.Labels = map[string]string{"k": c.label} })
		_, err := bookmarkServiceClient.CreateBookmark(ctx, &libraryservicepb.CreateBookmarkRequest{
			Parent:   book.Name,
			Bookmark: &librarypb.Bookmark{PageNumber: 1, DisplayName: c.title, Color: c.color},
		})
		require.NoError(t, err)
	}

	runNullCases(t, searchedBookTitles(shelf.Name, "nullsearch"), []nullCase{
		{"JoinedEnumNotEquals", `latest_bookmark_color != BOOKMARK_COLOR_RED`, []string{"Nullsearch Blue", "Nullsearch Plain"}},
		{"JoinedEnumEqualsUnspecified", `latest_bookmark_color = BOOKMARK_COLOR_UNSPECIFIED`, []string{"Nullsearch Plain"}},
		{"JoinedEnumPresence", `latest_bookmark_color:*`, []string{"Nullsearch Blue", "Nullsearch Red"}},
		{"NotJoinedEnumEquals", `NOT latest_bookmark_color = BOOKMARK_COLOR_RED`, []string{"Nullsearch Blue", "Nullsearch Plain"}},
		{"MapKeyNotEquals", `labels.k != "a"`, []string{"Nullsearch Blue", "Nullsearch Plain"}},
		{"NotMapKeyPresence", `NOT labels.k:*`, []string{"Nullsearch Plain"}},
	})
}

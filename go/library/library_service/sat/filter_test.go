package sat

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	grpcrequire "github.com/malonaz/core/go/grpc/require"

	libraryservicepb "github.com/malonaz/core/genproto/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/library/v1"
)

func createFilterAuthor(t *testing.T, parent string, opts func(*librarypb.Author)) *librarypb.Author {
	t.Helper()
	author := &librarypb.Author{
		DisplayName:    "Default Filter Author",
		EmailAddress:   "default-filter@example.com",
		EmailAddresses: []string{"default-filter@example.com"},
		Metadata:       &librarypb.AuthorMetadata{},
	}
	opts(author)
	createAuthorRequest := &libraryservicepb.CreateAuthorRequest{
		Parent: parent,
		Author: author,
	}
	created, err := libraryServiceClient.CreateAuthor(ctx, createAuthorRequest)
	require.NoError(t, err)
	return created
}

func listAuthors(t *testing.T, parent, filter string) []*librarypb.Author {
	t.Helper()
	listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
		Parent: parent,
		Filter: filter,
	}
	listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
	require.NoError(t, err)
	return listAuthorsResponse.Authors
}

func TestFilter_Equality(t *testing.T) {
	parent := getOrganizationParent()
	a1 := createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Alice Equality"
		a.EmailAddress = "alice-eq@example.com"
		a.Biography = "bio-alice"
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Bob Equality"
		a.EmailAddress = "bob-eq@example.com"
		a.Biography = "bio-bob"
	})

	t.Run("ExactMatch", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Alice Equality"`)
		require.Len(t, results, 1)
		require.Equal(t, a1.Name, results[0].Name)
	})

	t.Run("NoMatch", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Nonexistent Person"`)
		require.Empty(t, results)
	})

	t.Run("NotEqual", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name != "Alice Equality"`)
		require.Len(t, results, 1)
		require.Equal(t, "Bob Equality", results[0].DisplayName)
	})

	t.Run("NotEqualReturnsAllWhenNoMatch", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name != "Nonexistent"`)
		require.Len(t, results, 2)
	})

	t.Run("EmptyStringMatch", func(t *testing.T) {
		results := listAuthors(t, parent, `biography = "bio-alice"`)
		require.Len(t, results, 1)
		require.Equal(t, a1.Name, results[0].Name)
	})
}

func TestFilter_EmptyFilter(t *testing.T) {
	parent := getOrganizationParent()
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Empty Filter A"
		a.EmailAddress = "empty-a@example.com"
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Empty Filter B"
		a.EmailAddress = "empty-b@example.com"
	})

	results := listAuthors(t, parent, "")
	require.Len(t, results, 2)
}

func TestFilter_LogicalOperators(t *testing.T) {
	parent := getOrganizationParent()
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Logic Alpha"
		a.EmailAddress = "logic-alpha@example.com"
		a.Biography = "logic-bio-shared"
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Logic Beta"
		a.EmailAddress = "logic-beta@example.com"
		a.Biography = "logic-bio-shared"
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Logic Gamma"
		a.EmailAddress = "logic-gamma@example.com"
		a.Biography = "logic-bio-different"
	})

	t.Run("AND_BothMatch", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Logic Alpha" AND biography = "logic-bio-shared"`)
		require.Len(t, results, 1)
		require.Equal(t, "Logic Alpha", results[0].DisplayName)
	})

	t.Run("AND_OneFails", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Logic Alpha" AND biography = "logic-bio-different"`)
		require.Empty(t, results)
	})

	t.Run("OR_EitherMatches", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Logic Alpha" OR display_name = "Logic Beta"`)
		require.Len(t, results, 2)
	})

	t.Run("OR_OnlyOneMatches", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Logic Alpha" OR display_name = "Nonexistent"`)
		require.Len(t, results, 1)
	})

	t.Run("OR_NeitherMatches", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Nope1" OR display_name = "Nope2"`)
		require.Empty(t, results)
	})

	t.Run("NOT", func(t *testing.T) {
		results := listAuthors(t, parent, `NOT display_name = "Logic Gamma"`)
		require.Len(t, results, 2)
		for _, r := range results {
			require.NotEqual(t, "Logic Gamma", r.DisplayName)
		}
	})

	t.Run("MinusOperator", func(t *testing.T) {
		results := listAuthors(t, parent, `-display_name = "Logic Gamma"`)
		require.Len(t, results, 2)
	})

	t.Run("TripleOR", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Logic Alpha" OR display_name = "Logic Beta" OR display_name = "Logic Gamma"`)
		require.Len(t, results, 3)
	})

	t.Run("TripleAND_SharedBio", func(t *testing.T) {
		results := listAuthors(t, parent, `biography = "logic-bio-shared" AND display_name != "Logic Alpha" AND display_name != "Logic Gamma"`)
		require.Len(t, results, 1)
		require.Equal(t, "Logic Beta", results[0].DisplayName)
	})
}

func TestFilter_Parentheses(t *testing.T) {
	parent := getOrganizationParent()
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Paren A"
		a.EmailAddress = "paren-a@example.com"
		a.Biography = "paren-bio-x"
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Paren B"
		a.EmailAddress = "paren-b@example.com"
		a.Biography = "paren-bio-x"
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Paren C"
		a.EmailAddress = "paren-c@example.com"
		a.Biography = "paren-bio-y"
	})

	t.Run("GroupedOR_WithAND", func(t *testing.T) {
		results := listAuthors(t, parent, `(display_name = "Paren A" OR display_name = "Paren C") AND biography = "paren-bio-x"`)
		require.Len(t, results, 1)
		require.Equal(t, "Paren A", results[0].DisplayName)
	})

	t.Run("GroupedOR_WithAND_BothMatch", func(t *testing.T) {
		results := listAuthors(t, parent, `(display_name = "Paren A" OR display_name = "Paren B") AND biography = "paren-bio-x"`)
		require.Len(t, results, 2)
	})

	t.Run("NOT_GroupedOR", func(t *testing.T) {
		results := listAuthors(t, parent, `NOT (display_name = "Paren A" OR display_name = "Paren B")`)
		require.Len(t, results, 1)
		require.Equal(t, "Paren C", results[0].DisplayName)
	})

	t.Run("DoubleGroupedOR_WithAND", func(t *testing.T) {
		results := listAuthors(t, parent, `(display_name = "Paren A" OR display_name = "Paren B") AND (biography = "paren-bio-x" OR biography = "paren-bio-y")`)
		require.Len(t, results, 2)
	})
}

func TestFilter_WildcardStrings(t *testing.T) {
	parent := getOrganizationParent()
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Wildcard Hemingway Jones"
		a.EmailAddress = "wildcard-hj@example.com"
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Wildcard Smith Jones"
		a.EmailAddress = "wildcard-sj@example.com"
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Wildcard Hemingway Brown"
		a.EmailAddress = "wildcard-hb@example.com"
	})

	t.Run("TrailingWildcard", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Wildcard Hemingway*"`)
		require.Len(t, results, 2)
		for _, r := range results {
			require.True(t, strings.HasPrefix(r.DisplayName, "Wildcard Hemingway"))
		}
	})

	t.Run("LeadingWildcard", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "*Jones"`)
		require.Len(t, results, 2)
		for _, r := range results {
			require.True(t, strings.HasSuffix(r.DisplayName, "Jones"))
		}
	})

	t.Run("BothWildcards", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "*Hemingway*"`)
		require.Len(t, results, 2)
		for _, r := range results {
			require.True(t, strings.Contains(r.DisplayName, "Hemingway"))
		}
	})

	t.Run("MiddleWildcard_TreatedAsLiteral", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Wild*Jones"`)
		require.Empty(t, results)
	})

	t.Run("WildcardNoMatch", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Nonexistent*"`)
		require.Empty(t, results)
	})

	t.Run("WildcardCombinedWithAND", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Wildcard*" AND display_name = "*Jones"`)
		require.Len(t, results, 2)
	})
}

func TestFilter_NestedMessage(t *testing.T) {
	parent := getOrganizationParent()
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Nested US Author"
		a.EmailAddress = "nested-us@example.com"
		a.Metadata = &librarypb.AuthorMetadata{Country: "US-nested-test"}
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Nested UK Author"
		a.EmailAddress = "nested-uk@example.com"
		a.Metadata = &librarypb.AuthorMetadata{Country: "UK-nested-test"}
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Nested Empty Author"
		a.EmailAddress = "nested-empty@example.com"
		a.Metadata = &librarypb.AuthorMetadata{}
	})

	t.Run("ExactMatch", func(t *testing.T) {
		results := listAuthors(t, parent, `metadata.country = "US-nested-test"`)
		require.Len(t, results, 1)
		require.Equal(t, "US-nested-test", results[0].Metadata.Country)
	})

	t.Run("WildcardMatch", func(t *testing.T) {
		results := listAuthors(t, parent, `metadata.country = "*nested-test"`)
		require.Len(t, results, 2)
	})

	t.Run("PresenceCheck", func(t *testing.T) {
		results := listAuthors(t, parent, `metadata.country:*`)
		require.Len(t, results, 2)
		for _, r := range results {
			require.NotEmpty(t, r.Metadata.Country)
		}
	})

	t.Run("CombinedWithTopLevel", func(t *testing.T) {
		results := listAuthors(t, parent, `metadata.country = "US-nested-test" AND display_name = "Nested US Author"`)
		require.Len(t, results, 1)
	})
}

func TestFilter_RepeatedFields(t *testing.T) {
	parent := getOrganizationParent()
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Repeated A"
		a.EmailAddress = "rep-a@example.com"
		a.EmailAddresses = []string{"alice-rep@one.com", "alice-rep@two.com"}
		a.PhoneNumbers = []string{"+33142685300"}
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Repeated B"
		a.EmailAddress = "rep-b@example.com"
		a.EmailAddresses = []string{"bob-rep@one.com"}
	})

	t.Run("HasExactValue", func(t *testing.T) {
		results := listAuthors(t, parent, `email_addresses:"alice-rep@two.com"`)
		require.Len(t, results, 1)
		require.Equal(t, "Repeated A", results[0].DisplayName)
	})

	t.Run("HasWildcardValue", func(t *testing.T) {
		results := listAuthors(t, parent, `email_addresses:"*@one.com"`)
		require.Len(t, results, 2)
	})

	t.Run("HasPresence_NonEmpty", func(t *testing.T) {
		results := listAuthors(t, parent, `email_addresses:*`)
		require.Len(t, results, 2)
	})

	t.Run("HasPresence_PhoneNumbers", func(t *testing.T) {
		results := listAuthors(t, parent, `phone_numbers:*`)
		require.Len(t, results, 1)
		require.Equal(t, "Repeated A", results[0].DisplayName)
	})

	t.Run("NoMatch", func(t *testing.T) {
		results := listAuthors(t, parent, `email_addresses:"nonexistent@nowhere.com"`)
		require.Empty(t, results)
	})

	t.Run("NOT_Has", func(t *testing.T) {
		results := listAuthors(t, parent, `NOT email_addresses:"alice-rep@two.com"`)
		require.Len(t, results, 1)
		require.Equal(t, "Repeated B", results[0].DisplayName)
	})
}

func TestFilter_NestedRepeatedFields(t *testing.T) {
	parent := getOrganizationParent()
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "NestedRep A"
		a.EmailAddress = "nestedrep-a@example.com"
		a.Metadata = &librarypb.AuthorMetadata{
			EmailAddresses: []string{"meta-a@one.com", "meta-a@two.com"},
			PhoneNumbers:   []string{"+33142685300"},
		}
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "NestedRep B"
		a.EmailAddress = "nestedrep-b@example.com"
		a.Metadata = &librarypb.AuthorMetadata{
			EmailAddresses: []string{"meta-b@one.com"},
		}
	})

	t.Run("ExactValueInNestedRepeated", func(t *testing.T) {
		results := listAuthors(t, parent, `metadata.email_addresses:"meta-a@two.com"`)
		require.Len(t, results, 1)
		require.Equal(t, "NestedRep A", results[0].DisplayName)
	})

	t.Run("WildcardInNestedRepeated", func(t *testing.T) {
		results := listAuthors(t, parent, `metadata.email_addresses:"*@one.com"`)
		require.Len(t, results, 2)
	})

	t.Run("PresenceOfNestedRepeated", func(t *testing.T) {
		results := listAuthors(t, parent, `metadata.phone_numbers:*`)
		require.Len(t, results, 1)
		require.Equal(t, "NestedRep A", results[0].DisplayName)
	})
}

func TestFilter_MapLabels(t *testing.T) {
	parent := getOrganizationParent()
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Label A"
		a.EmailAddress = "label-a@example.com"
		a.Labels = map[string]string{"env": "prod-filter-test", "tier": "gold"}
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Label B"
		a.EmailAddress = "label-b@example.com"
		a.Labels = map[string]string{"env": "staging-filter-test"}
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Label None"
		a.EmailAddress = "label-none@example.com"
	})

	t.Run("HasKey", func(t *testing.T) {
		results := listAuthors(t, parent, `labels:"tier"`)
		require.Len(t, results, 1)
		require.Equal(t, "Label A", results[0].DisplayName)
	})

	t.Run("HasKey_Common", func(t *testing.T) {
		results := listAuthors(t, parent, `labels:"env"`)
		require.Len(t, results, 2)
	})

	t.Run("KeyValueExact", func(t *testing.T) {
		results := listAuthors(t, parent, `labels.env = "prod-filter-test"`)
		require.Len(t, results, 1)
		require.Equal(t, "Label A", results[0].DisplayName)
	})

	t.Run("KeyValueNotEqual", func(t *testing.T) {
		results := listAuthors(t, parent, `labels.env != "prod-filter-test"`)
		for _, r := range results {
			require.NotEqual(t, "prod-filter-test", r.Labels["env"])
		}
	})

	t.Run("KeyValueWildcard", func(t *testing.T) {
		results := listAuthors(t, parent, `labels.env = "*filter-test"`)
		require.Len(t, results, 2)
	})

	t.Run("HasAnyLabels", func(t *testing.T) {
		results := listAuthors(t, parent, `labels:*`)
		require.Len(t, results, 2)
		for _, r := range results {
			require.NotEmpty(t, r.Labels)
		}
	})

	t.Run("NOT_HasAnyLabels", func(t *testing.T) {
		results := listAuthors(t, parent, `NOT labels:*`)
		require.Len(t, results, 1)
		require.Equal(t, "Label None", results[0].DisplayName)
	})

	t.Run("HasKeyAND_KeyValue", func(t *testing.T) {
		results := listAuthors(t, parent, `labels:"tier" AND labels.env = "prod-filter-test"`)
		require.Len(t, results, 1)
	})

	t.Run("NOT_HasKey", func(t *testing.T) {
		results := listAuthors(t, parent, `NOT labels:"tier"`)
		require.Len(t, results, 2)
	})

	t.Run("HasOperatorWithValue", func(t *testing.T) {
		results := listAuthors(t, parent, `labels.env:"prod-filter-test"`)
		require.Len(t, results, 1)
	})
}

func TestFilter_PresenceChecks(t *testing.T) {
	parent := getOrganizationParent()
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Presence Full"
		a.EmailAddress = "presence-full@example.com"
		a.PhoneNumber = "+14155550001"
		a.Biography = "has-bio"
		a.Metadata = &librarypb.AuthorMetadata{Country: "US"}
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Presence Minimal"
		a.EmailAddress = "presence-min@example.com"
		a.Metadata = &librarypb.AuthorMetadata{}
	})

	t.Run("StringPresent", func(t *testing.T) {
		results := listAuthors(t, parent, `phone_number:*`)
		require.Len(t, results, 1)
		require.Equal(t, "Presence Full", results[0].DisplayName)
	})

	t.Run("StringAbsent", func(t *testing.T) {
		results := listAuthors(t, parent, `NOT phone_number:*`)
		require.Len(t, results, 1)
		require.Equal(t, "Presence Minimal", results[0].DisplayName)
	})

	t.Run("BiographyPresent", func(t *testing.T) {
		results := listAuthors(t, parent, `biography:*`)
		require.Len(t, results, 1)
	})

	t.Run("MetadataPresent", func(t *testing.T) {
		results := listAuthors(t, parent, `metadata:*`)
		require.Len(t, results, 2)
	})

	t.Run("TimestampPresent_CreateTime", func(t *testing.T) {
		results := listAuthors(t, parent, `create_time:*`)
		require.Len(t, results, 2)
	})

	t.Run("TimestampAbsent_DeleteTime", func(t *testing.T) {
		results := listAuthors(t, parent, `NOT delete_time:*`)
		require.Len(t, results, 2)
	})

	t.Run("CombinedPresenceAndEquality", func(t *testing.T) {
		results := listAuthors(t, parent, `phone_number:* AND display_name = "Presence Full"`)
		require.Len(t, results, 1)
	})
}

func TestFilter_Timestamps(t *testing.T) {
	parent := getOrganizationParent()
	a1 := createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Timestamp First"
		a.EmailAddress = "ts-first@example.com"
	})
	a2 := createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Timestamp Second"
		a.EmailAddress = "ts-second@example.com"
	})
	beforeCreate := a1.GetCreateTime().AsTime().Add(-time.Microsecond)
	midpoint := a1.GetCreateTime().AsTime().Add(time.Microsecond)
	afterCreate := a2.GetCreateTime().AsTime().Add(time.Microsecond)

	t.Run("GreaterThan", func(t *testing.T) {
		filter := fmt.Sprintf(`create_time > %q`, midpoint.Format(time.RFC3339Nano))
		results := listAuthors(t, parent, filter)
		require.Len(t, results, 1)
		require.Equal(t, "Timestamp Second", results[0].DisplayName)
	})

	t.Run("LessThan", func(t *testing.T) {
		filter := fmt.Sprintf(`create_time < %q`, midpoint.Format(time.RFC3339Nano))
		results := listAuthors(t, parent, filter)
		require.Len(t, results, 1)
		require.Equal(t, "Timestamp First", results[0].DisplayName)
	})

	t.Run("Range", func(t *testing.T) {
		filter := fmt.Sprintf(
			`create_time >= %q AND create_time <= %q`,
			beforeCreate.Format(time.RFC3339Nano),
			afterCreate.Format(time.RFC3339Nano),
		)
		results := listAuthors(t, parent, filter)
		require.Len(t, results, 2)
	})

	t.Run("NoMatch_FutureBound", func(t *testing.T) {
		future := time.Now().UTC().Add(24 * time.Hour)
		filter := fmt.Sprintf(`create_time > %q`, future.Format(time.RFC3339Nano))
		results := listAuthors(t, parent, filter)
		require.Empty(t, results)
	})
}

func TestFilter_EnumFields(t *testing.T) {
	parent := getOrganizationParent()
	createTestShelf(t, parent, "Enum Fiction Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	createTestShelf(t, parent, "Enum History Shelf", librarypb.ShelfGenre_SHELF_GENRE_HISTORY)
	createTestShelf(t, parent, "Enum SciFi Shelf", librarypb.ShelfGenre_SHELF_GENRE_SCIENCE_FICTION)

	listShelves := func(filter string) []*librarypb.Shelf {
		t.Helper()
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: parent,
			Filter: filter,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		return listShelvesResponse.Shelves
	}

	t.Run("ExactMatch", func(t *testing.T) {
		results := listShelves(`genre = SHELF_GENRE_FICTION`)
		require.Len(t, results, 1)
		require.Equal(t, librarypb.ShelfGenre_SHELF_GENRE_FICTION, results[0].Genre)
	})

	t.Run("NotEqual", func(t *testing.T) {
		results := listShelves(`genre != SHELF_GENRE_FICTION`)
		require.Len(t, results, 2)
		for _, s := range results {
			require.NotEqual(t, librarypb.ShelfGenre_SHELF_GENRE_FICTION, s.Genre)
		}
	})

	t.Run("MultipleWithOR", func(t *testing.T) {
		results := listShelves(`genre = SHELF_GENRE_FICTION OR genre = SHELF_GENRE_HISTORY`)
		require.Len(t, results, 2)
	})

	t.Run("EnumPresence", func(t *testing.T) {
		results := listShelves(`genre:*`)
		require.Len(t, results, 3)
	})

	t.Run("CombinedWithString", func(t *testing.T) {
		results := listShelves(`genre = SHELF_GENRE_FICTION AND display_name = "Enum Fiction Shelf"`)
		require.Len(t, results, 1)
	})
}

func TestFilter_IntegerComparisons(t *testing.T) {
	parent := getOrganizationParent()
	author := createTestAuthor(t, parent, "Int Cmp Author")
	shelf := createTestShelf(t, parent, "Int Cmp Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)

	createTestBookWithYear(t, shelf.Name, author.Name, "Book 1980", 1980)
	createTestBookWithYear(t, shelf.Name, author.Name, "Book 2000", 2000)
	createTestBookWithYear(t, shelf.Name, author.Name, "Book 2020", 2020)

	listBooks := func(filter string) []*librarypb.Book {
		t.Helper()
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: filter,
		}
		listBooksResponse, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		require.NoError(t, err)
		return listBooksResponse.Books
	}

	t.Run("Equal", func(t *testing.T) {
		results := listBooks(`publication_year = 2000`)
		require.Len(t, results, 1)
		require.Equal(t, int32(2000), results[0].PublicationYear)
	})

	t.Run("NotEqual", func(t *testing.T) {
		results := listBooks(`publication_year != 2000`)
		require.Len(t, results, 2)
	})

	t.Run("GreaterThan", func(t *testing.T) {
		results := listBooks(`publication_year > 2000`)
		require.Len(t, results, 1)
		require.Equal(t, int32(2020), results[0].PublicationYear)
	})

	t.Run("GreaterThanOrEqual", func(t *testing.T) {
		results := listBooks(`publication_year >= 2000`)
		require.Len(t, results, 2)
	})

	t.Run("LessThan", func(t *testing.T) {
		results := listBooks(`publication_year < 2000`)
		require.Len(t, results, 1)
		require.Equal(t, int32(1980), results[0].PublicationYear)
	})

	t.Run("LessThanOrEqual", func(t *testing.T) {
		results := listBooks(`publication_year <= 2000`)
		require.Len(t, results, 2)
	})

	t.Run("Range", func(t *testing.T) {
		results := listBooks(`publication_year >= 1990 AND publication_year <= 2010`)
		require.Len(t, results, 1)
		require.Equal(t, int32(2000), results[0].PublicationYear)
	})

	t.Run("Presence", func(t *testing.T) {
		results := listBooks(`publication_year:*`)
		require.Len(t, results, 3)
	})
}

func TestFilter_NestedIntegerField(t *testing.T) {
	parent := getOrganizationParent()
	createShelfWithCapacity := func(name string, capacity int32) {
		t.Helper()
		createShelfRequest := &libraryservicepb.CreateShelfRequest{
			Parent: parent,
			Shelf: &librarypb.Shelf{
				DisplayName: name,
				Genre:       librarypb.ShelfGenre_SHELF_GENRE_FICTION,
				Metadata:    &librarypb.ShelfMetadata{Capacity: capacity},
			},
		}
		_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
		require.NoError(t, err)
	}

	createShelfWithCapacity("Small Shelf", 50)
	createShelfWithCapacity("Medium Shelf", 200)
	createShelfWithCapacity("Large Shelf", 500)

	listShelves := func(filter string) []*librarypb.Shelf {
		t.Helper()
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: parent,
			Filter: filter,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		return listShelvesResponse.Shelves
	}

	t.Run("ExactMatch", func(t *testing.T) {
		results := listShelves(`metadata.capacity = 200`)
		require.Len(t, results, 1)
		require.Equal(t, "Medium Shelf", results[0].DisplayName)
	})

	t.Run("GreaterThan", func(t *testing.T) {
		results := listShelves(`metadata.capacity > 100`)
		require.Len(t, results, 2)
	})

	t.Run("LessThanOrEqual", func(t *testing.T) {
		results := listShelves(`metadata.capacity <= 200`)
		require.Len(t, results, 2)
	})

	t.Run("CombinedWithEnum", func(t *testing.T) {
		results := listShelves(`genre = SHELF_GENRE_FICTION AND metadata.capacity >= 500`)
		require.Len(t, results, 1)
		require.Equal(t, "Large Shelf", results[0].DisplayName)
	})
}

func TestFilter_ComplexCombined(t *testing.T) {
	parent := getOrganizationParent()
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Complex Alpha"
		a.EmailAddress = "complex-alpha@example.com"
		a.PhoneNumber = "+14155550001"
		a.EmailAddresses = []string{"complex-alpha@example.com"}
		a.Labels = map[string]string{"env": "prod", "team": "backend"}
		a.Metadata = &librarypb.AuthorMetadata{Country: "US"}
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Complex Beta"
		a.EmailAddress = "complex-beta@example.com"
		a.EmailAddresses = []string{"complex-beta@example.com"}
		a.Labels = map[string]string{"env": "staging"}
		a.Metadata = &librarypb.AuthorMetadata{Country: "UK"}
	})
	createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Complex Gamma"
		a.EmailAddress = "complex-gamma@example.com"
		a.EmailAddresses = []string{"complex-gamma@example.com"}
		a.Metadata = &librarypb.AuthorMetadata{Country: "US"}
	})

	t.Run("PresenceAND_Equality_AND_NestedField", func(t *testing.T) {
		results := listAuthors(t, parent, `phone_number:* AND labels.env = "prod" AND metadata.country = "US"`)
		require.Len(t, results, 1)
		require.Equal(t, "Complex Alpha", results[0].DisplayName)
	})

	t.Run("OR_WithNested_AND_Labels", func(t *testing.T) {
		results := listAuthors(t, parent, `(metadata.country = "US" OR metadata.country = "UK") AND labels:*`)
		require.Len(t, results, 2)
	})

	t.Run("NOT_WithNestedAND", func(t *testing.T) {
		results := listAuthors(t, parent, `metadata.country = "US" AND NOT labels:*`)
		require.Len(t, results, 1)
		require.Equal(t, "Complex Gamma", results[0].DisplayName)
	})

	t.Run("WildcardString_AND_Presence_AND_Label", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "Complex*" AND phone_number:* AND labels:"team"`)
		require.Len(t, results, 1)
		require.Equal(t, "Complex Alpha", results[0].DisplayName)
	})

	t.Run("RepeatedField_AND_Nested_AND_NOT", func(t *testing.T) {
		results := listAuthors(t, parent, `email_addresses:"*@example.com" AND metadata.country = "US" AND NOT phone_number:*`)
		require.Len(t, results, 1)
		require.Equal(t, "Complex Gamma", results[0].DisplayName)
	})
}

func TestFilter_SoftDeletedVisibility(t *testing.T) {
	parent := getOrganizationParent()
	author := createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "SoftDel Visible"
		a.EmailAddress = "softdel-vis@example.com"
	})
	toDelete := createFilterAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "SoftDel Hidden"
		a.EmailAddress = "softdel-hid@example.com"
	})
	deleteAuthorRequest := &libraryservicepb.DeleteAuthorRequest{Name: toDelete.Name}
	_, err := libraryServiceClient.DeleteAuthor(ctx, deleteAuthorRequest)
	require.NoError(t, err)

	t.Run("DefaultHidesDeleted", func(t *testing.T) {
		results := listAuthors(t, parent, `display_name = "SoftDel*"`)
		require.Len(t, results, 1)
		require.Equal(t, author.Name, results[0].Name)
	})

	t.Run("ShowDeletedReveals", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:      parent,
			Filter:      `display_name = "SoftDel*"`,
			ShowDeleted: true,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 2)
	})

	t.Run("FilterDeleteTime_ShowDeleted", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent:      parent,
			Filter:      `delete_time:*`,
			ShowDeleted: true,
		}
		listAuthorsResponse, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		require.NoError(t, err)
		require.Len(t, listAuthorsResponse.Authors, 1)
		require.Equal(t, toDelete.Name, listAuthorsResponse.Authors[0].Name)
	})
}

func TestFilter_DisallowedPaths(t *testing.T) {
	parent := getOrganizationParent()
	author := createTestAuthor(t, parent, "Disallowed Path Author")
	shelf := createTestShelf(t, parent, "Disallowed Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	createTestBook(t, shelf.Name, author.Name, "Disallowed Book")

	t.Run("Book_PageCountNotAllowed", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `page_count > 100`,
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Book_EtagNotAllowed", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `etag = "something"`,
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Book_NameNotAllowed", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `name = "something"`,
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("Book_CreateTimeNotAllowed", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `create_time > "2024-01-01T00:00:00Z"`,
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestFilter_InvalidSyntax(t *testing.T) {
	parent := getOrganizationParent()
	createTestAuthor(t, parent, "Syntax Error Author")

	tests := []struct {
		name   string
		filter string
	}{
		{"MissingValue", `display_name =`},
		{"UnbalancedParenOpen", `(display_name = "test"`},
		{"UnbalancedParenClose", `display_name = "test")`},
		{"EmptyParentheses", `()`},
		{"DoubleOperator", `display_name = = "test"`},
		{"TrailingAND", `display_name = "test" AND`},
		{"LeadingOR", `OR display_name = "test"`},
		{"UndefinedField", `undefined_field = "value"`},
		{"InvalidTimestamp", `create_time > "not-a-timestamp"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
				Parent: parent,
				Filter: tc.filter,
			}
			_, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
			grpcrequire.Error(t, codes.InvalidArgument, err)
		})
	}
}

func TestFilter_TypeMismatch(t *testing.T) {
	parent := getOrganizationParent()
	author := createTestAuthor(t, parent, "Type Mismatch Author")
	shelf := createTestShelf(t, parent, "Type Mismatch Shelf", librarypb.ShelfGenre_SHELF_GENRE_FICTION)
	createTestBook(t, shelf.Name, author.Name, "Type Mismatch Book")

	t.Run("StringFieldWithInteger", func(t *testing.T) {
		listAuthorsRequest := &libraryservicepb.ListAuthorsRequest{
			Parent: parent,
			Filter: `display_name = 123`,
		}
		_, err := libraryServiceClient.ListAuthors(ctx, listAuthorsRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("IntegerFieldWithString", func(t *testing.T) {
		listBooksRequest := &libraryservicepb.ListBooksRequest{
			Parent: shelf.Name,
			Filter: `publication_year = "not_a_number"`,
		}
		_, err := libraryServiceClient.ListBooks(ctx, listBooksRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("EnumWithString", func(t *testing.T) {
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: parent,
			Filter: `genre = "FICTION"`,
		}
		_, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("EnumWithInteger", func(t *testing.T) {
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: parent,
			Filter: `genre = 1`,
		}
		_, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})

	t.Run("InvalidEnumValue", func(t *testing.T) {
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: parent,
			Filter: `genre = INVALID_GENRE_VALUE`,
		}
		_, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		grpcrequire.Error(t, codes.InvalidArgument, err)
	})
}

func TestFilter_ColumnNameReplacement(t *testing.T) {
	parent := getOrganizationParent()
	createShelfRequest := &libraryservicepb.CreateShelfRequest{
		Parent: parent,
		Shelf: &librarypb.Shelf{
			DisplayName:     "ColName Shelf",
			Genre:           librarypb.ShelfGenre_SHELF_GENRE_FICTION,
			ExternalId:      "ext-filter-123",
			CorrelationId_2: "corr-filter-456",
			Metadata:        &librarypb.ShelfMetadata{Capacity: 100},
		},
	}
	_, err := libraryServiceClient.CreateShelf(ctx, createShelfRequest)
	require.NoError(t, err)

	listShelves := func(filter string) []*librarypb.Shelf {
		t.Helper()
		listShelvesRequest := &libraryservicepb.ListShelvesRequest{
			Parent: parent,
			Filter: filter,
		}
		listShelvesResponse, err := libraryServiceClient.ListShelves(ctx, listShelvesRequest)
		require.NoError(t, err)
		return listShelvesResponse.Shelves
	}

	t.Run("ExternalId", func(t *testing.T) {
		results := listShelves(`external_id = "ext-filter-123"`)
		require.Len(t, results, 1)
		require.Equal(t, "ext-filter-123", results[0].ExternalId)
	})

	t.Run("CorrelationId2", func(t *testing.T) {
		results := listShelves(`correlation_id_2 = "corr-filter-456"`)
		require.Len(t, results, 1)
		require.Equal(t, "corr-filter-456", results[0].CorrelationId_2)
	})

	t.Run("ExternalId_Presence", func(t *testing.T) {
		results := listShelves(`external_id:*`)
		require.Len(t, results, 1)
	})

	t.Run("Combined", func(t *testing.T) {
		results := listShelves(`external_id = "ext-filter-123" AND correlation_id_2 = "corr-filter-456"`)
		require.Len(t, results, 1)
	})
}

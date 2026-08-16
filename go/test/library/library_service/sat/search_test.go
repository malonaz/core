package sat

import (
	"testing"

	grpcrequire "github.com/malonaz/core/go/grpc/require"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func createSearchAuthor(t *testing.T, parent string, opts func(*librarypb.Author)) *librarypb.Author {
	t.Helper()
	author := validAuthor()
	if opts != nil {
		opts(author)
	}
	created, err := libraryServiceClient.CreateAuthor(ctx, &libraryservicepb.CreateAuthorRequest{
		Parent: parent,
		Author: author,
	})
	require.NoError(t, err)
	return created
}

func searchAuthors(t *testing.T, request *libraryservicepb.SearchAuthorsRequest) []*librarypb.Author {
	t.Helper()
	response, err := libraryServiceClient.SearchAuthors(ctx, request)
	require.NoError(t, err)
	return response.Authors
}

func TestSearch_CaseInsensitive(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	author := createSearchAuthor(t, parent, func(a *librarypb.Author) { a.DisplayName = "John Steinbeck" })
	createSearchAuthor(t, parent, func(a *librarypb.Author) { a.DisplayName = "Aldous Huxley" })

	for _, query := range []string{"john", "JOHN", "John", "jOhN sTeInBeCk"} {
		authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: query})
		require.Len(t, authors, 1, "query %q", query)
		require.Equal(t, author.Name, authors[0].Name)
	}
}

func TestSearch_PrefixMatching(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	author := createSearchAuthor(t, parent, func(a *librarypb.Author) { a.DisplayName = "Fyodor Dostoevsky" })

	for _, query := range []string{"fyo", "dosto", "fyodor dosto"} {
		authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: query})
		require.Len(t, authors, 1, "query %q", query)
		require.Equal(t, author.Name, authors[0].Name)
	}

	// Non-prefix substrings do not match.
	authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "ostoevsky"})
	require.Empty(t, authors)
}

func TestSearch_EmailSplitting(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	author := createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Ursula Le Guin"
		a.EmailAddress = "ursula.leguin@earthsea.org"
		a.EmailAddresses = []string{"contact@hainish.net"}
	})
	createSearchAuthor(t, parent, nil)

	// Whole address, local part, domain labels — of both the scalar and repeated fields.
	for _, query := range []string{"ursula.leguin@earthsea.org", "leguin", "earthsea", "hainish", "contact"} {
		authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: query})
		require.Len(t, authors, 1, "query %q", query)
		require.Equal(t, author.Name, authors[0].Name, "query %q", query)
	}
}

func TestSearch_PhoneSplitting(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	author := createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Phone Person"
		a.PhoneNumber = "+14155557890"
		a.PhoneNumbers = []string{"+33610102031"}
	})
	createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Other Person"
		a.PhoneNumber = "+12247704567"
		a.PhoneNumbers = nil
	})

	// Digits-only prefixes and mid-number fragments match (suffix tokens),
	// with or without the raw formatting.
	for _, query := range []string{"1415555", "+14155557890", "336101", "415", "5557890", "102031", "890"} {
		authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: query})
		require.Len(t, authors, 1, "query %q", query)
		require.Equal(t, author.Name, authors[0].Name, "query %q", query)
	}
}

func TestSearch_RelevanceRanking(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	// "tolstoy" appears in the display name (weight A) of one author and only
	// in the biography (weight D) of the other: the name hit must rank first.
	nameHit := createSearchAuthor(t, parent, func(a *librarypb.Author) { a.DisplayName = "Leo Tolstoy" })
	bioHit := createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Some Scholar"
		a.Biography = "Wrote extensively about Tolstoy and Russian literature."
	})

	authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "tolstoy"})
	require.Len(t, authors, 2)
	require.Equal(t, nameHit.Name, authors[0].Name)
	require.Equal(t, bioHit.Name, authors[1].Name)
}

func TestSearch_MultiTokenAnd(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	author := createSearchAuthor(t, parent, func(a *librarypb.Author) { a.DisplayName = "Gabriel Garcia Marquez" })
	createSearchAuthor(t, parent, func(a *librarypb.Author) { a.DisplayName = "Gabriel Faure" })

	// All tokens must match.
	authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "gabriel marquez"})
	require.Len(t, authors, 1)
	require.Equal(t, author.Name, authors[0].Name)

	// A token matching nothing yields no results.
	authors = searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "gabriel zzznope"})
	require.Empty(t, authors)
}

func TestSearch_WithFilter(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	fiction := createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Isaac Asimov"
		a.Labels = map[string]string{"genre": "fiction"}
	})
	createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Isaac Newton"
		a.Labels = map[string]string{"genre": "science"}
	})

	authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{
		Parent: parent,
		Query:  "isaac",
		Filter: `labels.genre = "fiction"`,
	})
	require.Len(t, authors, 1)
	require.Equal(t, fiction.Name, authors[0].Name)
}

func TestSearch_EmptyQueryRejected(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createSearchAuthor(t, parent, func(a *librarypb.Author) { a.DisplayName = "Author One" })

	// Empty query.
	_, err := libraryServiceClient.SearchAuthors(ctx, &libraryservicepb.SearchAuthorsRequest{Parent: parent})
	grpcrequire.Error(t, codes.InvalidArgument, err)

	// Query with no indexable token.
	_, err = libraryServiceClient.SearchAuthors(ctx, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "&&& !!!"})
	grpcrequire.Error(t, codes.InvalidArgument, err)
}

func TestSearch_SoftDeleted(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	author := createSearchAuthor(t, parent, func(a *librarypb.Author) { a.DisplayName = "Deleted Author" })
	_, err := libraryServiceClient.DeleteAuthor(ctx, &libraryservicepb.DeleteAuthorRequest{Name: author.Name})
	require.NoError(t, err)

	authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "deleted"})
	require.Empty(t, authors)

	authors = searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "deleted", ShowDeleted: true})
	require.Len(t, authors, 1)
	require.Equal(t, author.Name, authors[0].Name)
}

func TestSearch_Pagination(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	for i := 0; i < 5; i++ {
		createSearchAuthor(t, parent, func(a *librarypb.Author) { a.DisplayName = "Paginated Author" })
	}

	seen := map[string]bool{}
	pageToken := ""
	pages := 0
	for {
		response, err := libraryServiceClient.SearchAuthors(ctx, &libraryservicepb.SearchAuthorsRequest{
			Parent:    parent,
			Query:     "paginated",
			PageSize:  2,
			PageToken: pageToken,
		})
		require.NoError(t, err)
		require.LessOrEqual(t, len(response.Authors), 2)
		for _, author := range response.Authors {
			require.False(t, seen[author.Name], "duplicate result across pages")
			seen[author.Name] = true
		}
		pages++
		if response.NextPageToken == "" {
			break
		}
		pageToken = response.NextPageToken
	}
	require.Len(t, seen, 5)
	require.Equal(t, 3, pages)
}

func TestSearch_ParentIsolation(t *testing.T) {
	t.Parallel()
	parent1 := getOrganizationParent()
	parent2 := getOrganizationParent()
	author := createSearchAuthor(t, parent1, func(a *librarypb.Author) { a.DisplayName = "Isolated Author" })
	createSearchAuthor(t, parent2, func(a *librarypb.Author) { a.DisplayName = "Isolated Author" })

	authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent1, Query: "isolated"})
	require.Len(t, authors, 1)
	require.Equal(t, author.Name, authors[0].Name)
}

func TestSearch_InvalidRequests(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()

	// Missing parent.
	_, err := libraryServiceClient.SearchAuthors(ctx, &libraryservicepb.SearchAuthorsRequest{Query: "x"})
	grpcrequire.Error(t, codes.InvalidArgument, err)

	// Invalid parent.
	_, err = libraryServiceClient.SearchAuthors(ctx, &libraryservicepb.SearchAuthorsRequest{Parent: "bogus/thing", Query: "x"})
	grpcrequire.Error(t, codes.InvalidArgument, err)

	// Invalid filter.
	_, err = libraryServiceClient.SearchAuthors(ctx, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "x", Filter: "not a filter ("})
	grpcrequire.Error(t, codes.InvalidArgument, err)
}

// TestSearch_Accuracy builds a realistic corpus and checks precision, recall
// and ranking across many query shapes.
func TestSearch_Accuracy(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()

	type seed struct {
		displayName string
		email       string
		phone       string
		biography   string
	}
	seeds := []seed{
		{"John Smith", "john.smith@acme.com", "+14155551234", "Plumbing contractor in the Bay Area."},
		{"John Doe", "jdoe@gmail.com", "+12247704567", "Electrician."},
		{"Johnny Cash", "cash@sunrecords.com", "+16155550100", "Musician turned author."},
		{"Jane Smith", "jane@acme.com", "+14155559876", "Roofing specialist."},
		{"Smith Rowe", "rowe@arsenal.co.uk", "+447911123456", "Footballer writing memoirs."},
		{"Maria Garcia", "maria.garcia@yahoo.com", "+34600111222", "Novelist."},
		{"Marie Curie", "marie@sorbonne.fr", "+33610102030", "Scientist and biographer."},
		{"Mario Rossi", "mario@rossi.it", "+393331234567", "Travel writer."},
		{"Chen Wei", "chen.wei@baidu.cn", "+8613800138000", "Historian."},
		{"Ana Souza", "ana@souza.br", "+5511998765432", "Poet."},
		{"Peter Parker", "peter@dailybugle.com", "+12125550001", "Photojournalist."},
		{"Bruce Wayne", "bruce@wayneenterprises.com", "+12125550002", "Philanthropist."},
		{"Clark Kent", "clark@dailyplanet.com", "+12125550003", "Reporter covering plumbing scandals."},
		{"Diana Prince", "diana@themyscira.org", "+12125550004", "Curator."},
		{"Tony Stark", "tony@starkindustries.com", "+12125550005", "Engineer who pays cash."},
	}
	for _, sd := range seeds {
		createSearchAuthor(t, parent, func(a *librarypb.Author) {
			a.DisplayName = sd.displayName
			a.EmailAddress = sd.email
			a.PhoneNumber = sd.phone
			a.Biography = sd.biography
			a.EmailAddresses = []string{sd.email}
			a.PhoneNumbers = nil
		})
	}

	cases := []struct {
		query string
		// want is the exact expected result set, by display name.
		want []string
		// wantFirst asserts the top-ranked result, when order matters.
		wantFirst string
	}{
		// Prefix recall: "john" matches John*, Johnny, and jdoe's local part "jdoe"? no — but john.smith's email too.
		{query: "john", want: []string{"John Smith", "John Doe", "Johnny Cash"}},
		// Multi-token precision: both tokens must hit the same author.
		{query: "john smith", want: []string{"John Smith"}},
		{query: "jane acme", want: []string{"Jane Smith"}},
		// Surname across authors.
		{query: "smith", want: []string{"John Smith", "Jane Smith", "Smith Rowe"}},
		// Email local part, domain, and full address.
		{query: "jdoe", want: []string{"John Doe"}},
		{query: "sunrecords", want: []string{"Johnny Cash"}},
		{query: "marie@sorbonne.fr", want: []string{"Marie Curie"}},
		// Domain shared by two authors.
		{query: "acme", want: []string{"John Smith", "Jane Smith"}},
		// Phone prefixes.
		{query: "1415555", want: []string{"John Smith", "Jane Smith"}},
		{query: "+8613800", want: []string{"Chen Wei"}},
		// Biography terms.
		{query: "plumbing", want: []string{"John Smith", "Clark Kent"}},
		{query: "roofing", want: []string{"Jane Smith"}},
		// Similar names must not cross-match.
		{query: "maria", want: []string{"Maria Garcia"}},
		{query: "marie", want: []string{"Marie Curie"}},
		{query: "mario", want: []string{"Mario Rossi"}},
		// Shared prefix across all three.
		{query: "mari", want: []string{"Maria Garcia", "Marie Curie", "Mario Rossi"}},
		// No match.
		{query: "zzznope", want: nil},
		// Ranking: name hit (weight A) outranks biography hit (weight D).
		{query: "cash", want: []string{"Johnny Cash", "Tony Stark"}, wantFirst: "Johnny Cash"},
	}

	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: tc.query})
			got := make([]string, 0, len(authors))
			for _, author := range authors {
				got = append(got, author.DisplayName)
			}
			require.ElementsMatch(t, tc.want, got, "query %q", tc.query)
			if tc.wantFirst != "" {
				require.Equal(t, tc.wantFirst, got[0], "query %q top result", tc.query)
			}
		})
	}
}

func TestSearch_Snippets(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	bioHit := createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Herman Melville"
		a.Biography = "Wrote extensively about whaling voyages across the Pacific and the obsessions of sea captains."
	})
	nameHit := createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Whaling Expert"
		a.Biography = "Nothing relevant here."
	})

	t.Run("HighlightedFragment", func(t *testing.T) {
		response, err := libraryServiceClient.SearchAuthors(ctx, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "whaling"})
		require.NoError(t, err)
		require.Len(t, response.Authors, 2)
		// Snippets are index-aligned with authors.
		require.Len(t, response.Snippets, 2)
		for i, author := range response.Authors {
			snippet := response.Snippets[i]
			switch author.Name {
			case bioHit.Name:
				// Biography matched: fragment present, match highlighted.
				require.Contains(t, snippet.Fields["biography"], "**whaling**")
			case nameHit.Name:
				// Only the display name matched: no biography fragment.
				require.NotContains(t, snippet.Fields, "biography")
			default:
				t.Fatalf("unexpected author %s", author.Name)
			}
		}
	})

	t.Run("PrefixQueryHighlights", func(t *testing.T) {
		response, err := libraryServiceClient.SearchAuthors(ctx, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "obsess"})
		require.NoError(t, err)
		require.Len(t, response.Authors, 1)
		require.Len(t, response.Snippets, 1)
		require.Contains(t, response.Snippets[0].Fields["biography"], "**obsessions**")
	})

	t.Run("AlignedUnderPagination", func(t *testing.T) {
		pageToken := ""
		for {
			response, err := libraryServiceClient.SearchAuthors(ctx, &libraryservicepb.SearchAuthorsRequest{
				Parent: parent, Query: "whaling", PageSize: 1, PageToken: pageToken,
			})
			require.NoError(t, err)
			require.Equal(t, len(response.Authors), len(response.Snippets))
			if response.NextPageToken == "" {
				break
			}
			pageToken = response.NextPageToken
		}
	})
}

func TestSearch_MetadataJSONPaths(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	author := createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Gabriel Garcia Marquez"
		a.Metadata = &librarypb.AuthorMetadata{
			Country:        "Colombia",
			EmailAddresses: []string{"gabo@macondo.co"},
		}
	})
	createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Someone Else"
		a.Metadata = &librarypb.AuthorMetadata{Country: "France"}
	})

	// Matches inside the JSONB metadata column: the scalar string path and the
	// repeated string path (with email splitting).
	for _, query := range []string{"colombia", "Colombia", "colom", "gabo@macondo.co", "gabo", "macondo"} {
		authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: query})
		require.Len(t, authors, 1, "query %q", query)
		require.Equal(t, author.Name, authors[0].Name, "query %q", query)
	}
}

func TestSearch_MetadataNoMatch(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Metadata NoMatch"
		a.Metadata = &librarypb.AuthorMetadata{Country: "Japan"}
	})

	authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "wakanda"})
	require.Empty(t, authors)
}

func TestSearch_MetadataJSONKeyCasing(t *testing.T) {
	t.Parallel()
	parent := getOrganizationParent()
	// The search expression extracts metadata #>> '{email_addresses}': a match
	// on a nested repeated value proves JSONB keys are proto field names
	// (pbutil.JSONMarshal uses UseProtoNames), not protojson camelCase — a
	// camelCase document would store "emailAddresses" and never match.
	author := createSearchAuthor(t, parent, func(a *librarypb.Author) {
		a.DisplayName = "Casing Probe"
		a.Metadata = &librarypb.AuthorMetadata{EmailAddresses: []string{"snakecase@proto.names"}}
	})

	authors := searchAuthors(t, &libraryservicepb.SearchAuthorsRequest{Parent: parent, Query: "snakecase@proto.names"})
	require.Len(t, authors, 1)
	require.Equal(t, author.Name, authors[0].Name)
}

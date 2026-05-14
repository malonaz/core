package aip

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func escapeDollar(s string) string {
	return strings.ReplaceAll(s, "$", "@")
}

func TestFilteringRequestParser_NewParser(t *testing.T) {
	tests := []struct {
		name         string
		createParser func() error
		wantErr      bool
	}{
		{
			name: "valid parser creation - Author",
			createParser: func() error {
				_, err := NewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
				return err
			},
		},
		{
			name: "valid parser creation - Shelf",
			createParser: func() error {
				_, err := NewFilteringRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()
				return err
			},
		},
		{
			name: "valid parser creation - Book",
			createParser: func() error {
				_, err := NewFilteringRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.createParser()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFilteringRequestParser_BasicFieldFilters(t *testing.T) {
	parser := MustNewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "string field equality",
			filter:         `display_name = "John Doe"`,
			expectedClause: "WHERE (display_name = $1)",
			expectedParams: []any{"John Doe"},
		},
		{
			name:           "string field with special characters",
			filter:         `email_address = "user@example.com"`,
			expectedClause: "WHERE (email_address = $1)",
			expectedParams: []any{"user@example.com"},
		},
		{
			name:           "empty filter",
			filter:         "",
			expectedClause: "",
			expectedParams: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListAuthorsRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

func TestFilteringRequestParser_ComparisonOperators(t *testing.T) {
	parser := MustNewFilteringRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "string equality",
			filter:         `title = "The Great Gatsby"`,
			expectedClause: "WHERE (title = $1)",
			expectedParams: []any{"The Great Gatsby"},
		},
		{
			name:           "string not equal",
			filter:         `title != "Excluded"`,
			expectedClause: "WHERE (title != $1)",
			expectedParams: []any{"Excluded"},
		},
		{
			name:           "integer greater than",
			filter:         `publication_year > 2000`,
			expectedClause: "WHERE (publication_year > $1)",
			expectedParams: []any{int64(2000)},
		},
		{
			name:           "integer less than",
			filter:         `publication_year < 2020`,
			expectedClause: "WHERE (publication_year < $1)",
			expectedParams: []any{int64(2020)},
		},
		{
			name:           "integer greater than or equal",
			filter:         `publication_year >= 1990`,
			expectedClause: "WHERE (publication_year >= $1)",
			expectedParams: []any{int64(1990)},
		},
		{
			name:           "integer less than or equal",
			filter:         `publication_year <= 2010`,
			expectedClause: "WHERE (publication_year <= $1)",
			expectedParams: []any{int64(2010)},
		},
		{
			name:           "integer equality",
			filter:         `publication_year = 1984`,
			expectedClause: "WHERE (publication_year = $1)",
			expectedParams: []any{int64(1984)},
		},
		{
			name:           "integer not equal",
			filter:         `publication_year != 1999`,
			expectedClause: "WHERE (publication_year != $1)",
			expectedParams: []any{int64(1999)},
		},
		{
			name:           "string greater than (lexical)",
			filter:         `title > "A"`,
			expectedClause: "WHERE (title > $1)",
			expectedParams: []any{"A"},
		},
		{
			name:           "string less than (lexical)",
			filter:         `title < "Z"`,
			expectedClause: "WHERE (title < $1)",
			expectedParams: []any{"Z"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListBooksRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

func TestFilteringRequestParser_LogicalOperators(t *testing.T) {
	parser := MustNewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "AND with two conditions",
			filter:         `display_name = "John" AND biography = "Author bio"`,
			expectedClause: "WHERE ((display_name = $1) AND (biography = $2))",
			expectedParams: []any{"John", "Author bio"},
		},
		{
			name:           "AND with three conditions",
			filter:         `display_name = "John" AND biography = "Bio" AND email_address = "john@test.com"`,
			expectedClause: "WHERE (((display_name = $1) AND (biography = $2)) AND (email_address = $3))",
			expectedParams: []any{"John", "Bio", "john@test.com"},
		},
		{
			name:           "OR with two conditions",
			filter:         `display_name = "John" OR display_name = "Jane"`,
			expectedClause: "WHERE ((display_name = $1) OR (display_name = $2))",
			expectedParams: []any{"John", "Jane"},
		},
		{
			name:           "OR with three conditions",
			filter:         `display_name = "A" OR display_name = "B" OR display_name = "C"`,
			expectedClause: "WHERE (((display_name = $1) OR (display_name = $2)) OR (display_name = $3))",
			expectedParams: []any{"A", "B", "C"},
		},
		{
			name:           "NOT with comparison",
			filter:         `NOT display_name = "Excluded"`,
			expectedClause: "WHERE (NOT (display_name = $1))",
			expectedParams: []any{"Excluded"},
		},
		{
			name:           "NOT with parentheses",
			filter:         `NOT (display_name = "A" OR display_name = "B")`,
			expectedClause: "WHERE (NOT ((display_name = $1) OR (display_name = $2)))",
			expectedParams: []any{"A", "B"},
		},
		{
			name:           "minus operator with comparison",
			filter:         `-display_name = "Excluded"`,
			expectedClause: "WHERE (NOT (display_name = $1))",
			expectedParams: []any{"Excluded"},
		},
		{
			name:           "AND and OR combined with parentheses",
			filter:         `(display_name = "John" OR display_name = "Jane") AND NOT email_address = "spam@test.com"`,
			expectedClause: "WHERE (((display_name = $1) OR (display_name = $2)) AND (NOT (email_address = $3)))",
			expectedParams: []any{"John", "Jane", "spam@test.com"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListAuthorsRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

func TestFilteringRequestParser_OperatorPrecedence(t *testing.T) {
	parser := MustNewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
	}{
		{
			name:           "OR has higher precedence than AND",
			filter:         `display_name = "A" AND display_name = "B" OR display_name = "C"`,
			expectedClause: "WHERE ((display_name = $1) AND ((display_name = $2) OR (display_name = $3)))",
			expectedParams: []any{"A", "B", "C"},
		},
		{
			name:           "explicit parentheses override precedence",
			filter:         `(display_name = "A" AND display_name = "B") OR display_name = "C"`,
			expectedClause: "WHERE (((display_name = $1) AND (display_name = $2)) OR (display_name = $3))",
			expectedParams: []any{"A", "B", "C"},
		},
		{
			name:           "multiple OR groups with AND",
			filter:         `display_name = "A" AND display_name = "B" OR display_name = "C" AND display_name = "D" OR display_name = "E"`,
			expectedClause: "WHERE (((display_name = $1) AND ((display_name = $2) OR (display_name = $3))) AND ((display_name = $4) OR (display_name = $5)))",
			expectedParams: []any{"A", "B", "C", "D", "E"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListAuthorsRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

func TestFilteringRequestParser_TraversalOperator(t *testing.T) {
	t.Run("Author metadata", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
			wantErr        bool
		}{
			{
				name:           "nested string field",
				filter:         `metadata.country = "USA"`,
				expectedClause: "WHERE (metadata->>'country' = $1)",
				expectedParams: []any{"USA"},
			},
			{
				name:    "undefined nested field",
				filter:  `metadata.undefined_field = "test"`,
				wantErr: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListAuthorsRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})

	t.Run("Shelf metadata with integer", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
		}{
			{
				name:           "nested integer field equality",
				filter:         `metadata.capacity = 100`,
				expectedClause: "WHERE ((legacy_meta->>'capacity')::bigint = $1)",
				expectedParams: []any{int64(100)},
			},
			{
				name:           "nested integer field greater than",
				filter:         `metadata.capacity > 50`,
				expectedClause: "WHERE ((legacy_meta->>'capacity')::bigint > $1)",
				expectedParams: []any{int64(50)},
			},
			{
				name:           "nested integer field less than or equal",
				filter:         `metadata.capacity <= 200`,
				expectedClause: "WHERE ((legacy_meta->>'capacity')::bigint <= $1)",
				expectedParams: []any{int64(200)},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListShelvesRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})

	t.Run("Book metadata strings", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
		}{
			{
				name:           "nested summary field",
				filter:         `metadata.summary = "A great book"`,
				expectedClause: "WHERE (metadata->>'summary' = $1)",
				expectedParams: []any{"A great book"},
			},
			{
				name:           "nested language field",
				filter:         `metadata.language = "en"`,
				expectedClause: "WHERE (metadata->>'language' = $1)",
				expectedParams: []any{"en"},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListBooksRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})
}

func TestFilteringRequestParser_HasOperator(t *testing.T) {
	t.Run("Author fields", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
			wantErr        bool
		}{
			{
				name:           "string field is present",
				filter:         `display_name:*`,
				expectedClause: "WHERE (display_name IS NOT NULL AND display_name != '')",
				expectedParams: []any{},
			},
			{
				name:           "string field is not present",
				filter:         `NOT display_name:*`,
				expectedClause: "WHERE (NOT (display_name IS NOT NULL AND display_name != ''))",
				expectedParams: []any{},
			},
			{
				name:           "nested message field is present",
				filter:         `metadata:*`,
				expectedClause: "WHERE (metadata IS NOT NULL)",
				expectedParams: []any{},
			},
			{
				name:           "nested message field is not present",
				filter:         `-metadata:*`,
				expectedClause: "WHERE (NOT (metadata IS NOT NULL))",
				expectedParams: []any{},
			},
			{
				name:           "repeated string contains value",
				filter:         `email_addresses:"john@example.com"`,
				expectedClause: "WHERE ($1 = ANY(email_addresses))",
				expectedParams: []any{"john@example.com"},
			},
			{
				name:           "repeated string contains value with NOT",
				filter:         `NOT email_addresses:"spam@example.com"`,
				expectedClause: "WHERE (NOT ($1 = ANY(email_addresses)))",
				expectedParams: []any{"spam@example.com"},
			},
			{
				name:           "repeated field is present",
				filter:         `email_addresses:*`,
				expectedClause: "WHERE (email_addresses IS NOT NULL AND COALESCE(array_length(email_addresses, 1), 0) > 0)",
				expectedParams: []any{},
			},
			{
				name:           "map contains key",
				filter:         `labels:"environment"`,
				expectedClause: "WHERE (COALESCE(labels, '{}') ? $1)",
				expectedParams: []any{"environment"},
			},
			{
				name:           "map does not contain key",
				filter:         `NOT labels:"deprecated"`,
				expectedClause: "WHERE (NOT (COALESCE(labels, '{}') ? $1))",
				expectedParams: []any{"deprecated"},
			},
			{
				name:           "map field is present",
				filter:         `labels:*`,
				expectedClause: "WHERE (labels IS NOT NULL AND labels != '{}'::jsonb)",
				expectedParams: []any{},
			},
			{
				name:           "map key has specific value",
				filter:         `labels.environment:"production"`,
				expectedClause: "WHERE (labels->>'environment' = $1)",
				expectedParams: []any{"production"},
			},
			{
				name:           "map key equals specific value",
				filter:         `labels.environment = "staging"`,
				expectedClause: "WHERE (labels->>'environment' = $1)",
				expectedParams: []any{"staging"},
			},
			{
				name:           "has combined with AND",
				filter:         `email_addresses:"john@example.com" AND display_name = "John"`,
				expectedClause: "WHERE (($1 = ANY(email_addresses)) AND (display_name = $2))",
				expectedParams: []any{"john@example.com", "John"},
			},
			{
				name:           "presence check combined with equality",
				filter:         `display_name:* AND display_name = "John"`,
				expectedClause: "WHERE ((display_name IS NOT NULL AND display_name != '') AND (display_name = $1))",
				expectedParams: []any{"John"},
			},
			{
				name:           "timestamp field is present",
				filter:         `create_time:*`,
				expectedClause: "WHERE (create_time IS NOT NULL)",
				expectedParams: []any{},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListAuthorsRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})

	t.Run("Shelf enum field", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
		}{
			{
				name:           "enum field is present",
				filter:         `genre:*`,
				expectedClause: "WHERE (genre IS NOT NULL)",
				expectedParams: []any{},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListShelvesRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})

	t.Run("Book integer field", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
		}{
			{
				name:           "integer field is present",
				filter:         `publication_year:*`,
				expectedClause: "WHERE (publication_year IS NOT NULL)",
				expectedParams: []any{},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListBooksRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})
}

func TestFilteringRequestParser_WildcardStringMatching(t *testing.T) {
	parser := MustNewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "string starts with",
			filter:         `display_name = "John*"`,
			expectedClause: "WHERE (display_name LIKE $1)",
			expectedParams: []any{"John%"},
		},
		{
			name:           "string ends with",
			filter:         `display_name = "*Smith"`,
			expectedClause: "WHERE (display_name LIKE $1)",
			expectedParams: []any{"%Smith"},
		},
		{
			name:           "string contains",
			filter:         `display_name = "*middle*"`,
			expectedClause: "WHERE (display_name LIKE $1)",
			expectedParams: []any{"%middle%"},
		},
		{
			name:           "nested string starts with",
			filter:         `metadata.country = "United*"`,
			expectedClause: "WHERE (metadata->>'country' LIKE $1)",
			expectedParams: []any{"United%"},
		},
		{
			name:           "nested string ends with",
			filter:         `metadata.country = "*Kingdom"`,
			expectedClause: "WHERE (metadata->>'country' LIKE $1)",
			expectedParams: []any{"%Kingdom"},
		},
		{
			name:           "wildcard in middle only is literal",
			filter:         `display_name = "pre*fix"`,
			expectedClause: "WHERE (display_name = $1)",
			expectedParams: []any{"pre*fix"},
		},
		{
			name:           "wildcard with AND",
			filter:         `display_name = "John*" AND email_address = "john@example.com"`,
			expectedClause: "WHERE ((display_name LIKE $1) AND (email_address = $2))",
			expectedParams: []any{"John%", "john@example.com"},
		},
		{
			name:           "wildcard with OR",
			filter:         `display_name = "John*" OR display_name = "Jane*"`,
			expectedClause: "WHERE ((display_name LIKE $1) OR (display_name LIKE $2))",
			expectedParams: []any{"John%", "Jane%"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListAuthorsRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

func TestFilteringRequestParser_Timestamps(t *testing.T) {
	parser := MustNewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "timestamp equals",
			filter:         `create_time = "2024-01-15T10:30:00Z"`,
			expectedClause: "WHERE (create_time = $1)",
			expectedParams: []any{"2024-01-15T10:30:00Z"},
		},
		{
			name:           "timestamp greater than",
			filter:         `create_time > "2024-01-01T00:00:00Z"`,
			expectedClause: "WHERE (create_time > $1)",
			expectedParams: []any{"2024-01-01T00:00:00Z"},
		},
		{
			name:           "timestamp less than",
			filter:         `create_time < "2024-12-31T23:59:59Z"`,
			expectedClause: "WHERE (create_time < $1)",
			expectedParams: []any{"2024-12-31T23:59:59Z"},
		},
		{
			name:           "timestamp greater than or equal",
			filter:         `create_time >= "2024-06-01T00:00:00Z"`,
			expectedClause: "WHERE (create_time >= $1)",
			expectedParams: []any{"2024-06-01T00:00:00Z"},
		},
		{
			name:           "timestamp less than or equal",
			filter:         `update_time <= "2024-06-30T23:59:59Z"`,
			expectedClause: "WHERE (update_time <= $1)",
			expectedParams: []any{"2024-06-30T23:59:59Z"},
		},
		{
			name:           "timestamp with positive UTC offset",
			filter:         `create_time > "2024-01-15T10:30:00+05:30"`,
			expectedClause: "WHERE (create_time > $1)",
			expectedParams: []any{"2024-01-15T10:30:00+05:30"},
		},
		{
			name:           "timestamp with negative UTC offset",
			filter:         `create_time < "2024-01-15T10:30:00-08:00"`,
			expectedClause: "WHERE (create_time < $1)",
			expectedParams: []any{"2024-01-15T10:30:00-08:00"},
		},
		{
			name:           "timestamp range with AND",
			filter:         `create_time >= "2024-01-01T00:00:00Z" AND create_time < "2024-02-01T00:00:00Z"`,
			expectedClause: "WHERE ((create_time >= $1) AND (create_time < $2))",
			expectedParams: []any{"2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z"},
		},
		{
			name:           "timestamp field is present",
			filter:         `create_time:*`,
			expectedClause: "WHERE (create_time IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:           "nullable timestamp delete_time",
			filter:         `delete_time:*`,
			expectedClause: "WHERE (delete_time IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:    "invalid timestamp format",
			filter:  `create_time > "not-a-timestamp")`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListAuthorsRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

func TestFilteringRequestParser_Enums(t *testing.T) {
	parser := MustNewFilteringRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "enum equality - FICTION",
			filter:         `genre = SHELF_GENRE_FICTION`,
			expectedClause: "WHERE (genre = $1)",
			expectedParams: []any{int64(1)},
		},
		{
			name:           "enum equality - NON_FICTION",
			filter:         `genre = SHELF_GENRE_NON_FICTION`,
			expectedClause: "WHERE (genre = $1)",
			expectedParams: []any{int64(2)},
		},
		{
			name:           "enum equality - SCIENCE_FICTION",
			filter:         `genre = SHELF_GENRE_SCIENCE_FICTION`,
			expectedClause: "WHERE (genre = $1)",
			expectedParams: []any{int64(3)},
		},
		{
			name:           "enum equality - HISTORY",
			filter:         `genre = SHELF_GENRE_HISTORY`,
			expectedClause: "WHERE (genre = $1)",
			expectedParams: []any{int64(4)},
		},
		{
			name:           "enum equality - BIOGRAPHY",
			filter:         `genre = SHELF_GENRE_BIOGRAPHY`,
			expectedClause: "WHERE (genre = $1)",
			expectedParams: []any{int64(5)},
		},
		{
			name:           "enum unspecified",
			filter:         `genre = SHELF_GENRE_UNSPECIFIED`,
			expectedClause: "WHERE (genre = $1)",
			expectedParams: []any{int64(0)},
		},
		{
			name:           "enum not equal",
			filter:         `genre != SHELF_GENRE_FICTION`,
			expectedClause: "WHERE (genre != $1)",
			expectedParams: []any{int64(1)},
		},
		{
			name:           "enum presence check",
			filter:         `genre:*`,
			expectedClause: "WHERE (genre IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:    "invalid enum value",
			filter:  `genre = INVALID_GENRE`,
			wantErr: true,
		},
		{
			name:           "enum with AND",
			filter:         `genre = SHELF_GENRE_FICTION AND display_name = "Fiction Shelf"`,
			expectedClause: "WHERE ((genre = $1) AND (display_name = $2))",
			expectedParams: []any{int64(1), "Fiction Shelf"},
		},
		{
			name:           "multiple enum conditions with OR",
			filter:         `genre = SHELF_GENRE_FICTION OR genre = SHELF_GENRE_SCIENCE_FICTION`,
			expectedClause: "WHERE ((genre = $1) OR (genre = $2))",
			expectedParams: []any{int64(1), int64(3)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListShelvesRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

func TestFilteringRequestParser_TypeValidation(t *testing.T) {
	t.Run("Author type mismatches", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

		tests := []struct {
			name   string
			filter string
		}{
			{
				name:   "string field with integer value",
				filter: `display_name = 123`,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListAuthorsRequest{Filter: tc.filter}
				_, err := parser.Parse(request)
				require.Error(t, err, "expected type mismatch error for: %s", tc.filter)
			})
		}
	})

	t.Run("Book type mismatches", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()

		tests := []struct {
			name   string
			filter string
		}{
			{
				name:   "integer field with string value",
				filter: `publication_year = "not_a_number"`,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListBooksRequest{Filter: tc.filter}
				_, err := parser.Parse(request)
				require.Error(t, err, "expected type mismatch error for: %s", tc.filter)
			})
		}
	})

	t.Run("Shelf type mismatches", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()

		tests := []struct {
			name   string
			filter string
		}{
			{
				name:   "enum field with string value",
				filter: `genre = "FICTION"`,
			},
			{
				name:   "enum field with integer value",
				filter: `genre = 1`,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListShelvesRequest{Filter: tc.filter}
				_, err := parser.Parse(request)
				require.Error(t, err, "expected type mismatch error for: %s", tc.filter)
			})
		}
	})
}

func TestFilteringRequestParser_ErrorCases(t *testing.T) {
	parser := MustNewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

	tests := []struct {
		name   string
		filter string
	}{
		{
			name:   "undefined field",
			filter: `undefined_field = "value"`,
		},
		{
			name:   "undefined nested field",
			filter: `metadata.undefined = "value"`,
		},
		{
			name:   "missing value after operator",
			filter: `display_name =`,
		},
		{
			name:   "unbalanced parentheses - missing close",
			filter: `(display_name = "John"`,
		},
		{
			name:   "unbalanced parentheses - missing open",
			filter: `display_name = "John")`,
		},
		{
			name:   "invalid operator",
			filter: `display_name === "test"`,
		},
		{
			name:   "empty parentheses",
			filter: `()`,
		},
		{
			name:   "double operator",
			filter: `display_name = = "test"`,
		},
		{
			name:   "missing operand for AND",
			filter: `display_name = "test" AND`,
		},
		{
			name:   "missing operand for OR",
			filter: `OR display_name = "test"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListAuthorsRequest{Filter: tc.filter}
			_, err := parser.Parse(request)
			require.Error(t, err, "expected error for invalid filter: %s", tc.filter)
		})
	}
}

func TestFilteringRequestParser_ComplexFilters(t *testing.T) {
	t.Run("Author complex filters", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
		}{
			{
				name:           "multi-condition query",
				filter:         `display_name = "John" AND email_address = "john@test.com" AND metadata.country = "USA"`,
				expectedClause: "WHERE (((display_name = $1) AND (email_address = $2)) AND (metadata->>'country' = $3))",
				expectedParams: []any{"John", "john@test.com", "USA"},
			},
			{
				name:           "presence checks combined",
				filter:         `display_name:* AND metadata:* AND email_addresses:*`,
				expectedClause: "WHERE (((display_name IS NOT NULL AND display_name != '') AND (metadata IS NOT NULL)) AND (email_addresses IS NOT NULL AND COALESCE(array_length(email_addresses, 1), 0) > 0))",
				expectedParams: []any{},
			},
			{
				name:           "wildcard string with map check",
				filter:         `display_name = "John*" AND labels:"env"`,
				expectedClause: "WHERE ((display_name LIKE $1) AND (COALESCE(labels, '{}') ? $2))",
				expectedParams: []any{"John%", "env"},
			},
			{
				name:           "repeated field with other conditions",
				filter:         `email_addresses:"john@test.com" AND display_name = "John"`,
				expectedClause: "WHERE (($1 = ANY(email_addresses)) AND (display_name = $2))",
				expectedParams: []any{"john@test.com", "John"},
			},
			{
				name:           "map key value and nested combined",
				filter:         `labels.env = "prod" AND metadata.country = "USA"`,
				expectedClause: "WHERE ((labels->>'env' = $1) AND (metadata->>'country' = $2))",
				expectedParams: []any{"prod", "USA"},
			},
			{
				name:           "multiple OR groups",
				filter:         `(display_name = "John" OR display_name = "Jane") AND (email_address = "a@test.com" OR email_address = "b@test.com")`,
				expectedClause: "WHERE (((display_name = $1) OR (display_name = $2)) AND ((email_address = $3) OR (email_address = $4)))",
				expectedParams: []any{"John", "Jane", "a@test.com", "b@test.com"},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListAuthorsRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})

	t.Run("Shelf complex filters with enum", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
		}{
			{
				name:           "enum with string and nested int",
				filter:         `genre = SHELF_GENRE_FICTION AND display_name = "My Shelf" AND metadata.capacity > 50`,
				expectedClause: "WHERE (((genre = $1) AND (display_name = $2)) AND ((legacy_meta->>'capacity')::bigint > $3))",
				expectedParams: []any{int64(1), "My Shelf", int64(50)},
			},
			{
				name:           "multiple enums with OR",
				filter:         `(genre = SHELF_GENRE_FICTION OR genre = SHELF_GENRE_NON_FICTION) AND metadata.capacity >= 100`,
				expectedClause: "WHERE (((genre = $1) OR (genre = $2)) AND ((legacy_meta->>'capacity')::bigint >= $3))",
				expectedParams: []any{int64(1), int64(2), int64(100)},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListShelvesRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})

	t.Run("Book complex filters with integers", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
		}{
			{
				name:           "integer range with string",
				filter:         `publication_year >= 2000 AND publication_year < 2020 AND title = "My Book*"`,
				expectedClause: "WHERE (((publication_year >= $1) AND (publication_year < $2)) AND (title LIKE $3))",
				expectedParams: []any{int64(2000), int64(2020), "My Book%"},
			},
			{
				name:           "nested metadata with labels",
				filter:         `metadata.language = "en" AND labels:"category"`,
				expectedClause: "WHERE ((metadata->>'language' = $1) AND (COALESCE(labels, '{}') ? $2))",
				expectedParams: []any{"en", "category"},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListBooksRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})
}

func TestFilteringRequestParser_SelectivePaths(t *testing.T) {
	parser := MustNewFilteringRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()

	t.Run("allowed paths work", func(t *testing.T) {
		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
		}{
			{
				name:           "title is allowed",
				filter:         `title = "The Book"`,
				expectedClause: "WHERE (title = $1)",
				expectedParams: []any{"The Book"},
			},
			{
				name:           "author is allowed",
				filter:         `author = "organizations/123/authors/456"`,
				expectedClause: "WHERE (author = $1)",
				expectedParams: []any{"organizations/123/authors/456"},
			},
			{
				name:           "isbn is allowed",
				filter:         `isbn = "978-3-16-148410-0"`,
				expectedClause: "WHERE (isbn = $1)",
				expectedParams: []any{"978-3-16-148410-0"},
			},
			{
				name:           "publication_year is allowed",
				filter:         `publication_year = 2024`,
				expectedClause: "WHERE (publication_year = $1)",
				expectedParams: []any{int64(2024)},
			},
			{
				name:           "metadata.summary via wildcard",
				filter:         `metadata.summary = "A summary"`,
				expectedClause: "WHERE (metadata->>'summary' = $1)",
				expectedParams: []any{"A summary"},
			},
			{
				name:           "metadata.language via wildcard",
				filter:         `metadata.language = "en"`,
				expectedClause: "WHERE (metadata->>'language' = $1)",
				expectedParams: []any{"en"},
			},
			{
				name:           "labels is allowed",
				filter:         `labels:"category"`,
				expectedClause: "WHERE (COALESCE(labels, '{}') ? $1)",
				expectedParams: []any{"category"},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListBooksRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})

	t.Run("disallowed paths fail", func(t *testing.T) {
		tests := []struct {
			name   string
			filter string
		}{
			{
				name:   "name is not in allowed paths",
				filter: `name = "organizations/123/shelves/456/books/789"`,
			},
			{
				name:   "create_time is not in allowed paths",
				filter: `create_time > "2024-01-01T00:00:00Z"`,
			},
			{
				name:   "page_count is not in allowed paths",
				filter: `page_count > 100`,
			},
			{
				name:   "etag is not in allowed paths",
				filter: `etag = "abc123"`,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListBooksRequest{Filter: tc.filter}
				_, err := parser.Parse(request)
				require.Error(t, err, "expected error for disallowed path: %s", tc.filter)
			})
		}
	})

	t.Run("combined allowed fields", func(t *testing.T) {
		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
		}{
			{
				name:           "multiple allowed fields combined",
				filter:         `title = "Book" AND publication_year >= 2000 AND metadata.language = "en"`,
				expectedClause: "WHERE (((title = $1) AND (publication_year >= $2)) AND (metadata->>'language' = $3))",
				expectedParams: []any{"Book", int64(2000), "en"},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListBooksRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})
}

func TestApplyReplacement(t *testing.T) {
	tests := []struct {
		name        string
		clause      string
		field       string
		replacement string
		expected    string
	}{
		{
			name:        "simple field replacement",
			clause:      "hello = 1",
			field:       "hello",
			replacement: "h",
			expected:    "h = 1",
		},
		{
			name:        "field with underscore",
			clause:      "user_id = 123",
			field:       "user_id",
			replacement: "uid",
			expected:    "uid = 123",
		},
		{
			name:        "multiple occurrences",
			clause:      "count > 5 AND count < 10",
			field:       "count",
			replacement: "cnt",
			expected:    "cnt > 5 AND cnt < 10",
		},
		{
			name:        "short field doesn't match longer path",
			clause:      "hello.my.path = 1",
			field:       "hello",
			replacement: "h",
			expected:    "hello.my.path = 1",
		},
		{
			name:        "medium field doesn't match longer path",
			clause:      "hello.my.path = 1",
			field:       "hello.my",
			replacement: "hello@my",
			expected:    "hello.my.path = 1",
		},
		{
			name:        "only replaces exact field not substrings",
			clause:      "hello.my.path = 1 AND hello.my = 2 AND hello = 3",
			field:       "hello",
			replacement: "h",
			expected:    "hello.my.path = 1 AND hello.my = 2 AND h = 3",
		},
		{
			name:        "field part of larger word - prefix",
			clause:      "user_id = 123",
			field:       "id",
			replacement: "i",
			expected:    "user_id = 123",
		},
		{
			name:        "field not present in clause",
			clause:      "other_field = 1",
			field:       "missing_field",
			replacement: "mf",
			expected:    "other_field = 1",
		},
		{
			name:        "empty clause",
			clause:      "",
			field:       "field",
			replacement: "f",
			expected:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := &Node{
				Path:                  tc.field,
				ReplacementPath:       tc.replacement,
				ReplacementPathRegexp: getReplacementPathRegexp(tc.field),
			}
			result := node.ApplyReplacement(tc.clause)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestFilteringRequestParser_ColumnNameReplacement(t *testing.T) {
	parser := MustNewFilteringRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
	}{
		{
			name:           "external_id uses ext_id column",
			filter:         `external_id = "abc123"`,
			expectedClause: "WHERE (ext_id = $1)",
			expectedParams: []any{"abc123"},
		},
		{
			name:           "correlation_id_2 uses correlation_id column",
			filter:         `correlation_id_2 = "corr-456"`,
			expectedClause: "WHERE (correlation_id = $1)",
			expectedParams: []any{"corr-456"},
		},
		{
			name:           "external_id presence check",
			filter:         `external_id:*`,
			expectedClause: "WHERE (ext_id IS NOT NULL AND ext_id != '')",
			expectedParams: []any{},
		},
		{
			name:           "correlation_id_2 presence check",
			filter:         `correlation_id_2:*`,
			expectedClause: "WHERE (correlation_id IS NOT NULL AND correlation_id != '')",
			expectedParams: []any{},
		},
		{
			name:           "combined column replacements",
			filter:         `external_id = "ext" AND correlation_id_2 = "corr"`,
			expectedClause: "WHERE ((ext_id = $1) AND (correlation_id = $2))",
			expectedParams: []any{"ext", "corr"},
		},
		{
			name:           "nested field with parent column replacement",
			filter:         `metadata.capacity > 100`,
			expectedClause: "WHERE ((legacy_meta->>'capacity')::bigint > $1)",
			expectedParams: []any{int64(100)},
		},
		{
			name:           "mixed standard and renamed columns",
			filter:         `display_name = "Test" AND external_id = "ext123" AND metadata.capacity >= 50`,
			expectedClause: "WHERE (((display_name = $1) AND (ext_id = $2)) AND ((legacy_meta->>'capacity')::bigint >= $3))",
			expectedParams: []any{"Test", "ext123", int64(50)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListShelvesRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

func TestFilteringRequestParser_Duration(t *testing.T) {
	t.Run("Shelf nullable duration", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
			wantErr        bool
		}{
			{
				name:           "duration presence check",
				filter:         `duration:*`,
				expectedClause: "WHERE (duration IS NOT NULL)",
				expectedParams: []any{},
			},
			{
				name:           "duration not present",
				filter:         `NOT duration:*`,
				expectedClause: "WHERE (NOT (duration IS NOT NULL))",
				expectedParams: []any{},
			},
			{
				name:           "duration equals 5s",
				filter:         `duration = duration("5s")`,
				expectedClause: "WHERE (duration = $1)",
				expectedParams: []any{5 * time.Second},
			},
			{
				name:           "duration greater than 1m",
				filter:         `duration > duration("1m")`,
				expectedClause: "WHERE (duration > $1)",
				expectedParams: []any{time.Minute},
			},
			{
				name:           "duration less than or equal 30s",
				filter:         `duration <= duration("30s")`,
				expectedClause: "WHERE (duration <= $1)",
				expectedParams: []any{30 * time.Second},
			},
			{
				name:           "duration range with AND",
				filter:         `duration >= duration("10s") AND duration < duration("1h")`,
				expectedClause: "WHERE ((duration >= $1) AND (duration < $2))",
				expectedParams: []any{10 * time.Second, time.Hour},
			},
			{
				name:           "duration combined with other fields",
				filter:         `duration:* AND display_name = "Test"`,
				expectedClause: "WHERE ((duration IS NOT NULL) AND (display_name = $1))",
				expectedParams: []any{"Test"},
			},
			{
				name:    "invalid duration format",
				filter:  `duration = duration("not-a-duration")`,
				wantErr: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListShelvesRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})

	t.Run("Book non-nullable duration", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
			wantErr        bool
		}{
			{
				name:           "duration presence check",
				filter:         `duration:*`,
				expectedClause: "WHERE (duration IS NOT NULL)",
				expectedParams: []any{},
			},
			{
				name:           "duration equals 5s",
				filter:         `duration = duration("5s")`,
				expectedClause: "WHERE (duration = $1)",
				expectedParams: []any{5 * time.Second},
			},
			{
				name:           "duration greater than 2h30m",
				filter:         `duration > duration("2h30m")`,
				expectedClause: "WHERE (duration > $1)",
				expectedParams: []any{2*time.Hour + 30*time.Minute},
			},
			{
				name:           "duration not equal",
				filter:         `duration != duration("0s")`,
				expectedClause: "WHERE (duration != $1)",
				expectedParams: []any{time.Duration(0)},
			},
			{
				name:           "duration combined with title and publication_year",
				filter:         `duration > duration("1m") AND title = "MyBook" AND publication_year >= 2000`,
				expectedClause: "WHERE (((duration > $1) AND (title = $2)) AND (publication_year >= $3))",
				expectedParams: []any{time.Minute, "MyBook", int64(2000)},
			},
			{
				name:    "invalid duration format",
				filter:  `duration > duration("xyz")`,
				wantErr: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListBooksRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})

	t.Run("Book nested duration in metadata", func(t *testing.T) {
		parser := MustNewFilteringRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()

		tests := []struct {
			name           string
			filter         string
			expectedClause string
			expectedParams []any
			wantErr        bool
		}{
			{
				name:           "nested duration presence check",
				filter:         `metadata.duration:*`,
				expectedClause: "WHERE ((REPLACE(metadata->>'duration', 's', ''))::double precision IS NOT NULL)",
				expectedParams: []any{},
			},
			{
				name:           "nested duration equals 5s",
				filter:         `metadata.duration = duration("5s")`,
				expectedClause: "WHERE ((REPLACE(metadata->>'duration', 's', ''))::double precision = $1)",
				expectedParams: []any{float64(5)},
			},
			{
				name:           "nested duration greater than 1m",
				filter:         `metadata.duration > duration("1m")`,
				expectedClause: "WHERE ((REPLACE(metadata->>'duration', 's', ''))::double precision > $1)",
				expectedParams: []any{float64(60)},
			},
			{
				name:           "nested duration less than or equal 2h30m",
				filter:         `metadata.duration <= duration("2h30m")`,
				expectedClause: "WHERE ((REPLACE(metadata->>'duration', 's', ''))::double precision <= $1)",
				expectedParams: []any{float64(9000)},
			},
			{
				name:           "nested duration range",
				filter:         `metadata.duration >= duration("10s") AND metadata.duration < duration("1h")`,
				expectedClause: "WHERE (((REPLACE(metadata->>'duration', 's', ''))::double precision >= $1) AND ((REPLACE(metadata->>'duration', 's', ''))::double precision < $2))",
				expectedParams: []any{float64(10), float64(3600)},
			},
			{
				name:           "nested duration combined with title",
				filter:         `metadata.duration > duration("30s") AND title = "MyBook"`,
				expectedClause: "WHERE (((REPLACE(metadata->>'duration', 's', ''))::double precision > $1) AND (title = $2))",
				expectedParams: []any{float64(30), "MyBook"},
			},
			{
				name:    "nested duration invalid format",
				filter:  `metadata.duration = duration("bad")`,
				wantErr: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				request := &libraryservicepb.ListBooksRequest{Filter: tc.filter}
				parsedRequest, err := parser.Parse(request)
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			})
		}
	})
}

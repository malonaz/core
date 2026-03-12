package aip

import (
	"testing"

	"github.com/stretchr/testify/require"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestOrderingRequestParser_NewParser(t *testing.T) {
	t.Run("ListAuthorsRequest", func(t *testing.T) {
		parser, err := NewOrderingRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
		require.NoError(t, err)
		require.NotNil(t, parser)
		require.Equal(t, "create_time desc", parser.options.Default)
	})

	t.Run("ListBooksRequest", func(t *testing.T) {
		parser, err := NewOrderingRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()
		require.NoError(t, err)
		require.NotNil(t, parser)
		require.Equal(t, "create_time desc", parser.options.Default)
	})

	t.Run("ListShelvesRequest", func(t *testing.T) {
		parser, err := NewOrderingRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()
		require.NoError(t, err)
		require.NotNil(t, parser)
		require.Equal(t, "create_time desc", parser.options.Default)
	})
}

func TestOrderingRequestParser_Parse(t *testing.T) {
	parser, err := NewOrderingRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
	require.NoError(t, err)

	tests := []struct {
		name                 string
		orderBy              string
		expectedOrderBySQL   string
		wantErr              bool
		expectedErrorMessage string
	}{
		{
			name:               "default order by when empty",
			orderBy:            "",
			expectedOrderBySQL: "ORDER BY create_time DESC NULLS LAST",
		},
		{
			name:               "single field ascending",
			orderBy:            "create_time asc",
			expectedOrderBySQL: "ORDER BY create_time ASC NULLS LAST",
		},
		{
			name:               "single field descending",
			orderBy:            "create_time desc",
			expectedOrderBySQL: "ORDER BY create_time DESC NULLS LAST",
		},
		{
			name:               "single field implicit ascending",
			orderBy:            "update_time",
			expectedOrderBySQL: "ORDER BY update_time ASC NULLS LAST",
		},
		{
			name:               "multiple fields mixed order",
			orderBy:            "create_time desc, update_time asc",
			expectedOrderBySQL: "ORDER BY create_time DESC NULLS LAST, update_time ASC NULLS LAST",
		},
		{
			name:               "all allowed fields",
			orderBy:            "create_time asc, update_time desc, display_name asc",
			expectedOrderBySQL: "ORDER BY create_time ASC NULLS LAST, update_time DESC NULLS LAST, display_name ASC NULLS LAST",
		},
		{
			name:                 "unauthorized field",
			orderBy:              "unauthorized_field asc",
			wantErr:              true,
			expectedErrorMessage: "invalid order path",
		},
		{
			name:                 "mixed authorized and unauthorized fields",
			orderBy:              "create_time asc, unauthorized_field desc",
			wantErr:              true,
			expectedErrorMessage: "invalid order path",
		},
		{
			name:                 "invalid syntax - wrong direction keyword",
			orderBy:              "create_time ascending",
			wantErr:              true,
			expectedErrorMessage: "parsing order by",
		},
		{
			name:                 "invalid syntax - trailing comma",
			orderBy:              "create_time asc,",
			wantErr:              true,
			expectedErrorMessage: "parsing order by",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListAuthorsRequest{OrderBy: tc.orderBy}
			parsedRequest, err := parser.Parse(request)

			if tc.wantErr {
				require.Error(t, err)
				if tc.expectedErrorMessage != "" {
					require.Contains(t, err.Error(), tc.expectedErrorMessage)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, parsedRequest)
			require.Equal(t, tc.expectedOrderBySQL, parsedRequest.GetSQLOrderByClause())
		})
	}
}

func TestOrderingRequestParser_Parse_Books(t *testing.T) {
	parser, err := NewOrderingRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()
	require.NoError(t, err)

	tests := []struct {
		name               string
		orderBy            string
		expectedOrderBySQL string
		wantErr            bool
	}{
		{
			name:               "default order by",
			orderBy:            "",
			expectedOrderBySQL: "ORDER BY create_time DESC NULLS LAST",
		},
		{
			name:               "title ascending",
			orderBy:            "title asc",
			expectedOrderBySQL: "ORDER BY title ASC NULLS LAST",
		},
		{
			name:               "publication_year descending",
			orderBy:            "publication_year desc",
			expectedOrderBySQL: "ORDER BY publication_year DESC NULLS LAST",
		},
		{
			name:               "multiple book-specific fields",
			orderBy:            "title asc, publication_year desc",
			expectedOrderBySQL: "ORDER BY title ASC NULLS LAST, publication_year DESC NULLS LAST",
		},
		{
			name:               "all allowed book fields",
			orderBy:            "create_time desc, update_time asc, title desc, publication_year asc",
			expectedOrderBySQL: "ORDER BY create_time DESC NULLS LAST, update_time ASC NULLS LAST, title DESC NULLS LAST, publication_year ASC NULLS LAST",
		},
		{
			name:    "field not in book ordering paths",
			orderBy: "display_name asc",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListBooksRequest{OrderBy: tc.orderBy}
			parsedRequest, err := parser.Parse(request)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedOrderBySQL, parsedRequest.GetSQLOrderByClause())
		})
	}
}

func TestOrderingRequestParser_DefaultOrderByInjection(t *testing.T) {
	parser, err := NewOrderingRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
	require.NoError(t, err)

	t.Run("empty order_by receives default", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: ""}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, parser.options.Default, request.GetOrderBy())
		require.NotEmpty(t, parsedRequest.GetSQLOrderByClause())
	})

	t.Run("explicit order_by preserved", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: "display_name asc"}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "display_name asc", request.GetOrderBy())
		require.NotEmpty(t, parsedRequest.GetSQLOrderByClause())
	})
}

func TestOrderingRequestParser_SQLTranspilation(t *testing.T) {
	parser, err := NewOrderingRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
	require.NoError(t, err)

	tests := []struct {
		name        string
		orderBy     string
		expectedSQL string
	}{
		{
			name:        "ascending includes ASC keyword",
			orderBy:     "create_time asc",
			expectedSQL: "ORDER BY create_time ASC NULLS LAST",
		},
		{
			name:        "descending includes DESC keyword",
			orderBy:     "create_time desc",
			expectedSQL: "ORDER BY create_time DESC NULLS LAST",
		},
		{
			name:        "multiple fields mixed directions",
			orderBy:     "create_time desc, display_name asc",
			expectedSQL: "ORDER BY create_time DESC NULLS LAST, display_name ASC NULLS LAST",
		},
		{
			name:        "all ascending",
			orderBy:     "create_time asc, update_time asc",
			expectedSQL: "ORDER BY create_time ASC NULLS LAST, update_time ASC NULLS LAST",
		},
		{
			name:        "all descending",
			orderBy:     "create_time desc, update_time desc",
			expectedSQL: "ORDER BY create_time DESC NULLS LAST, update_time DESC NULLS LAST",
		},
		{
			name:        "three fields alternating",
			orderBy:     "create_time desc, update_time asc, display_name desc",
			expectedSQL: "ORDER BY create_time DESC NULLS LAST, update_time ASC NULLS LAST, display_name DESC NULLS LAST",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListAuthorsRequest{OrderBy: tc.orderBy}
			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSQL, parsedRequest.GetSQLOrderByClause())
		})
	}
}

func TestOrderingRequestParser_DifferentResourceHierarchies(t *testing.T) {
	t.Run("Author - two level hierarchy", func(t *testing.T) {
		parser, err := NewOrderingRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
		require.NoError(t, err)
		require.NotNil(t, parser.tree.Resource)
		require.Equal(t, "author", parser.tree.Resource.Singular)
	})

	t.Run("Book - three level hierarchy", func(t *testing.T) {
		parser, err := NewOrderingRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()
		require.NoError(t, err)
		require.NotNil(t, parser.tree.Resource)
		require.Equal(t, "book", parser.tree.Resource.Singular)
	})

	t.Run("Shelf - two level hierarchy", func(t *testing.T) {
		parser, err := NewOrderingRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()
		require.NoError(t, err)
		require.NotNil(t, parser.tree.Resource)
		require.Equal(t, "shelf", parser.tree.Resource.Singular)
	})
}

func TestOrderingRequestParser_PathAllowValidation(t *testing.T) {
	authorParser, err := NewOrderingRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
	require.NoError(t, err)

	bookParser, err := NewOrderingRequestParser[*libraryservicepb.ListBooksRequest, *librarypb.Book]()
	require.NoError(t, err)

	t.Run("author allows display_name", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: "display_name asc"}
		_, err := authorParser.Parse(request)
		require.NoError(t, err)
	})

	t.Run("book disallows display_name", func(t *testing.T) {
		request := &libraryservicepb.ListBooksRequest{OrderBy: "display_name asc"}
		_, err := bookParser.Parse(request)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid order path")
	})

	t.Run("book allows title", func(t *testing.T) {
		request := &libraryservicepb.ListBooksRequest{OrderBy: "title desc"}
		_, err := bookParser.Parse(request)
		require.NoError(t, err)
	})

	t.Run("author disallows title", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: "title desc"}
		_, err := authorParser.Parse(request)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid order path")
	})

	t.Run("book allows publication_year", func(t *testing.T) {
		request := &libraryservicepb.ListBooksRequest{OrderBy: "publication_year asc"}
		_, err := bookParser.Parse(request)
		require.NoError(t, err)
	})

	t.Run("common fields allowed in both", func(t *testing.T) {
		authorRequest := &libraryservicepb.ListAuthorsRequest{OrderBy: "create_time desc, update_time asc"}
		_, err := authorParser.Parse(authorRequest)
		require.NoError(t, err)

		bookRequest := &libraryservicepb.ListBooksRequest{OrderBy: "create_time desc, update_time asc"}
		_, err = bookParser.Parse(bookRequest)
		require.NoError(t, err)
	})
}

func TestOrderingRequestParser_ImplicitDirection(t *testing.T) {
	parser, err := NewOrderingRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
	require.NoError(t, err)

	t.Run("implicit ascending single field", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: "create_time"}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "ORDER BY create_time ASC NULLS LAST", parsedRequest.GetSQLOrderByClause())
	})

	t.Run("implicit ascending multiple fields", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: "create_time, update_time"}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "ORDER BY create_time ASC NULLS LAST, update_time ASC NULLS LAST", parsedRequest.GetSQLOrderByClause())
	})

	t.Run("mixed implicit and explicit", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: "create_time, update_time desc, display_name"}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "ORDER BY create_time ASC NULLS LAST, update_time DESC NULLS LAST, display_name ASC NULLS LAST", parsedRequest.GetSQLOrderByClause())
	})
}

func TestOrderingRequestParser_EdgeCases(t *testing.T) {
	parser, err := NewOrderingRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
	require.NoError(t, err)

	t.Run("whitespace handling", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: "  create_time   desc  "}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "ORDER BY create_time DESC NULLS LAST", parsedRequest.GetSQLOrderByClause())
	})

	t.Run("repeated field same direction", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: "create_time asc, create_time asc"}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "ORDER BY create_time ASC NULLS LAST, create_time ASC NULLS LAST", parsedRequest.GetSQLOrderByClause())
	})

	t.Run("repeated field different directions", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: "create_time asc, create_time desc"}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "ORDER BY create_time ASC NULLS LAST, create_time DESC NULLS LAST", parsedRequest.GetSQLOrderByClause())
	})
}

func TestOrderingRequestParser_MustNewPanicsOnInvalidConfig(t *testing.T) {
	t.Run("valid config does not panic", func(t *testing.T) {
		require.NotPanics(t, func() {
			MustNewOrderingRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
		})
	})
}

func TestOrderingRequestParser_RequestMutation(t *testing.T) {
	parser, err := NewOrderingRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()
	require.NoError(t, err)

	t.Run("empty request gets default set", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{}
		_, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "create_time desc", request.GetOrderBy())
	})

	t.Run("explicit request preserved", func(t *testing.T) {
		request := &libraryservicepb.ListAuthorsRequest{OrderBy: "update_time asc"}
		_, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "update_time asc", request.GetOrderBy())
	})
}

func TestOrderingRequestParser_ColumnNameReplacement(t *testing.T) {
	parser, err := NewOrderingRequestParser[*libraryservicepb.ListShelvesRequest, *librarypb.Shelf]()
	require.NoError(t, err)

	tests := []struct {
		name               string
		orderBy            string
		expectedOrderBySQL string
		wantErr            bool
	}{
		{
			name:               "external_id uses ext_id column ascending",
			orderBy:            "external_id asc",
			expectedOrderBySQL: "ORDER BY ext_id ASC NULLS LAST",
		},
		{
			name:               "external_id uses ext_id column descending",
			orderBy:            "external_id desc",
			expectedOrderBySQL: "ORDER BY ext_id DESC NULLS LAST",
		},
		{
			name:               "correlation_id_2 uses correlation_id column",
			orderBy:            "correlation_id_2 desc",
			expectedOrderBySQL: "ORDER BY correlation_id DESC NULLS LAST",
		},
		{
			name:               "multiple renamed columns",
			orderBy:            "external_id asc, correlation_id_2 desc",
			expectedOrderBySQL: "ORDER BY ext_id ASC NULLS LAST, correlation_id DESC NULLS LAST",
		},
		{
			name:               "mixed standard and renamed columns",
			orderBy:            "display_name asc, external_id desc, create_time asc",
			expectedOrderBySQL: "ORDER BY display_name ASC NULLS LAST, ext_id DESC NULLS LAST, create_time ASC NULLS LAST",
		},
		{
			name:               "renamed column with standard columns",
			orderBy:            "create_time desc, correlation_id_2 asc",
			expectedOrderBySQL: "ORDER BY create_time DESC NULLS LAST, correlation_id ASC NULLS LAST",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListShelvesRequest{OrderBy: tc.orderBy}
			parsedRequest, err := parser.Parse(request)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedOrderBySQL, parsedRequest.GetSQLOrderByClause())
		})
	}
}

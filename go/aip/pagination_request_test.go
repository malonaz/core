package aip

import (
	"testing"

	"github.com/stretchr/testify/require"

	libraryservicepb "github.com/malonaz/core/genproto/test/library/library_service/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestPaginationRequestParser_NewParser(t *testing.T) {
	parser, err := NewPaginationRequestParser[*libraryservicepb.ListAuthorsRequest]()
	require.NoError(t, err)
	require.NotNil(t, parser)
	require.Equal(t, uint32(100), parser.options.DefaultPageSize)
}

func TestPaginationRequestParser_Parse(t *testing.T) {
	parser, err := NewPaginationRequestParser[*libraryservicepb.ListAuthorsRequest]()
	require.NoError(t, err)

	tests := []struct {
		name                  string
		request               *libraryservicepb.ListAuthorsRequest
		expectedPageSize      int32
		expectedPaginationSQL string
		hasNextPageToken      bool
		itemsFetched          int
	}{
		{
			name:                  "default page size - no page token",
			request:               &libraryservicepb.ListAuthorsRequest{},
			expectedPageSize:      100,
			expectedPaginationSQL: "OFFSET 0 LIMIT 101",
			hasNextPageToken:      false,
			itemsFetched:          30,
		},
		{
			name: "custom page size - no page token",
			request: &libraryservicepb.ListAuthorsRequest{
				PageSize: 25,
			},
			expectedPageSize:      25,
			expectedPaginationSQL: "OFFSET 0 LIMIT 26",
			hasNextPageToken:      false,
			itemsFetched:          20,
		},
		{
			name: "next page token generation - more items than page size",
			request: &libraryservicepb.ListAuthorsRequest{
				PageSize: 10,
			},
			expectedPageSize:      10,
			expectedPaginationSQL: "OFFSET 0 LIMIT 11",
			hasNextPageToken:      true,
			itemsFetched:          11,
		},
		{
			name: "large page size",
			request: &libraryservicepb.ListAuthorsRequest{
				PageSize: 1000,
			},
			expectedPageSize:      1000,
			expectedPaginationSQL: "OFFSET 0 LIMIT 1001",
			hasNextPageToken:      false,
			itemsFetched:          500,
		},
		{
			name:                  "page size zero (default used)",
			request:               &libraryservicepb.ListAuthorsRequest{},
			expectedPageSize:      100,
			expectedPaginationSQL: "OFFSET 0 LIMIT 101",
			hasNextPageToken:      true,
			itemsFetched:          101,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsedRequest, err := parser.Parse(tc.request)
			require.NoError(t, err)
			require.NotNil(t, parsedRequest)
			require.Equal(t, tc.expectedPageSize, tc.request.GetPageSize())
			require.Equal(t, tc.expectedPaginationSQL, parsedRequest.GetSQLPaginationClause())

			nextPageToken := parsedRequest.GetNextPageToken(tc.itemsFetched)
			if tc.hasNextPageToken {
				require.NotEmpty(t, nextPageToken)
			} else {
				require.Empty(t, nextPageToken)
			}
		})
	}
}

func TestPaginationRequestParser_PageTokenRoundTrip(t *testing.T) {
	parser, err := NewPaginationRequestParser[*libraryservicepb.ListAuthorsRequest]()
	require.NoError(t, err)

	tests := []struct {
		name          string
		pageSize      int32
		itemsPerFetch []int
	}{
		{
			name:          "single page - no next token",
			pageSize:      10,
			itemsPerFetch: []int{5},
		},
		{
			name:          "multiple pages",
			pageSize:      10,
			itemsPerFetch: []int{11, 11, 8},
		},
		{
			name:          "exact page boundary",
			pageSize:      20,
			itemsPerFetch: []int{21, 15},
		},
		{
			name:          "many pages",
			pageSize:      5,
			itemsPerFetch: []int{6, 6, 6, 6, 3},
		},
		{
			name:          "single item pages",
			pageSize:      1,
			itemsPerFetch: []int{2, 2, 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			currentPageToken := ""

			for pageIdx, itemsFetched := range tc.itemsPerFetch {
				request := &libraryservicepb.ListAuthorsRequest{
					PageSize:  tc.pageSize,
					PageToken: currentPageToken,
				}

				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)

				nextPageToken := parsedRequest.GetNextPageToken(itemsFetched)
				isLastPage := pageIdx == len(tc.itemsPerFetch)-1
				if isLastPage {
					require.Empty(t, nextPageToken)
				} else {
					require.NotEmpty(t, nextPageToken)
					currentPageToken = nextPageToken
				}
			}
		})
	}
}

func TestPaginationRequestParser_DifferentResources(t *testing.T) {
	t.Run("ListAuthorsRequest", func(t *testing.T) {
		parser, err := NewPaginationRequestParser[*libraryservicepb.ListAuthorsRequest]()
		require.NoError(t, err)
		require.Equal(t, uint32(100), parser.options.DefaultPageSize)

		request := &libraryservicepb.ListAuthorsRequest{}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "OFFSET 0 LIMIT 101", parsedRequest.GetSQLPaginationClause())
	})

	t.Run("ListBooksRequest", func(t *testing.T) {
		parser, err := NewPaginationRequestParser[*libraryservicepb.ListBooksRequest]()
		require.NoError(t, err)
		require.Equal(t, uint32(100), parser.options.DefaultPageSize)

		request := &libraryservicepb.ListBooksRequest{}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "OFFSET 0 LIMIT 101", parsedRequest.GetSQLPaginationClause())
	})

	t.Run("ListShelvesRequest", func(t *testing.T) {
		parser, err := NewPaginationRequestParser[*libraryservicepb.ListShelvesRequest]()
		require.NoError(t, err)
		require.Equal(t, uint32(100), parser.options.DefaultPageSize)

		request := &libraryservicepb.ListShelvesRequest{}
		parsedRequest, err := parser.Parse(request)
		require.NoError(t, err)
		require.Equal(t, "OFFSET 0 LIMIT 101", parsedRequest.GetSQLPaginationClause())
	})
}

func TestPaginatedRequest_GetNextPageToken(t *testing.T) {
	parser, err := NewPaginationRequestParser[*libraryservicepb.ListAuthorsRequest]()
	require.NoError(t, err)

	tests := []struct {
		name             string
		pageSize         int32
		itemsFetched     int
		hasNextPageToken bool
	}{
		{
			name:             "no next page - fetched less than page size",
			pageSize:         10,
			itemsFetched:     8,
			hasNextPageToken: false,
		},
		{
			name:             "no next page - fetched exactly page size",
			pageSize:         10,
			itemsFetched:     10,
			hasNextPageToken: false,
		},
		{
			name:             "has next page - fetched more than page size",
			pageSize:         10,
			itemsFetched:     11,
			hasNextPageToken: true,
		},
		{
			name:             "page size zero (default used)",
			pageSize:         0,
			itemsFetched:     101,
			hasNextPageToken: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &libraryservicepb.ListAuthorsRequest{
				PageSize: tc.pageSize,
			}

			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)

			nextPageToken := parsedRequest.GetNextPageToken(tc.itemsFetched)
			if tc.hasNextPageToken {
				require.NotEmpty(t, nextPageToken)
			} else {
				require.Empty(t, nextPageToken)
			}
		})
	}
}

func TestPaginationRequestParser_MustNew(t *testing.T) {
	require.NotPanics(t, func() {
		MustNewPaginationRequestParser[*libraryservicepb.ListAuthorsRequest]()
	})
}

func TestPaginationRequestParser_ListRequestParser(t *testing.T) {
	parser := MustNewListRequestParser[*libraryservicepb.ListAuthorsRequest, *librarypb.Author]()

	request := &libraryservicepb.ListAuthorsRequest{
		PageSize: 50,
	}

	parsedRequest, err := parser.Parse(request)
	require.NoError(t, err)
	require.Equal(t, "OFFSET 0 LIMIT 51", parsedRequest.GetSQLPaginationClause())
}

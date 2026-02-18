package aip

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/malonaz/core/genproto/test/aip"
)

func TestPaginationRequestParser_NewParser(t *testing.T) {
	tests := []struct {
		name             string
		createParser     func() (*PaginationRequestParser[*pb.PaginateOnlyRequest], error)
		wantErr          bool
		expectedPageSize uint32
	}{
		{
			name: "valid parser creation",
			createParser: func() (*PaginationRequestParser[*pb.PaginateOnlyRequest], error) {
				return NewPaginationRequestParser[*pb.PaginateOnlyRequest]()
			},
			wantErr:          false,
			expectedPageSize: 50,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser, err := tc.createParser()

			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, parser)
			} else {
				require.NoError(t, err)
				require.NotNil(t, parser)
				require.Equal(t, tc.expectedPageSize, parser.options.DefaultPageSize)
			}
		})
	}
}

func TestPaginationRequestParser_Parse(t *testing.T) {
	parser, err := NewPaginationRequestParser[*pb.PaginateOnlyRequest]()
	require.NoError(t, err)

	tests := []struct {
		name                  string
		request               *pb.PaginateOnlyRequest
		expectedPageSize      int32
		expectedPaginationSQL string
		hasNextPageToken      bool
		itemsFetched          int
	}{
		{
			name: "default page size - no page token",
			request: &pb.PaginateOnlyRequest{
				PageSize:  0, // Should use default
				PageToken: "",
			},
			expectedPageSize:      50,
			expectedPaginationSQL: "OFFSET 0 LIMIT 51",
			hasNextPageToken:      false,
			itemsFetched:          30, // Less than page size
		},
		{
			name: "custom page size - no page token",
			request: &pb.PaginateOnlyRequest{
				PageSize:  25,
				PageToken: "",
			},
			expectedPageSize:      25,
			expectedPaginationSQL: "OFFSET 0 LIMIT 26",
			hasNextPageToken:      false,
			itemsFetched:          20, // Less than page size
		},
		{
			name: "next page token generation - more items than page size",
			request: &pb.PaginateOnlyRequest{
				PageSize:  10,
				PageToken: "",
			},
			expectedPageSize:      10,
			expectedPaginationSQL: "OFFSET 0 LIMIT 11",
			hasNextPageToken:      true,
			itemsFetched:          11, // More than page size (fetched page_size + 1)
		},
		{
			name: "large page size",
			request: &pb.PaginateOnlyRequest{
				PageSize:  1000,
				PageToken: "",
			},
			expectedPageSize:      1000,
			expectedPaginationSQL: "OFFSET 0 LIMIT 1001",
			hasNextPageToken:      false,
			itemsFetched:          500,
		},
		{
			name: "page size zero (default used)",
			request: &pb.PaginateOnlyRequest{
				PageSize:  0, // Will use default of 50
				PageToken: "",
			},
			expectedPageSize:      50,
			expectedPaginationSQL: "OFFSET 0 LIMIT 51",
			hasNextPageToken:      true,
			itemsFetched:          51,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsedRequest, err := parser.Parse(tc.request)
			require.NoError(t, err)
			require.NotNil(t, parsedRequest)

			// Verify the parsed request has correct page size
			require.Equal(t, tc.expectedPageSize, tc.request.GetPageSize())

			// Verify SQL pagination clause
			paginationSQL := parsedRequest.GetSQLPaginationClause()
			require.Equal(t, tc.expectedPaginationSQL, paginationSQL)

			// Verify next page token generation
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
	parser, err := NewPaginationRequestParser[*pb.PaginateOnlyRequest]()
	require.NoError(t, err)

	tests := []struct {
		name          string
		pageSize      int32
		itemsPerFetch []int // Items returned on each fetch (including the +1 lookahead)
	}{
		{
			name:          "single page - no next token",
			pageSize:      10,
			itemsPerFetch: []int{5}, // Less than page size, so no next page
		},
		{
			name:          "multiple pages",
			pageSize:      10,
			itemsPerFetch: []int{11, 11, 8}, // Full page + 1 lookahead, then last page
		},
		{
			name:          "exact page boundary",
			pageSize:      20,
			itemsPerFetch: []int{21, 15}, // Full page + 1, then partial last page
		},
		{
			name:          "many pages",
			pageSize:      5,
			itemsPerFetch: []int{6, 6, 6, 6, 3}, // Four full pages, then last partial page
		},
		{
			name:          "single item pages",
			pageSize:      1,
			itemsPerFetch: []int{2, 2, 1}, // Multiple single-item pages
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			currentPageToken := ""

			for pageIdx, itemsFetched := range tc.itemsPerFetch {
				request := &pb.PaginateOnlyRequest{
					PageSize:  tc.pageSize,
					PageToken: currentPageToken,
				}

				parsedRequest, err := parser.Parse(request)
				require.NoError(t, err)

				// Get the next page token
				nextPageToken := parsedRequest.GetNextPageToken(itemsFetched)

				// If this is the last page, there should be no next token
				isLastPage := pageIdx == len(tc.itemsPerFetch)-1
				if isLastPage {
					require.Empty(t, nextPageToken, "Last page should have no next token")
				} else {
					require.NotEmpty(t, nextPageToken, "Non-last page should have next token when itemsFetched > pageSize")
					currentPageToken = nextPageToken
				}
			}
		})
	}
}

func TestPaginationRequestParser_DifferentDefaultPageSizes(t *testing.T) {
	tests := []struct {
		name             string
		createRequest    func() any
		expectedDefault  uint32
		expectedSQLLimit string
	}{
		{
			name: "default page size 50",
			createRequest: func() any {
				return &pb.PaginateOnlyRequest{PageSize: 0, PageToken: ""}
			},
			expectedDefault:  50,
			expectedSQLLimit: "OFFSET 0 LIMIT 51",
		},
		{
			name: "default page size 10",
			createRequest: func() any {
				return &pb.PaginateWithSmallDefaultRequest{PageSize: 0, PageToken: ""}
			},
			expectedDefault:  10,
			expectedSQLLimit: "OFFSET 0 LIMIT 11",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			switch req := tc.createRequest().(type) {
			case *pb.PaginateOnlyRequest:
				parser, err := NewPaginationRequestParser[*pb.PaginateOnlyRequest]()
				require.NoError(t, err)
				require.Equal(t, tc.expectedDefault, parser.options.DefaultPageSize)

				parsedRequest, err := parser.Parse(req)
				require.NoError(t, err)
				require.Equal(t, tc.expectedSQLLimit, parsedRequest.GetSQLPaginationClause())

			case *pb.PaginateWithSmallDefaultRequest:
				parser, err := NewPaginationRequestParser[*pb.PaginateWithSmallDefaultRequest]()
				require.NoError(t, err)
				require.Equal(t, tc.expectedDefault, parser.options.DefaultPageSize)

				parsedRequest, err := parser.Parse(req)
				require.NoError(t, err)
				require.Equal(t, tc.expectedSQLLimit, parsedRequest.GetSQLPaginationClause())
			}
		})
	}
}

func TestPaginatedRequest_GetNextPageToken(t *testing.T) {
	parser, err := NewPaginationRequestParser[*pb.PaginateOnlyRequest]()
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
			pageSize:         0, // Will use default of 50
			itemsFetched:     51,
			hasNextPageToken: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.PaginateOnlyRequest{
				PageSize:  tc.pageSize,
				PageToken: "",
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

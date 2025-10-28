package aip

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/malonaz/core/genproto/test/aip"
	"github.com/malonaz/core/go/aip"
)

func escapeDollar(s string) string {
	return strings.ReplaceAll(s, "$", "@")
}

func TestParser_ParseRequest(t *testing.T) {
	parser := aip.NewListRequestParser(&pb.ListResourcesRequest{})
	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
	}{
		{
			name:           "FilterByID",
			filter:         `id="testUser"`,
			expectedClause: "WHERE (id = $1)",
			expectedParams: []any{"testUser"},
		},
		{
			name:           "Replacement",
			filter:         `replaceable_field="testUser"`,
			expectedClause: "WHERE (replaced_field = $1)",
			expectedParams: []any{"testUser"},
		},
		{
			name:           "FilterByCreateTimestamp",
			filter:         `create_timestamp > 1609459200000000`, // 2021-01-01 00:00:00 UTC in microseconds
			expectedClause: "WHERE (create_timestamp > $1)",
			expectedParams: []any{int64(1609459200000000)},
		},
		{
			name:           "FilterByDeletedStatus",
			filter:         `NOT deleted`,
			expectedClause: "WHERE (NOT deleted)",
			expectedParams: []any{},
		},
		{
			name:           "IsNull",
			filter:         `ISNULL(id)`,
			expectedClause: "WHERE (id IS NULL)",
			expectedParams: []any{},
		},
		{
			name:           "IsNull (enum)",
			filter:         `ISNULL(my_enum)`,
			expectedClause: "WHERE (my_enum IS NULL)",
			expectedParams: []any{},
		},
		{
			name:           "jsonb query",
			filter:         `JSONB(json_field@key) = 'value'`,
			expectedClause: "WHERE ((json_field->>'key') = $1)",
			expectedParams: []any{"value"},
		},
		{
			name:           "jsonb nested query",
			filter:         `JSONB(json_field@key@key2) = 'value'`,
			expectedClause: "WHERE ((json_field->'key'->>'key2') = $1)",
			expectedParams: []any{"value"},
		},
		{
			name:           "ComplexFilter",
			filter:         `id="testUser" AND create_timestamp > 1609459200000000 AND NOT deleted`,
			expectedClause: "WHERE (((id = $1) AND (create_timestamp > $2)) AND (NOT deleted))",
			expectedParams: []any{"testUser", int64(1609459200000000)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				Filter:   tc.filter,
				PageSize: 0,
				OrderBy:  "create_timestamp desc",
			}
			parsedRequest, err := parser.ParseRequest(request)
			require.NoError(t, err)

			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
			paginationClause := parsedRequest.GetSQLPaginationClause()
			require.Equal(t, "OFFSET 0 LIMIT 1001", paginationClause)
		})

		t.Run(tc.name+"_default_page_size", func(t *testing.T) {
			// Second request with set page size.
			request := &pb.ListResourcesRequest{
				Filter:   tc.filter,
				PageSize: 10,
				OrderBy:  "create_timestamp desc",
			}
			parsedRequest, err := parser.ParseRequest(request)
			require.NoError(t, err)

			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
			paginationClause := parsedRequest.GetSQLPaginationClause()
			require.Equal(t, "OFFSET 0 LIMIT 11", paginationClause)
		})
	}
}

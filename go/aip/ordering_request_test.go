package aip

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/malonaz/core/genproto/test/aip"
)

func TestOrderingRequestParser_NewParser(t *testing.T) {
	tests := []struct {
		name            string
		createParser    func() (*OrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource], error)
		wantErr         bool
		expectedDefault string
	}{
		{
			name: "valid parser creation",
			createParser: func() (*OrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource], error) {
				return NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
			},
			wantErr:         false,
			expectedDefault: "create_timestamp desc",
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
				require.Equal(t, tc.expectedDefault, parser.options.Default)
			}
		})
	}
}

func TestOrderingRequestParser_Parse(t *testing.T) {
	parser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
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
			expectedOrderBySQL: "ORDER BY create_timestamp DESC",
			wantErr:            false,
		},
		{
			name:               "single field ascending",
			orderBy:            "id asc",
			expectedOrderBySQL: "ORDER BY id",
			wantErr:            false,
		},
		{
			name:               "single field descending",
			orderBy:            "id desc",
			expectedOrderBySQL: "ORDER BY id DESC",
			wantErr:            false,
		},
		{
			name:               "multiple fields mixed order",
			orderBy:            "create_timestamp desc, id asc",
			expectedOrderBySQL: "ORDER BY create_timestamp DESC, id",
			wantErr:            false,
		},
		{
			name:               "default field descending",
			orderBy:            "create_timestamp desc",
			expectedOrderBySQL: "ORDER BY create_timestamp DESC",
			wantErr:            false,
		},
		{
			name:               "all allowed fields",
			orderBy:            "id asc, create_timestamp desc, update_time asc",
			expectedOrderBySQL: "ORDER BY id, create_timestamp DESC, update_time",
			wantErr:            false,
		},
		{
			name:                 "unauthorized field",
			orderBy:              "unauthorized_field asc",
			expectedOrderBySQL:   "",
			wantErr:              true,
			expectedErrorMessage: "invalid order path",
		},
		{
			name:                 "mixed authorized and unauthorized fields",
			orderBy:              "id asc, unauthorized_field desc",
			expectedOrderBySQL:   "",
			wantErr:              true,
			expectedErrorMessage: "invalid order path",
		},
		{
			name:               "single field asc (implicit)",
			orderBy:            "id",
			expectedOrderBySQL: "ORDER BY id",
			wantErr:            false,
		},
		{
			name:                 "invalid syntax - wrong direction",
			orderBy:              "id ascending",
			expectedOrderBySQL:   "",
			wantErr:              true,
			expectedErrorMessage: "parsing order by",
		},
		{
			name:                 "invalid syntax - extra comma",
			orderBy:              "id asc,",
			expectedOrderBySQL:   "",
			wantErr:              true,
			expectedErrorMessage: "parsing order by",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				OrderBy: tc.orderBy,
			}

			parsedRequest, err := parser.Parse(request)

			if tc.wantErr {
				require.Error(t, err)
				if tc.expectedErrorMessage != "" {
					require.Contains(t, err.Error(), tc.expectedErrorMessage)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, parsedRequest)

				// Verify SQL order by clause
				orderBySQL := parsedRequest.GetSQLOrderByClause()
				require.Equal(t, tc.expectedOrderBySQL, orderBySQL)
			}
		})
	}
}

func TestOrderingRequestParser_DefaultOrderByInjection(t *testing.T) {
	parser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		initialOrderBy string
		expectedInject bool
	}{
		{
			name:           "empty order_by gets default injected",
			initialOrderBy: "",
			expectedInject: true,
		},
		{
			name:           "explicit order_by not modified",
			initialOrderBy: "id asc",
			expectedInject: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				OrderBy: tc.initialOrderBy,
			}

			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)

			if tc.expectedInject {
				// Should have the default order by
				require.Equal(t, parser.options.Default, request.GetOrderBy())
			} else {
				// Should retain the original order by
				require.Equal(t, tc.initialOrderBy, request.GetOrderBy())
			}

			// SQL should be generated in both cases
			orderBySQL := parsedRequest.GetSQLOrderByClause()
			require.NotEmpty(t, orderBySQL)
		})
	}
}

func TestOrderingRequestParser_SQLTranspilation(t *testing.T) {
	parser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name        string
		orderBy     string
		expectedSQL string
	}{
		{
			name:        "single field ascending omits ASC",
			orderBy:     "id asc",
			expectedSQL: "ORDER BY id",
		},
		{
			name:        "single field descending includes DESC",
			orderBy:     "id desc",
			expectedSQL: "ORDER BY id DESC",
		},
		{
			name:        "multiple fields mixed order",
			orderBy:     "create_timestamp desc, id asc",
			expectedSQL: "ORDER BY create_timestamp DESC, id",
		},
		{
			name:        "all ascending omits ASC",
			orderBy:     "id asc, create_timestamp asc",
			expectedSQL: "ORDER BY id, create_timestamp",
		},
		{
			name:        "all descending includes DESC",
			orderBy:     "id desc, create_timestamp desc",
			expectedSQL: "ORDER BY id DESC, create_timestamp DESC",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				OrderBy: tc.orderBy,
			}

			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)

			orderBySQL := parsedRequest.GetSQLOrderByClause()
			require.Equal(t, tc.expectedSQL, orderBySQL)
		})
	}
}

func TestOrderingRequest_GetSQLOrderByClause(t *testing.T) {
	parser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name         string
		orderBy      string
		verifyClause func(t *testing.T, clause string)
	}{
		{
			name:    "clause starts with ORDER BY",
			orderBy: "id asc",
			verifyClause: func(t *testing.T, clause string) {
				require.Contains(t, clause, "ORDER BY")
			},
		},
		{
			name:    "clause contains field name",
			orderBy: "create_timestamp desc",
			verifyClause: func(t *testing.T, clause string) {
				require.Contains(t, clause, "create_timestamp")
			},
		},
		{
			name:    "clause contains DESC for descending",
			orderBy: "id desc",
			verifyClause: func(t *testing.T, clause string) {
				require.Contains(t, clause, "DESC")
			},
		},
		{
			name:    "clause omits ASC for ascending",
			orderBy: "id asc",
			verifyClause: func(t *testing.T, clause string) {
				require.NotContains(t, clause, "ASC")
			},
		},
		{
			name:    "multiple fields separated by comma",
			orderBy: "id asc, create_timestamp desc",
			verifyClause: func(t *testing.T, clause string) {
				require.Contains(t, clause, "id")
				require.Contains(t, clause, "create_timestamp")
				require.Contains(t, clause, ",")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				OrderBy: tc.orderBy,
			}

			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)

			clause := parsedRequest.GetSQLOrderByClause()
			tc.verifyClause(t, clause)
		})
	}
}

func TestOrderingRequestParser_ColumnNameOverride(t *testing.T) {
	parser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name               string
		orderBy            string
		expectedOrderBySQL string
		wantErr            bool
	}{
		{
			name:               "column name override ascending works",
			orderBy:            "column_name_changed asc",
			expectedOrderBySQL: "ORDER BY new_name",
			wantErr:            false,
		},
		{
			name:               "column name override descending",
			orderBy:            "column_name_changed desc",
			expectedOrderBySQL: "ORDER BY new_name DESC",
			wantErr:            false,
		},
		{
			name:               "column name override with other fields",
			orderBy:            "id asc, column_name_changed desc",
			expectedOrderBySQL: "ORDER BY id, new_name DESC",
			wantErr:            false,
		},
		{
			name:               "multiple column name overrides",
			orderBy:            "column_name_changed asc, column_name_changed desc",
			expectedOrderBySQL: "ORDER BY new_name, new_name DESC",
			wantErr:            false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				OrderBy: tc.orderBy,
			}

			parsedRequest, err := parser.Parse(request)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, parsedRequest)

				orderBySQL := parsedRequest.GetSQLOrderByClause()
				require.Equal(t, tc.expectedOrderBySQL, orderBySQL)
			}
		})
	}
}

func TestOrderingRequestParser_NameExpansion(t *testing.T) {
	parser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name               string
		orderBy            string
		expectedOrderBySQL string
		wantErr            bool
		expectedErrMsg     string
	}{
		{
			name:               "name ascending expands to composite keys",
			orderBy:            "name asc",
			expectedOrderBySQL: "ORDER BY organization_id, user_id, resource_id",
		},
		{
			name:               "name descending expands to composite keys",
			orderBy:            "name desc",
			expectedOrderBySQL: "ORDER BY organization_id DESC, user_id DESC, resource_id DESC",
		},
		{
			name:               "name implicit ascending",
			orderBy:            "name",
			expectedOrderBySQL: "ORDER BY organization_id, user_id, resource_id",
		},
		{
			name:               "name with other fields before",
			orderBy:            "create_timestamp desc, name asc",
			expectedOrderBySQL: "ORDER BY create_timestamp DESC, organization_id, user_id, resource_id",
		},
		{
			name:               "name with other fields after",
			orderBy:            "name desc, id asc",
			expectedOrderBySQL: "ORDER BY organization_id DESC, user_id DESC, resource_id DESC, id",
		},
		{
			name:               "name between other fields",
			orderBy:            "id asc, name desc, create_timestamp asc",
			expectedOrderBySQL: "ORDER BY id, organization_id DESC, user_id DESC, resource_id DESC, create_timestamp",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				OrderBy: tc.orderBy,
			}

			parsedRequest, err := parser.Parse(request)

			if tc.wantErr {
				require.Error(t, err)
				if tc.expectedErrMsg != "" {
					require.Contains(t, err.Error(), tc.expectedErrMsg)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, parsedRequest)

			orderBySQL := parsedRequest.GetSQLOrderByClause()
			require.Equal(t, tc.expectedOrderBySQL, orderBySQL)
		})
	}
}

/*
func TestOrderingRequestParser_NameNotAllowed(t *testing.T) {
	// This test requires a parser where "name" is NOT in the allowed paths.
	// You may need to create a separate test proto/request type for this,
	// or skip if the test proto always allows "name".
	parser, err := NewOrderingRequestParser[*pb.ListResourcesWithoutNameOrderingRequest, *pb.Resource]()
	require.NoError(t, err)

	request := &pb.ListResourcesWithoutNameOrderingRequest{
		OrderBy: "name asc",
	}

	_, err = parser.Parse(request)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ordering by path name not allowed")
}
*/

func TestOrderingRequestParser_GetCompositeKeyFields(t *testing.T) {
	parser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	// Access the composite key fields through the expansion
	// by checking the resulting SQL
	request := &pb.ListResourcesRequest{
		OrderBy: "name",
	}

	parsedRequest, err := parser.Parse(request)
	require.NoError(t, err)

	sql := parsedRequest.GetSQLOrderByClause()

	// Verify all composite key fields are present in order
	require.Contains(t, sql, "organization_id")
	require.Contains(t, sql, "user_id")
	require.Contains(t, sql, "resource_id")

	// Verify the order is correct (organization before user before resource)
	orgIdx := strings.Index(sql, "organization_id")
	userIdx := strings.Index(sql, "user_id")
	resourceIdx := strings.Index(sql, "resource_id")

	require.Less(t, orgIdx, userIdx, "organization_id should come before user_id")
	require.Less(t, userIdx, resourceIdx, "user_id should come before resource_id")
}

func TestOrderingRequestParser_NameExpansionPreservesDirection(t *testing.T) {
	parser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name     string
		orderBy  string
		wantDesc bool
	}{
		{
			name:     "ascending direction preserved",
			orderBy:  "name asc",
			wantDesc: false,
		},
		{
			name:     "descending direction preserved",
			orderBy:  "name desc",
			wantDesc: true,
		},
		{
			name:     "implicit ascending",
			orderBy:  "name",
			wantDesc: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				OrderBy: tc.orderBy,
			}

			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)

			sql := parsedRequest.GetSQLOrderByClause()

			if tc.wantDesc {
				// All expanded fields should have DESC
				require.Contains(t, sql, "organization_id DESC")
				require.Contains(t, sql, "user_id DESC")
				require.Contains(t, sql, "resource_id DESC")
			} else {
				// No DESC should appear (ASC is implicit and omitted)
				require.NotContains(t, sql, "DESC")
			}
		})
	}
}

func TestOrderingRequestParser_NameWithColumnOverride(t *testing.T) {
	parser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	// Test that name expansion works alongside column name overrides
	request := &pb.ListResourcesRequest{
		OrderBy: "name asc, column_name_changed desc",
	}

	parsedRequest, err := parser.Parse(request)
	require.NoError(t, err)

	sql := parsedRequest.GetSQLOrderByClause()

	// Verify name expansion happened
	require.Contains(t, sql, "organization_id")
	require.Contains(t, sql, "user_id")
	require.Contains(t, sql, "resource_id")

	// Verify column override also applied
	require.Contains(t, sql, "new_name DESC")
}

func TestOrderingRequestParser_CustomIdColumnName(t *testing.T) {
	// Test with Book resource which has id_column_name: "id"
	parser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Book]()
	require.NoError(t, err)

	tests := []struct {
		name               string
		orderBy            string
		expectedOrderBySQL string
		wantErr            bool
	}{
		{
			name:               "name ascending with custom id column",
			orderBy:            "name asc",
			expectedOrderBySQL: "ORDER BY id",
		},
		{
			name:               "name descending with custom id column",
			orderBy:            "name desc",
			expectedOrderBySQL: "ORDER BY id DESC",
		},
		{
			name:               "name implicit ascending with custom id column",
			orderBy:            "name",
			expectedOrderBySQL: "ORDER BY id",
		},
		{
			name:               "name with other fields",
			orderBy:            "name desc, create_time asc",
			expectedOrderBySQL: "ORDER BY id DESC, create_time",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				OrderBy: tc.orderBy,
			}

			parsedRequest, err := parser.Parse(request)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, parsedRequest)

			orderBySQL := parsedRequest.GetSQLOrderByClause()
			require.Equal(t, tc.expectedOrderBySQL, orderBySQL)
		})
	}
}

func TestOrderingRequestParser_CustomIdColumnNameVsStandard(t *testing.T) {
	// Compare Book (custom id) vs Resource (standard id)

	// Book uses id_column_name: "id", pattern: "books/{book}"
	bookParser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Book]()
	require.NoError(t, err)

	// Resource uses standard pattern: "organizations/{organization}/users/{user}/resources/{resource}"
	resourceParser, err := NewOrderingRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	// Book should expand "name" to just "id"
	bookRequest := &pb.ListResourcesRequest{OrderBy: "name"}
	bookParsed, err := bookParser.Parse(bookRequest)
	require.NoError(t, err)
	require.Equal(t, "ORDER BY id", bookParsed.GetSQLOrderByClause())

	// Resource should expand "name" to "organization_id, user_id, resource_id"
	resourceRequest := &pb.ListResourcesRequest{OrderBy: "name"}
	resourceParsed, err := resourceParser.Parse(resourceRequest)
	require.NoError(t, err)
	require.Equal(t, "ORDER BY organization_id, user_id, resource_id", resourceParsed.GetSQLOrderByClause())
}

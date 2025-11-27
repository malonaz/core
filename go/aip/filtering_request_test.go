package aip

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/malonaz/core/genproto/test/aip"
)

func escapeDollar(s string) string {
	return strings.ReplaceAll(s, "$", "@")
}

func TestFilteringRequestParser_NewParser(t *testing.T) {
	tests := []struct {
		name           string
		createParser   func() (*FilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource], error)
		wantErr        bool
		expectedFields int
	}{
		{
			name: "valid parser creation",
			createParser: func() (*FilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource], error) {
				return NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
			},
			wantErr:        false,
			expectedFields: 9, // Number of filters defined in the proto
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
				require.NotNil(t, parser.validator)
				require.NotNil(t, parser.declarations)
			}
		})
	}
}

func TestFilteringRequestParser_Parse(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name                 string
		filter               string
		expectedClause       string
		expectedParams       []any
		wantErr              bool
		expectedErrorMessage string
	}{
		// Basic field filters
		{
			name:           "filter by string field",
			filter:         `id="testUser"`,
			expectedClause: "WHERE (id = $1)",
			expectedParams: []any{"testUser"},
			wantErr:        false,
		},
		{
			name:           "filter by integer field",
			filter:         `create_timestamp > 1609459200000000`,
			expectedClause: "WHERE (create_timestamp > $1)",
			expectedParams: []any{int64(1609459200000000)},
			wantErr:        false,
		},
		{
			name:           "filter by boolean field",
			filter:         `deleted`,
			expectedClause: "WHERE deleted",
			expectedParams: []any{},
			wantErr:        false,
		},
		{
			name:           "filter by enum field",
			filter:         `my_enum = MY_ENUM_VALUE`,
			expectedClause: "WHERE (my_enum = $1)",
			expectedParams: []any{int64(1)},
			wantErr:        false,
		},

		// Comparison operators
		{
			name:           "greater than comparison",
			filter:         `create_timestamp > 1000`,
			expectedClause: "WHERE (create_timestamp > $1)",
			expectedParams: []any{int64(1000)},
			wantErr:        false,
		},
		{
			name:           "less than comparison",
			filter:         `create_timestamp < 2000`,
			expectedClause: "WHERE (create_timestamp < $1)",
			expectedParams: []any{int64(2000)},
			wantErr:        false,
		},
		{
			name:           "greater than or equal comparison",
			filter:         `create_timestamp >= 1500`,
			expectedClause: "WHERE (create_timestamp >= $1)",
			expectedParams: []any{int64(1500)},
			wantErr:        false,
		},
		{
			name:           "less than or equal comparison",
			filter:         `create_timestamp <= 1800`,
			expectedClause: "WHERE (create_timestamp <= $1)",
			expectedParams: []any{int64(1800)},
			wantErr:        false,
		},
		{
			name:           "not equal comparison",
			filter:         `id != "excluded"`,
			expectedClause: "WHERE (id != $1)",
			expectedParams: []any{"excluded"},
			wantErr:        false,
		},

		// Logical operators
		{
			name:           "AND logical operator",
			filter:         `id="user1" AND deleted`,
			expectedClause: "WHERE ((id = $1) AND deleted)",
			expectedParams: []any{"user1"},
			wantErr:        false,
		},
		{
			name:           "OR logical operator",
			filter:         `id="user1" OR id="user2"`,
			expectedClause: "WHERE ((id = $1) OR (id = $2))",
			expectedParams: []any{"user1", "user2"},
			wantErr:        false,
		},
		{
			name:           "NOT logical operator",
			filter:         `NOT deleted`,
			expectedClause: "WHERE (NOT deleted)",
			expectedParams: []any{},
			wantErr:        false,
		},
		{
			name:           "complex logical expression",
			filter:         `(id="user1" OR id="user2") AND NOT deleted`,
			expectedClause: "WHERE (((id = $1) OR (id = $2)) AND (NOT deleted))",
			expectedParams: []any{"user1", "user2"},
			wantErr:        false,
		},

		// ISNULL function
		{
			name:           "ISNULL on string field",
			filter:         `ISNULL(id)`,
			expectedClause: "WHERE (id IS NULL)",
			expectedParams: []any{},
			wantErr:        false,
		},
		{
			name:           "ISNULL on integer field",
			filter:         `ISNULL(create_timestamp)`,
			expectedClause: "WHERE (create_timestamp IS NULL)",
			expectedParams: []any{},
			wantErr:        false,
		},
		{
			name:           "ISNULL on boolean field",
			filter:         `ISNULL(deleted)`,
			expectedClause: "WHERE (deleted IS NULL)",
			expectedParams: []any{},
			wantErr:        false,
		},
		{
			name:           "ISNULL on enum field",
			filter:         `ISNULL(my_enum)`,
			expectedClause: "WHERE (my_enum IS NULL)",
			expectedParams: []any{},
			wantErr:        false,
		},
		{
			name:           "NOT ISNULL",
			filter:         `NOT ISNULL(id)`,
			expectedClause: "WHERE (NOT (id IS NULL))",
			expectedParams: []any{},
			wantErr:        false,
		},

		// JSONB function
		{
			name:           "JSONB single key",
			filter:         `nested.field2 = 24`,
			expectedClause: "WHERE ((nested->>'field2') = $1)",
			expectedParams: []any{int64(24)},
			wantErr:        false,
		},
		{
			name:           "JSONB nested keys",
			filter:         `nested.further_nested.field3 = 'value'`,
			expectedClause: "WHERE ((nested->'further_nested'->>'field3') = $1)",
			expectedParams: []any{"value"},
			wantErr:        false,
		},
		{
			name:           "JSONB with comparison operators",
			filter:         `nested.field2 > 3`,
			expectedClause: "WHERE ((nested->>'field2') > $1)",
			expectedParams: []any{int64(3)},
			wantErr:        false,
		},

		// Empty filter
		{
			name:           "empty filter",
			filter:         "",
			expectedClause: "",
			expectedParams: nil,
			wantErr:        false,
		},

		// Complex filters
		{
			name:           "complex filter with multiple conditions",
			filter:         `id="testUser" AND create_timestamp > 1609459200000000 AND NOT deleted`,
			expectedClause: "WHERE (((id = $1) AND (create_timestamp > $2)) AND (NOT deleted))",
			expectedParams: []any{"testUser", int64(1609459200000000)},
			wantErr:        false,
		},
		{
			name:           "complex filter with OR and AND",
			filter:         `(id="user1" OR id="user2") AND create_timestamp > 1000 AND NOT deleted`,
			expectedClause: "WHERE ((((id = $1) OR (id = $2)) AND (create_timestamp > $3)) AND (NOT deleted))",
			expectedParams: []any{"user1", "user2", int64(1000)},
			wantErr:        false,
		},
		{
			name:           "complex filter with ISNULL and JSONB",
			filter:         `NOT ISNULL(id) AND nested.field3 = 'value'`,
			expectedClause: "WHERE ((NOT (id IS NULL)) AND ((nested->>'field3') = $1))",
			expectedParams: []any{"value"},
			wantErr:        false,
		},

		// Error cases
		{
			name:                 "invalid field",
			filter:               `invalid_field = "value"`,
			expectedClause:       "",
			expectedParams:       nil,
			wantErr:              true,
			expectedErrorMessage: "parsing filter",
		},
		{
			name:                 "invalid syntax - missing value",
			filter:               `id =`,
			expectedClause:       "",
			expectedParams:       nil,
			wantErr:              true,
			expectedErrorMessage: "parsing filter",
		},
		{
			name:                 "invalid syntax - unbalanced parentheses",
			filter:               `(id = "user1"`,
			expectedClause:       "",
			expectedParams:       nil,
			wantErr:              true,
			expectedErrorMessage: "parsing filter",
		},
		{
			name:                 "invalid enum value",
			filter:               `my_enum = INVALID_ENUM_VALUE`,
			expectedClause:       "",
			expectedParams:       nil,
			wantErr:              true,
			expectedErrorMessage: "parsing filter",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				Filter: tc.filter,
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

				whereClause, whereParams := parsedRequest.GetSQLWhereClause()
				require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
				require.Equal(t, tc.expectedParams, whereParams)
			}
		})
	}
}

func TestFilteringRequestParser_TypeValidation(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name    string
		filter  string
		wantErr bool
	}{
		{
			name:    "valid string comparison",
			filter:  `id = "test"`,
			wantErr: false,
		},
		{
			name:    "valid integer comparison",
			filter:  `create_timestamp > 1000`,
			wantErr: false,
		},
		{
			name:    "valid boolean comparison",
			filter:  `deleted`,
			wantErr: false,
		},
		{
			name:    "valid enum comparison",
			filter:  `my_enum = MY_ENUM_VALUE`,
			wantErr: false,
		},
		{
			name:    "type mismatch - string to int",
			filter:  `create_timestamp = "not_a_number"`,
			wantErr: true,
		},
		{
			name:    "type mismatch - int to string",
			filter:  `id = 123`,
			wantErr: true,
		},
		{
			name:    "type mismatch - boolean to string",
			filter:  `deleted = "true"`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				Filter: tc.filter,
			}

			_, err := parser.Parse(request)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFilteringRequestParser_SpecialFunctions(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
	}{
		{
			name:           "ISNULL on string",
			filter:         `ISNULL(id)`,
			expectedClause: "WHERE (id IS NULL)",
			expectedParams: []any{},
		},
		{
			name:           "ISNULL on integer",
			filter:         `ISNULL(create_timestamp)`,
			expectedClause: "WHERE (create_timestamp IS NULL)",
			expectedParams: []any{},
		},
		{
			name:           "ISNULL on boolean",
			filter:         `ISNULL(deleted)`,
			expectedClause: "WHERE (deleted IS NULL)",
			expectedParams: []any{},
		},
		{
			name:           "ISNULL on enum",
			filter:         `ISNULL(my_enum)`,
			expectedClause: "WHERE (my_enum IS NULL)",
			expectedParams: []any{},
		},
		{
			name:           "JSONB single level",
			filter:         `nested.field3 = 'value'`,
			expectedClause: "WHERE ((nested->>'field3') = $1)",
			expectedParams: []any{"value"},
		},
		{
			name:           "JSONB double nested",
			filter:         `nested.further_nested.field2 = 2`,
			expectedClause: "WHERE ((nested->'further_nested'->>'field2') = $1)",
			expectedParams: []any{int64(2)},
		},
		{
			name:           "combined ISNULL and JSONB",
			filter:         `NOT ISNULL(id) AND nested.field3 = 'test'`,
			expectedClause: "WHERE ((NOT (id IS NULL)) AND ((nested->>'field3') = $1))",
			expectedParams: []any{"test"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{
				Filter: tc.filter,
			}

			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)

			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

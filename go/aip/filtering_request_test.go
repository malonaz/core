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
			name:    "ISNULL on non-nullable enum field",
			filter:  `ISNULL(my_enum)`,
			wantErr: true,
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
			expectedClause: "WHERE ((nested->>'field2')::bigint = @1)",
			expectedParams: []any{int64(24)},
			wantErr:        false,
		},
		{
			name:           "JSONB nested keys",
			filter:         `nested.further_nested.field3 = 'value'`,
			expectedClause: "WHERE (nested->'further_nested'->>'field3' = $1)",
			expectedParams: []any{"value"},
			wantErr:        false,
		},
		{
			name:           "JSONB with comparison operators",
			filter:         `nested.field2 > 3`,
			expectedClause: "WHERE ((nested->>'field2')::bigint > @1)",
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
			expectedClause: "WHERE ((NOT (id IS NULL)) AND (nested->>'field3' = $1))",
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

		// Column name change.
		{
			name:           "filter by nested field with column name override",
			filter:         `nested_changed.field2 = 42`,
			expectedClause: "WHERE ((nested_new_name->>'field2')::bigint = $1)",
			expectedParams: []any{int64(42)},
			wantErr:        false,
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
			filter:         `ISNULL(nullable_enum)`,
			expectedClause: "WHERE (nullable_enum IS NULL)",
			expectedParams: []any{},
		},
		{
			name:           "JSONB single level",
			filter:         `nested.field3 = 'value'`,
			expectedClause: "WHERE (nested->>'field3' = $1)",
			expectedParams: []any{"value"},
		},
		{
			name:           "JSONB double nested",
			filter:         `nested.further_nested.field2 = 2`,
			expectedClause: "WHERE ((nested->'further_nested'->>'field2')::bigint = @1)",
			expectedParams: []any{int64(2)},
		},
		{
			name:           "combined ISNULL and JSONB",
			filter:         `NOT ISNULL(id) AND nested.field3 = 'test'`,
			expectedClause: "WHERE ((NOT (id IS NULL)) AND (nested->>'field3' = $1))",
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

func TestApplyReplacement(t *testing.T) {
	tests := []struct {
		name        string
		clause      string
		field       string
		replacement string
		expected    string
	}{
		// Basic replacements without JSONB
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

		// Critical case: preventing partial matches
		{
			name:        "short field doesn't match longer path",
			clause:      "hello.my.path = 1",
			field:       "hello",
			replacement: "h",
			expected:    "hello.my.path = 1", // NO replacement - followed by dot
		},
		{
			name:        "medium field doesn't match longer path",
			clause:      "hello.my.path = 1",
			field:       "hello.my",
			replacement: "hello@my",
			expected:    "hello.my.path = 1", // NO replacement - followed by dot
		},

		// Multiple different fields in one clause
		{
			name:        "only replaces exact field not substrings",
			clause:      "hello.my.path = 1 AND hello.my = 2 AND hello = 3",
			field:       "hello",
			replacement: "h",
			expected:    "hello.my.path = 1 AND hello.my = 2 AND h = 3",
		},

		// Word boundary checks
		{
			name:        "field part of larger word - prefix",
			clause:      "user_id = 123",
			field:       "id",
			replacement: "i",
			expected:    "user_id = 123", // NO replacement - 'id' is part of 'user_id'
		},
		{
			name:        "field part of larger word - suffix",
			clause:      "discount = 10",
			field:       "count",
			replacement: "cnt",
			expected:    "discount = 10", // NO replacement - 'count' is part of 'discount'
		},
		{
			name:        "field part of larger word - middle",
			clause:      "account_name = 'test'",
			field:       "count",
			replacement: "cnt",
			expected:    "account_name = 'test'", // NO replacement
		},

		// Complex expressions
		{
			name:        "field in parentheses",
			clause:      "(count > 5 OR count < 2) AND total = 10",
			field:       "count",
			replacement: "cnt",
			expected:    "(cnt > 5 OR cnt < 2) AND total = 10",
		},
		{
			name:        "field with no spaces",
			clause:      "count>10AND count<=20",
			field:       "count",
			replacement: "cnt",
			expected:    "cnt>10AND cnt<=20",
		},

		// Edge cases
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
		{
			name:        "field at start of clause",
			clause:      "count = 5",
			field:       "count",
			replacement: "cnt",
			expected:    "cnt = 5",
		},
		{
			name:        "field at end of clause",
			clause:      "value = count",
			field:       "count",
			replacement: "cnt",
			expected:    "value = cnt",
		},

		// Root field with column name override (no @)
		{
			name:        "root field with column override",
			clause:      "name = 'John'",
			field:       "name",
			replacement: "id",
			expected:    "id = 'John'", // No JSONB wrapping - no @ present
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

func TestFilteringRequestParser_WildcardPaths(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest2, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		// Root field explicitly allowed
		{
			name:           "explicitly allowed root field",
			filter:         `id = "test"`,
			expectedClause: "WHERE (id = $1)",
			expectedParams: []any{"test"},
			wantErr:        false,
		},

		// First level wildcard: nested.*
		{
			name:           "wildcard allows nested.field1",
			filter:         `nested.field1`,
			expectedClause: "WHERE (nested->>'field1')::boolean",
			expectedParams: []any{},
			wantErr:        false,
		},
		{
			name:           "wildcard allows nested.field2",
			filter:         `nested.field2 = 42`,
			expectedClause: "WHERE ((nested->>'field2')::bigint = $1)",
			expectedParams: []any{int64(42)},
			wantErr:        false,
		},
		{
			name:           "wildcard allows nested.field3",
			filter:         `nested.field3 = "value"`,
			expectedClause: "WHERE (nested->>'field3' = $1)",
			expectedParams: []any{"value"},
			wantErr:        false,
		},

		// Second level wildcard: nested2.further_nested.*
		{
			name:           "second level wildcard allows field1",
			filter:         `NOT nested2.further_nested.field1`,
			expectedClause: "WHERE (NOT (nested2->'further_nested'->>'field1')::boolean)",
			expectedParams: []any{},
			wantErr:        false,
		},
		{
			name:           "second level wildcard allows field2",
			filter:         `nested2.further_nested.field2 = 99`,
			expectedClause: "WHERE ((nested2->'further_nested'->>'field2')::bigint = $1)",
			expectedParams: []any{int64(99)},
			wantErr:        false,
		},
		{
			name:           "second level wildcard allows field3",
			filter:         `nested2.further_nested.field3 = "test"`,
			expectedClause: "WHERE (nested2->'further_nested'->>'field3' = $1)",
			expectedParams: []any{"test"},
			wantErr:        false,
		},

		// Fields not covered by wildcards should fail
		{
			name:    "non-whitelisted root field fails",
			filter:  `create_timestamp > 1000`,
			wantErr: true,
		},
		{
			name:    "non-whitelisted nested field fails",
			filter:  `nested3.field1 = true`,
			wantErr: true,
		},
		{
			name:    "wildcard doesn't cover parent",
			filter:  `nested2.field1 = true`,
			wantErr: true,
		},

		// Complex filters with wildcards
		{
			name:           "multiple wildcard fields combined",
			filter:         `id = "test" AND nested.field1 AND nested2.further_nested.field2 = 5`,
			expectedClause: "WHERE (((id = @1) AND (nested->>'field1')::boolean) AND ((nested2->'further_nested'->>'field2')::bigint = @2))",
			expectedParams: []any{"test", int64(5)},
			wantErr:        false,
		},
		{
			name:           "OR with wildcard fields",
			filter:         `nested.field2 = 1 OR nested.field2 = 2`,
			expectedClause: "WHERE (((nested->>'field2')::bigint = @1) OR ((nested->>'field2')::bigint = @2))",
			expectedParams: []any{int64(1), int64(2)},
			wantErr:        false,
		},
		{
			name:           "ISNULL on wildcard field",
			filter:         `ISNULL(nested.field3)`,
			expectedClause: "WHERE (nested->>'field3' IS NULL)",
			expectedParams: []any{},
			wantErr:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest2{
				Filter: tc.filter,
			}

			parsedRequest, err := parser.Parse(request)

			if tc.wantErr {
				require.Error(t, err)
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

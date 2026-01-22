package aip

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/malonaz/core/genproto/test/aip"
)

func escapeDollar(s string) string {
	return strings.ReplaceAll(s, "$", "@")
}

func TestFilteringRequestParser_NewParser(t *testing.T) {
	tests := []struct {
		name         string
		createParser func() (*FilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource], error)
		wantErr      bool
	}{
		{
			name: "valid parser creation",
			createParser: func() (*FilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource], error) {
				return NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
			},
			wantErr: false,
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

func TestFilteringRequestParser_BasicFieldFilters(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "string field equality",
			filter:         `id = "testUser"`,
			expectedClause: "WHERE (id = $1)",
			expectedParams: []any{"testUser"},
		},
		{
			name:           "string field with special characters",
			filter:         `id = "user@example.com"`,
			expectedClause: "WHERE (id = $1)",
			expectedParams: []any{"user@example.com"},
		},
		{
			name:           "boolean field bare identifier (true)",
			filter:         `deleted`,
			expectedClause: "WHERE deleted",
			expectedParams: []any{},
		},
		{
			name:           "boolean field explicit true",
			filter:         `deleted = true`,
			expectedClause: "WHERE (deleted = true)",
			expectedParams: []any{},
		},
		{
			name:           "boolean field explicit false",
			filter:         `deleted = false`,
			expectedClause: "WHERE (deleted = false)",
			expectedParams: []any{},
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
			request := &pb.ListResourcesRequest{Filter: tc.filter}
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
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "string equality",
			filter:         `id = "test"`,
			expectedClause: "WHERE (id = $1)",
			expectedParams: []any{"test"},
		},
		{
			name:           "string not equal",
			filter:         `id != "excluded"`,
			expectedClause: "WHERE (id != $1)",
			expectedParams: []any{"excluded"},
		},
		{
			name:           "integer greater than",
			filter:         `column_name_changed > 1000`,
			expectedClause: "WHERE (new_name > $1)",
			expectedParams: []any{int64(1000)},
		},
		{
			name:           "integer less than",
			filter:         `column_name_changed < 2000`,
			expectedClause: "WHERE (new_name < $1)",
			expectedParams: []any{int64(2000)},
		},
		{
			name:           "integer greater than or equal",
			filter:         `column_name_changed >= 1500`,
			expectedClause: "WHERE (new_name >= $1)",
			expectedParams: []any{int64(1500)},
		},
		{
			name:           "integer less than or equal",
			filter:         `column_name_changed <= 1800`,
			expectedClause: "WHERE (new_name <= $1)",
			expectedParams: []any{int64(1800)},
		},
		{
			name:           "integer equality",
			filter:         `column_name_changed = 100`,
			expectedClause: "WHERE (new_name = $1)",
			expectedParams: []any{int64(100)},
		},
		{
			name:           "integer not equal",
			filter:         `column_name_changed != 999`,
			expectedClause: "WHERE (new_name != $1)",
			expectedParams: []any{int64(999)},
		},
		{
			name:           "string greater than (lexical)",
			filter:         `id > "abc"`,
			expectedClause: "WHERE (id > $1)",
			expectedParams: []any{"abc"},
		},
		{
			name:           "string less than (lexical)",
			filter:         `id < "xyz"`,
			expectedClause: "WHERE (id < $1)",
			expectedParams: []any{"xyz"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
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
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "AND with two conditions",
			filter:         `id = "user1" AND deleted`,
			expectedClause: "WHERE ((id = $1) AND deleted)",
			expectedParams: []any{"user1"},
		},
		{
			name:           "AND with three conditions",
			filter:         `id = "user1" AND deleted AND field1 = "test"`,
			expectedClause: "WHERE (((id = $1) AND deleted) AND (field1 = $2))",
			expectedParams: []any{"user1", "test"},
		},
		{
			name:           "OR with two conditions",
			filter:         `id = "user1" OR id = "user2"`,
			expectedClause: "WHERE ((id = $1) OR (id = $2))",
			expectedParams: []any{"user1", "user2"},
		},
		{
			name:           "OR with three conditions",
			filter:         `id = "a" OR id = "b" OR id = "c"`,
			expectedClause: "WHERE (((id = $1) OR (id = $2)) OR (id = $3))",
			expectedParams: []any{"a", "b", "c"},
		},
		{
			name:           "NOT with boolean",
			filter:         `NOT deleted`,
			expectedClause: "WHERE (NOT deleted)",
			expectedParams: []any{},
		},
		{
			name:           "NOT with comparison",
			filter:         `NOT id = "excluded"`,
			expectedClause: "WHERE (NOT (id = $1))",
			expectedParams: []any{"excluded"},
		},
		{
			name:           "NOT with parentheses",
			filter:         `NOT (id = "a" OR id = "b")`,
			expectedClause: "WHERE (NOT ((id = $1) OR (id = $2)))",
			expectedParams: []any{"a", "b"},
		},
		{
			name:           "minus operator with boolean",
			filter:         `-deleted`,
			expectedClause: "WHERE (NOT deleted)",
			expectedParams: []any{},
		},
		{
			name:           "minus operator with comparison",
			filter:         `-id = "excluded"`,
			expectedClause: "WHERE (NOT (id = $1))",
			expectedParams: []any{"excluded"},
		},
		{
			name:           "AND and OR combined with parentheses",
			filter:         `(id = "user1" OR id = "user2") AND NOT deleted`,
			expectedClause: "WHERE (((id = $1) OR (id = $2)) AND (NOT deleted))",
			expectedParams: []any{"user1", "user2"},
		},
		{
			name:           "nested NOT with parentheses",
			filter:         `NOT (NOT deleted)`,
			expectedClause: "WHERE (NOT (NOT deleted))",
			expectedParams: []any{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
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
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
	}{
		{
			name:           "OR has higher precedence than AND",
			filter:         `id = "a" AND id = "b" OR id = "c"`,
			expectedClause: "WHERE ((id = $1) AND ((id = $2) OR (id = $3)))",
			expectedParams: []any{"a", "b", "c"},
		},
		{
			name:           "explicit parentheses override precedence",
			filter:         `(id = "a" AND id = "b") OR id = "c"`,
			expectedClause: "WHERE (((id = $1) AND (id = $2)) OR (id = $3))",
			expectedParams: []any{"a", "b", "c"},
		},
		{
			name:           "multiple OR groups with AND",
			filter:         `id = "a" AND id = "b" OR id = "c" AND id = "d" OR id = "e"`,
			expectedClause: "WHERE (((id = $1) AND ((id = $2) OR (id = $3))) AND ((id = $4) OR (id = $5)))",
			expectedParams: []any{"a", "b", "c", "d", "e"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

func TestFilteringRequestParser_TraversalOperator(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "nested string field",
			filter:         `nested.field3 = "value"`,
			expectedClause: "WHERE (nested->>'field3' = $1)",
			expectedParams: []any{"value"},
		},
		{
			name:           "nested integer field",
			filter:         `nested.field2 = 42`,
			expectedClause: "WHERE ((nested->>'field2')::bigint = $1)",
			expectedParams: []any{int64(42)},
		},
		{
			name:           "nested boolean field bare",
			filter:         `nested.field1`,
			expectedClause: "WHERE (nested->>'field1')::boolean",
			expectedParams: []any{},
		},
		{
			name:           "nested boolean field explicit",
			filter:         `nested.field1 = true`,
			expectedClause: "WHERE ((nested->>'field1')::boolean = true)",
			expectedParams: []any{},
		},
		{
			name:           "deeply nested string field",
			filter:         `nested.further_nested.field3 = "deep"`,
			expectedClause: "WHERE (nested->'further_nested'->>'field3' = $1)",
			expectedParams: []any{"deep"},
		},
		{
			name:           "deeply nested integer field",
			filter:         `nested.further_nested.field2 = 99`,
			expectedClause: "WHERE ((nested->'further_nested'->>'field2')::bigint = $1)",
			expectedParams: []any{int64(99)},
		},
		{
			name:           "deeply nested boolean field",
			filter:         `nested.further_nested.field1`,
			expectedClause: "WHERE (nested->'further_nested'->>'field1')::boolean",
			expectedParams: []any{},
		},
		{
			name:           "nested integer greater than",
			filter:         `nested.field2 > 10`,
			expectedClause: "WHERE ((nested->>'field2')::bigint > $1)",
			expectedParams: []any{int64(10)},
		},
		{
			name:           "nested integer less than or equal",
			filter:         `nested.further_nested.field2 <= 100`,
			expectedClause: "WHERE ((nested->'further_nested'->>'field2')::bigint <= $1)",
			expectedParams: []any{int64(100)},
		},
		{
			name:    "undefined nested field",
			filter:  `nested.undefined_field = "test"`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
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

func TestFilteringRequestParser_HasOperator(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "string field is present",
			filter:         `id:*`,
			expectedClause: "WHERE (id IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:           "string field is not present (null check)",
			filter:         `NOT id:*`,
			expectedClause: "WHERE (NOT (id IS NOT NULL))",
			expectedParams: []any{},
		},
		{
			name:           "boolean field is present",
			filter:         `deleted:*`,
			expectedClause: "WHERE (deleted IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:           "integer field is present",
			filter:         `column_name_changed:*`,
			expectedClause: "WHERE (new_name IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:           "enum field is present",
			filter:         `my_enum:*`,
			expectedClause: "WHERE (my_enum IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:           "nested message field is present",
			filter:         `nested:*`,
			expectedClause: "WHERE (nested IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:           "nested message field is not present",
			filter:         `-nested:*`,
			expectedClause: "WHERE (NOT (nested IS NOT NULL))",
			expectedParams: []any{},
		},
		{
			name:           "repeated string contains value",
			filter:         `tags:"important"`,
			expectedClause: "WHERE ($1 = ANY(tags))",
			expectedParams: []any{"important"},
		},
		{
			name:           "repeated string contains value with NOT",
			filter:         `NOT tags:"spam"`,
			expectedClause: "WHERE (NOT ($1 = ANY(tags)))",
			expectedParams: []any{"spam"},
		},
		{
			name:           "repeated field is present",
			filter:         `tags:*`,
			expectedClause: "WHERE (tags IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:           "repeated message nested string field",
			filter:         `items.field3:"test"`,
			expectedClause: "WHERE (EXISTS(SELECT 1 FROM jsonb_array_elements(items) AS _elem WHERE _elem->>'field3' = $1))",
			expectedParams: []any{"test"},
		},
		{
			name:           "repeated message deeply nested field",
			filter:         `items.further_nested.field3:"deep"`,
			expectedClause: "WHERE (EXISTS(SELECT 1 FROM jsonb_array_elements(items) AS _elem WHERE _elem->'further_nested'->>'field3' = $1))",
			expectedParams: []any{"deep"},
		},
		{
			name:           "map contains key",
			filter:         `labels:"environment"`,
			expectedClause: "WHERE (labels ? $1)",
			expectedParams: []any{"environment"},
		},
		{
			name:           "map does not contain key",
			filter:         `NOT labels:"deprecated"`,
			expectedClause: "WHERE (NOT (labels ? $1))",
			expectedParams: []any{"deprecated"},
		},
		{
			name:           "map field is present",
			filter:         `labels:*`,
			expectedClause: "WHERE (labels IS NOT NULL)",
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
			filter:         `tags:"important" AND NOT deleted`,
			expectedClause: "WHERE (($1 = ANY(tags)) AND (NOT deleted))",
			expectedParams: []any{"important"},
		},
		{
			name:           "presence check combined with equality",
			filter:         `id:* AND id = "user1"`,
			expectedClause: "WHERE ((id IS NOT NULL) AND (id = $1))",
			expectedParams: []any{"user1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
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

func TestFilteringRequestParser_WildcardStringMatching(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "string starts with",
			filter:         `id = "user_*"`,
			expectedClause: "WHERE (id LIKE $1)",
			expectedParams: []any{"user_%"},
		},
		{
			name:           "string starts with prefix",
			filter:         `field1 = "prefix*"`,
			expectedClause: "WHERE (field1 LIKE $1)",
			expectedParams: []any{"prefix%"},
		},
		{
			name:           "string ends with",
			filter:         `id = "*.example.com"`,
			expectedClause: "WHERE (id LIKE $1)",
			expectedParams: []any{"%.example.com"},
		},
		{
			name:           "string ends with suffix",
			filter:         `field1 = "*_suffix"`,
			expectedClause: "WHERE (field1 LIKE $1)",
			expectedParams: []any{"%_suffix"},
		},
		{
			name:           "string contains",
			filter:         `id = "*middle*"`,
			expectedClause: "WHERE (id LIKE $1)",
			expectedParams: []any{"%middle%"},
		},
		{
			name:           "nested string starts with",
			filter:         `nested.field3 = "test_*"`,
			expectedClause: "WHERE (nested->>'field3' LIKE $1)",
			expectedParams: []any{"test_%"},
		},
		{
			name:           "nested string ends with",
			filter:         `nested.field3 = "*.json"`,
			expectedClause: "WHERE (nested->>'field3' LIKE $1)",
			expectedParams: []any{"%.json"},
		},
		{
			name:           "wildcard in middle only is invalid",
			filter:         `id = "pre*fix"`,
			expectedClause: "WHERE (id = $1)",
			expectedParams: []any{"pre*fix"},
			//wantErr: true,
		},
		{
			name:           "wildcard with AND",
			filter:         `id = "user_*" AND NOT deleted`,
			expectedClause: "WHERE ((id LIKE $1) AND (NOT deleted))",
			expectedParams: []any{"user_%"},
		},
		{
			name:           "wildcard with OR",
			filter:         `id = "admin_*" OR id = "super_*"`,
			expectedClause: "WHERE ((id LIKE $1) OR (id LIKE $2))",
			expectedParams: []any{"admin_%", "super_%"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
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
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "timestamp equals",
			filter:         `create_timestamp = timestamp("2024-01-15T10:30:00Z")`,
			expectedClause: "WHERE (create_timestamp = $1)",
			expectedParams: []any{mustParseTime("2024-01-15T10:30:00Z")},
		},
		{
			name:           "timestamp greater than",
			filter:         `create_timestamp > timestamp("2024-01-01T00:00:00Z")`,
			expectedClause: "WHERE (create_timestamp > $1)",
			expectedParams: []any{mustParseTime("2024-01-01T00:00:00Z")},
		},
		{
			name:           "timestamp less than",
			filter:         `create_timestamp < timestamp("2024-12-31T23:59:59Z")`,
			expectedClause: "WHERE (create_timestamp < $1)",
			expectedParams: []any{mustParseTime("2024-12-31T23:59:59Z")},
		},
		{
			name:           "timestamp greater than or equal",
			filter:         `create_timestamp >= timestamp("2024-06-01T00:00:00Z")`,
			expectedClause: "WHERE (create_timestamp >= $1)",
			expectedParams: []any{mustParseTime("2024-06-01T00:00:00Z")},
		},
		{
			name:           "timestamp less than or equal",
			filter:         `update_time <= timestamp("2024-06-30T23:59:59Z")`,
			expectedClause: "WHERE (update_time <= $1)",
			expectedParams: []any{mustParseTime("2024-06-30T23:59:59Z")},
		},
		{
			name:           "timestamp with positive UTC offset",
			filter:         `create_timestamp > timestamp("2024-01-15T10:30:00+05:30")`,
			expectedClause: "WHERE (create_timestamp > $1)",
			expectedParams: []any{mustParseTime("2024-01-15T10:30:00+05:30")},
		},
		{
			name:           "timestamp with negative UTC offset",
			filter:         `create_timestamp < timestamp("2024-01-15T10:30:00-08:00")`,
			expectedClause: "WHERE (create_timestamp < $1)",
			expectedParams: []any{mustParseTime("2024-01-15T10:30:00-08:00")},
		},
		{
			name:           "timestamp range with AND",
			filter:         `create_timestamp >= timestamp("2024-01-01T00:00:00Z") AND create_timestamp < timestamp("2024-02-01T00:00:00Z")`,
			expectedClause: "WHERE ((create_timestamp >= $1) AND (create_timestamp < $2))",
			expectedParams: []any{mustParseTime("2024-01-01T00:00:00Z"), mustParseTime("2024-02-01T00:00:00Z")},
		},
		{
			name:           "timestamp field is present",
			filter:         `create_timestamp:*`,
			expectedClause: "WHERE (create_timestamp IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:    "invalid timestamp format",
			filter:  `create_timestamp > timestamp("not-a-timestamp")`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
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
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
		wantErr        bool
	}{
		{
			name:           "root enum equality",
			filter:         `my_enum = MY_ENUM_VALUE`,
			expectedClause: "WHERE (my_enum = $1)",
			expectedParams: []any{int64(1)},
		},
		{
			name:           "root enum unspecified",
			filter:         `my_enum = MY_ENUM_UNSPECIFIED`,
			expectedClause: "WHERE (my_enum = $1)",
			expectedParams: []any{int64(0)},
		},
		{
			name:           "root enum not equal",
			filter:         `my_enum != MY_ENUM_VALUE`,
			expectedClause: "WHERE (my_enum != $1)",
			expectedParams: []any{int64(1)},
		},
		{
			name:           "root enum presence check",
			filter:         `my_enum:*`,
			expectedClause: "WHERE (my_enum IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:           "nullable enum equality",
			filter:         `nullable_enum = MY_NULLABLE_ENUM_UNSPECIFIED`,
			expectedClause: "WHERE (nullable_enum = $1)",
			expectedParams: []any{int64(0)},
		},
		{
			name:           "nested enum equality",
			filter:         `nested.state = MY_ENUM_VALUE`,
			expectedClause: "WHERE (nested->>'state' = $1)",
			expectedParams: []any{"MY_ENUM_VALUE"},
		},
		{
			name:           "nested enum unspecified",
			filter:         `nested.state = MY_ENUM_UNSPECIFIED`,
			expectedClause: "WHERE (nested->>'state' = $1)",
			expectedParams: []any{"MY_ENUM_UNSPECIFIED"},
		},
		{
			name:           "nested enum not equal",
			filter:         `nested.state != MY_ENUM_VALUE`,
			expectedClause: "WHERE (nested->>'state' != $1)",
			expectedParams: []any{"MY_ENUM_VALUE"},
		},
		{
			name:    "invalid enum value",
			filter:  `my_enum = INVALID_ENUM_VALUE`,
			wantErr: true,
		},
		{
			name:    "nested invalid enum value",
			filter:  `nested.state = INVALID_ENUM`,
			wantErr: true,
		},
		{
			name:           "enum with AND",
			filter:         `my_enum = MY_ENUM_VALUE AND NOT deleted`,
			expectedClause: "WHERE ((my_enum = $1) AND (NOT deleted))",
			expectedParams: []any{int64(1)},
		},
		{
			name:           "nested enum with AND",
			filter:         `nested.state = MY_ENUM_VALUE AND nested.field1`,
			expectedClause: "WHERE ((nested->>'state' = $1) AND (nested->>'field1')::boolean)",
			expectedParams: []any{"MY_ENUM_VALUE"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
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

func TestFilteringRequestParser_ColumnNameChanges(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
	}{
		{
			name:           "root field with column name override",
			filter:         `column_name_changed = 42`,
			expectedClause: "WHERE (new_name = $1)",
			expectedParams: []any{int64(42)},
		},
		{
			name:           "root field with column name override comparison",
			filter:         `column_name_changed > 100`,
			expectedClause: "WHERE (new_name > $1)",
			expectedParams: []any{int64(100)},
		},
		{
			name:           "root field with column name override presence",
			filter:         `column_name_changed:*`,
			expectedClause: "WHERE (new_name IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:           "nested field with column override - integer",
			filter:         `nested_changed.field2 = 42`,
			expectedClause: "WHERE ((nested_new_name->>'field2')::bigint = $1)",
			expectedParams: []any{int64(42)},
		},
		{
			name:           "nested field with column override - string",
			filter:         `nested_changed.field3 = "test"`,
			expectedClause: "WHERE (nested_new_name->>'field3' = $1)",
			expectedParams: []any{"test"},
		},
		{
			name:           "nested field with column override - boolean",
			filter:         `nested_changed.field1`,
			expectedClause: "WHERE (nested_new_name->>'field1')::boolean",
			expectedParams: []any{},
		},
		{
			name:           "nested field with column override - deeply nested",
			filter:         `nested_changed.further_nested.field3 = "deep"`,
			expectedClause: "WHERE (nested_new_name->'further_nested'->>'field3' = $1)",
			expectedParams: []any{"deep"},
		},
		{
			name:           "nested field with column override - presence",
			filter:         `nested_changed:*`,
			expectedClause: "WHERE (nested_new_name IS NOT NULL)",
			expectedParams: []any{},
		},
		{
			name:           "column name changes combined",
			filter:         `column_name_changed > 10 AND nested_changed.field2 = 5`,
			expectedClause: "WHERE ((new_name > $1) AND ((nested_new_name->>'field2')::bigint = $2))",
			expectedParams: []any{int64(10), int64(5)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
		})
	}
}

func TestFilteringRequestParser_TypeValidation(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name   string
		filter string
	}{
		{
			name:   "string field with integer value",
			filter: `id = 123`,
		},
		{
			name:   "integer field with string value",
			filter: `column_name_changed = "not_a_number"`,
		},
		{
			name:   "boolean field with string value",
			filter: `deleted = "true"`,
		},
		{
			name:   "boolean field with integer value",
			filter: `deleted = 1`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
			_, err := parser.Parse(request)
			require.Error(t, err, "expected type mismatch error for: %s", tc.filter)
		})
	}
}

func TestFilteringRequestParser_ErrorCases(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

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
			filter: `nested.undefined = "value"`,
		},
		{
			name:   "missing value after operator",
			filter: `id =`,
		},
		{
			name:   "unbalanced parentheses - missing close",
			filter: `(id = "user1"`,
		},
		{
			name:   "unbalanced parentheses - missing open",
			filter: `id = "user1")`,
		},
		{
			name:   "invalid operator",
			filter: `id === "test"`,
		},
		{
			name:   "empty parentheses",
			filter: `()`,
		},
		{
			name:   "double operator",
			filter: `id = = "test"`,
		},
		{
			name:   "missing operand for AND",
			filter: `id = "test" AND`,
		},
		{
			name:   "missing operand for OR",
			filter: `OR id = "test"`,
		},
		{
			name:   "invalid enum value",
			filter: `my_enum = COMPLETELY_INVALID_ENUM`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
			_, err := parser.Parse(request)
			require.Error(t, err, "expected error for invalid filter: %s", tc.filter)
		})
	}
}

func TestFilteringRequestParser_ComplexFilters(t *testing.T) {
	parser, err := NewFilteringRequestParser[*pb.ListResourcesRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filter         string
		expectedClause string
		expectedParams []any
	}{
		{
			name:           "multi-condition query",
			filter:         `id = "user1" AND NOT deleted AND my_enum = MY_ENUM_VALUE`,
			expectedClause: "WHERE (((id = $1) AND (NOT deleted)) AND (my_enum = $2))",
			expectedParams: []any{"user1", int64(1)},
		},
		{
			name:           "nested fields with logical operators",
			filter:         `nested.field1 AND nested.field2 > 10 OR nested.field3 = "test"`,
			expectedClause: "WHERE ((nested->>'field1')::boolean AND (((nested->>'field2')::bigint > $1) OR (nested->>'field3' = $2)))",
			expectedParams: []any{int64(10), "test"},
		},
		{
			name:           "presence checks combined",
			filter:         `id:* AND nested:* AND NOT deleted`,
			expectedClause: "WHERE (((id IS NOT NULL) AND (nested IS NOT NULL)) AND (NOT deleted))",
			expectedParams: []any{},
		},
		{
			name:           "wildcard string with enum",
			filter:         `id = "user_*" AND my_enum = MY_ENUM_VALUE`,
			expectedClause: "WHERE ((id LIKE $1) AND (my_enum = $2))",
			expectedParams: []any{"user_%", int64(1)},
		},
		{
			name:           "repeated field with other conditions",
			filter:         `tags:"important" AND NOT deleted AND id = "user1"`,
			expectedClause: "WHERE ((($1 = ANY(tags)) AND (NOT deleted)) AND (id = $2))",
			expectedParams: []any{"important", "user1"},
		},
		{
			name:           "map and nested combined",
			filter:         `labels:"env" AND nested.field2 > 5`,
			expectedClause: "WHERE ((labels ? $1) AND ((nested->>'field2')::bigint > $2))",
			expectedParams: []any{"env", int64(5)},
		},
		{
			name:           "deeply nested with column changes",
			filter:         `nested_changed.further_nested.field2 > 100 AND column_name_changed < 50`,
			expectedClause: "WHERE (((nested_new_name->'further_nested'->>'field2')::bigint > $1) AND (new_name < $2))",
			expectedParams: []any{int64(100), int64(50)},
		},
		{
			name:           "multiple OR groups",
			filter:         `(id = "a" OR id = "b") AND (my_enum = MY_ENUM_VALUE OR my_enum = MY_ENUM_UNSPECIFIED)`,
			expectedClause: "WHERE (((id = $1) OR (id = $2)) AND ((my_enum = $3) OR (my_enum = $4)))",
			expectedParams: []any{"a", "b", int64(1), int64(0)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest{Filter: tc.filter}
			parsedRequest, err := parser.Parse(request)
			require.NoError(t, err)
			whereClause, whereParams := parsedRequest.GetSQLWhereClause()
			require.Equal(t, escapeDollar(tc.expectedClause), escapeDollar(whereClause))
			require.Equal(t, tc.expectedParams, whereParams)
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
		{
			name:           "explicitly allowed root field",
			filter:         `id = "test"`,
			expectedClause: "WHERE (id = $1)",
			expectedParams: []any{"test"},
		},
		{
			name:           "wildcard allows nested.field1",
			filter:         `nested.field1`,
			expectedClause: "WHERE (nested->>'field1')::boolean",
			expectedParams: []any{},
		},
		{
			name:           "wildcard allows nested.field2",
			filter:         `nested.field2 = 42`,
			expectedClause: "WHERE ((nested->>'field2')::bigint = $1)",
			expectedParams: []any{int64(42)},
		},
		{
			name:           "wildcard allows nested.field3",
			filter:         `nested.field3 = "value"`,
			expectedClause: "WHERE (nested->>'field3' = $1)",
			expectedParams: []any{"value"},
		},
		{
			name:           "second level wildcard allows field1",
			filter:         `NOT nested2.further_nested.field1`,
			expectedClause: "WHERE (NOT (nested2->'further_nested'->>'field1')::boolean)",
			expectedParams: []any{},
		},
		{
			name:           "second level wildcard allows field2",
			filter:         `nested2.further_nested.field2 = 99`,
			expectedClause: "WHERE ((nested2->'further_nested'->>'field2')::bigint = $1)",
			expectedParams: []any{int64(99)},
		},
		{
			name:           "second level wildcard allows field3",
			filter:         `nested2.further_nested.field3 = "test"`,
			expectedClause: "WHERE (nested2->'further_nested'->>'field3' = $1)",
			expectedParams: []any{"test"},
		},
		{
			name:    "non-whitelisted root field fails",
			filter:  `deleted`,
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
		{
			name:           "multiple wildcard fields combined",
			filter:         `id = "test" AND nested.field1 AND nested2.further_nested.field2 = 5`,
			expectedClause: "WHERE (((id = $1) AND (nested->>'field1')::boolean) AND ((nested2->'further_nested'->>'field2')::bigint = $2))",
			expectedParams: []any{"test", int64(5)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.ListResourcesRequest2{Filter: tc.filter}
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

func mustParseTime(s string) any {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

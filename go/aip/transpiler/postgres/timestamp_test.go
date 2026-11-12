package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.einride.tech/aip/filtering"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

type filterRequest string

func (r filterRequest) GetFilter() string { return string(r) }

func transpileTimestampFilter(t *testing.T, filter string, opts ...TranspilerOption) (string, []any, error) {
	t.Helper()
	declarations, err := filtering.NewDeclarations(
		filtering.DeclareStandardFunctions(),
		filtering.DeclareIdent("create_time", filtering.TypeTimestamp),
	)
	require.NoError(t, err)
	parsed, err := filtering.ParseFilter(filterRequest(filter), declarations)
	require.NoError(t, err)
	return TranspileFilter(parsed, opts...)
}

func TestTimestampLiteral_BindsUTC(t *testing.T) {
	want := time.Date(2026, time.October, 1, 6, 0, 0, 0, time.UTC)
	for _, filter := range []string{
		`create_time >= "2026-10-01T00:00:00-06:00"`,
		`create_time >= timestamp("2026-10-01T00:00:00-06:00")`,
	} {
		t.Run(filter, func(t *testing.T) {
			sql, params, err := transpileTimestampFilter(t, filter)
			require.NoError(t, err)
			require.Equal(t, "WHERE (create_time >= $1)", sql)
			require.Len(t, params, 1)
			got, ok := params[0].(time.Time)
			require.True(t, ok, "bound %T, want time.Time", params[0])
			require.True(t, want.Equal(got))
			require.Equal(t, time.UTC, got.Location())
		})
	}
}

func TestTimestampLiteral_Inline(t *testing.T) {
	sql, params, err := transpileTimestampFilter(t, `create_time >= "2026-10-01T00:00:00.5-06:00"`, WithInlineParams())
	require.NoError(t, err)
	require.Empty(t, params)
	require.Equal(t, "WHERE (create_time >= '2026-10-01T06:00:00.5Z')", sql)
}

func TestBuildJSONBTypedExpr_Timestamp(t *testing.T) {
	got := buildJSONBTypedExpr("metadata", []string{"publish_time"}, filtering.TypeTimestamp)
	require.Equal(t, rawSQL("("+buildJSONBTextPath("metadata", []string{"publish_time"})+")::timestamptz"), got)
	require.False(t, isTimestamp(&expr.Type{TypeKind: &expr.Type_Primitive{Primitive: expr.Type_STRING}}))
}

package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	// Registers google.protobuf.NullValue, the enum the tests resolve.
	_ "google.golang.org/protobuf/types/known/structpb"
)

func primitive(kind expr.Type_PrimitiveType) *expr.Type {
	return &expr.Type{TypeKind: &expr.Type_Primitive{Primitive: kind}}
}

func wellKnown(kind expr.Type_WellKnownType) *expr.Type {
	return &expr.Type{TypeKind: &expr.Type_WellKnown{WellKnown: kind}}
}

func enumType(name string) *expr.Type {
	return &expr.Type{TypeKind: &expr.Type_MessageType{MessageType: name}}
}

func TestNullMatches(t *testing.T) {
	tests := []struct {
		name       string
		columnType *expr.Type
		op         string
		value      any
		want       bool
	}{
		// Bools: NULL is false.
		{"bool = false", primitive(expr.Type_BOOL), opEq, false, true},
		{"bool = true", primitive(expr.Type_BOOL), opEq, true, false},
		{"bool != true", primitive(expr.Type_BOOL), opNe, true, true},
		{"bool != false", primitive(expr.Type_BOOL), opNe, false, false},
		// Numbers: NULL is 0.
		{"int = 0", primitive(expr.Type_INT64), opEq, int64(0), true},
		{"int = 1", primitive(expr.Type_INT64), opEq, int64(1), false},
		{"int != 1", primitive(expr.Type_INT64), opNe, int64(1), true},
		{"int != 0", primitive(expr.Type_INT64), opNe, int64(0), false},
		{"int < 1", primitive(expr.Type_INT64), opLt, int64(1), true},
		{"int < 0", primitive(expr.Type_INT64), opLt, int64(0), false},
		{"int <= 0", primitive(expr.Type_INT64), opLe, int64(0), true},
		{"int > -1", primitive(expr.Type_INT64), opGt, int64(-1), true},
		{"int > 0", primitive(expr.Type_INT64), opGt, int64(0), false},
		{"int >= 0", primitive(expr.Type_INT64), opGe, int64(0), true},
		{"double < 0.5", primitive(expr.Type_DOUBLE), opLt, 0.5, true},
		{"double > 0.5", primitive(expr.Type_DOUBLE), opGt, 0.5, false},
		// Strings: NULL is "".
		{`string = ""`, primitive(expr.Type_STRING), opEq, "", true},
		{`string = "a"`, primitive(expr.Type_STRING), opEq, "a", false},
		{`string != "a"`, primitive(expr.Type_STRING), opNe, "a", true},
		{`string < "a"`, primitive(expr.Type_STRING), opLt, "a", true},
		{`string > "a"`, primitive(expr.Type_STRING), opGt, "a", false},
		// Enums: NULL is the zero value, compared by number.
		{"enum = 0", enumType("google.protobuf.NullValue"), opEq, int64(0), true},
		{"enum != 1", enumType("google.protobuf.NullValue"), opNe, int64(1), true},
		// Timestamps and durations: NULL is absent.
		{"timestamp != x", wellKnown(expr.Type_TIMESTAMP), opNe, time.Now(), true},
		{"timestamp = x", wellKnown(expr.Type_TIMESTAMP), opEq, time.Now(), false},
		{"timestamp < x", wellKnown(expr.Type_TIMESTAMP), opLt, time.Now(), false},
		{"timestamp < string", wellKnown(expr.Type_TIMESTAMP), opLt, "2024-01-01T00:00:00Z", false},
		{"duration != 0s", wellKnown(expr.Type_DURATION), opNe, time.Duration(0), true},
		{"duration = 0s", wellKnown(expr.Type_DURATION), opEq, time.Duration(0), false},
		{"duration <= 0s", wellKnown(expr.Type_DURATION), opLe, time.Duration(0), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, nullMatches(tc.columnType, tc.op, literal{value: tc.value}))
		})
	}
}

func TestZeroLiteral(t *testing.T) {
	transpiler := &Transpiler{}
	tests := []struct {
		name     string
		exprType *expr.Type
		jsonb    bool
		want     string
		ok       bool
	}{
		{"string", primitive(expr.Type_STRING), false, "''", true},
		{"bool", primitive(expr.Type_BOOL), false, "FALSE", true},
		{"int", primitive(expr.Type_INT64), false, "0", true},
		{"uint", primitive(expr.Type_UINT64), false, "0", true},
		{"double", primitive(expr.Type_DOUBLE), false, "0", true},
		{"enum column", enumType("google.protobuf.NullValue"), false, "0", true},
		{"enum jsonb", enumType("google.protobuf.NullValue"), true, "'NULL_VALUE'", true},
		{"unknown enum", enumType("google.protobuf.Missing"), false, "", false},
		{"timestamp", wellKnown(expr.Type_TIMESTAMP), false, "", false},
		{"duration", wellKnown(expr.Type_DURATION), false, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := transpiler.zeroLiteral(tc.exprType, tc.jsonb)
			require.Equal(t, tc.ok, ok)
			if ok {
				require.Equal(t, tc.want, got.SQL())
			}
		})
	}
}

func TestNotExpr(t *testing.T) {
	comparison := comparisonOp{lhs: ident("t.col"), op: opEq, rhs: sqlParam("$1")}
	tests := []struct {
		name    string
		operand boolExpr
		want    string
	}{
		{"three-valued comparison is coalesced", paren{expr: comparison}, "NOT COALESCE(t.col = $1, FALSE)"},
		{"null guard is strict", paren{expr: nullGuard{column: ident("t.col"), matches: true, comparison: comparison}}, "NOT (t.col IS NULL OR t.col = $1)"},
		{"presence is strict", paren{expr: isNullExpr{lhs: ident("t.col"), negate: true}}, "NOT (t.col IS NOT NULL)"},
		{"map has is strict", paren{expr: coalesceHasKey{field: ident("t.labels"), key: sqlParam("$1")}}, "NOT (COALESCE(t.labels, '{}') ? $1)"},
		{"nested NOT is strict", paren{expr: notExpr{operand: paren{expr: comparison}}}, "NOT (NOT COALESCE(t.col = $1, FALSE))"},
		{"AND of strict operands is strict", paren{expr: logicalOp{op: opAnd, lhs: isNullExpr{lhs: ident("a")}, rhs: isNullExpr{lhs: ident("b")}}}, "NOT (a IS NULL AND b IS NULL)"},
		{"AND with a three-valued operand is coalesced", paren{expr: logicalOp{op: opAnd, lhs: isNullExpr{lhs: ident("a")}, rhs: comparison}}, "NOT COALESCE(a IS NULL AND t.col = $1, FALSE)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, notExpr{operand: tc.operand}.SQL())
		})
	}
}

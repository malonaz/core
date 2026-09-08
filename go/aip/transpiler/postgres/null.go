package postgres

import (
	"cmp"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// NULL semantics.
//
// A filter evaluates against the resource as the API presents it, not against
// the row. The model layer stores a nullable scalar's proto zero value as NULL
// (and protojson omits zero-valued scalars, so a missing JSONB key is NULL
// too), while the API renders that NULL back as the zero value. A NULL column
// therefore *is* zero for scalars and enums: `genre = GENRE_UNSPECIFIED`,
// `count < 10` and `title != "x"` all match it. Well-known message types
// (timestamp, duration) have no zero rendering — NULL is simply absent, which
// differs from every value and orders with none.
//
// The stored form is not observable at transpile time, so every comparison is
// rewritten by whether the type's zero satisfies it: `(col IS NULL OR col OP
// $n)` when it does, plain `col OP $n` when it does not — keeping the common
// non-zero equality index-friendly and strictly boolean.

// literal is a comparison operand whose value is known at transpile time.
// value is the proto-level value (an enum's number even when the SQL operand
// is its name), so zero-ness is decided independently of the SQL rendering.
type literal struct {
	expr  sqlExpr
	value any // bool, int64, float64, string, time.Time, time.Duration
}

// zeroSign compares the literal's type zero against its value (-1, 0, 1).
func (l literal) zeroSign() int {
	switch v := l.value.(type) {
	case bool:
		if v {
			return -1
		}
		return 0
	case int64:
		return cmp.Compare(0, v)
	case float64:
		return cmp.Compare(0, v)
	case string:
		return strings.Compare("", v)
	default:
		return 0
	}
}

// absent reports whether NULL means absent rather than zero for a column
// type: the well-known message types (timestamp, duration), which the API
// renders as unset, never as a zero value. Decided on the column, not the
// literal — a timestamp column compares against plain string literals.
func absent(columnType *expr.Type) bool {
	return columnType.GetWellKnown() != expr.Type_WELL_KNOWN_TYPE_UNSPECIFIED
}

// nullMatches reports whether a NULL column satisfies `column op literal`.
func nullMatches(columnType *expr.Type, op string, l literal) bool {
	if absent(columnType) {
		// Absent differs from every value and orders with none.
		return op == opNe
	}
	sign := l.zeroSign()
	switch op {
	case opEq:
		return sign == 0
	case opNe:
		return sign != 0
	case opLt:
		return sign < 0
	case opLe:
		return sign <= 0
	case opGt:
		return sign > 0
	case opGe:
		return sign >= 0
	default:
		return false
	}
}

// nullAware renders `column op literal` with the NULL semantics above. parent,
// when set, is the enclosing message of a traversed path (see traversalParent).
func nullAware(column sqlExpr, columnType *expr.Type, op string, l literal, parent sqlExpr) boolExpr {
	comparison := comparisonOp{lhs: column, op: op, rhs: l.expr}
	if !nullMatches(columnType, op, l) {
		return comparison
	}
	matched := nullGuard{column: column, matches: true, comparison: comparison}
	if parent == nil {
		return matched
	}
	return logicalOp{op: opAnd, lhs: isNullExpr{lhs: parent, negate: true}, rhs: paren{expr: matched}}
}

// traversalParent returns the enclosing message of a traversed path, as a
// JSONB object expression, or nil. AIP-160 traversal: an entry whose
// non-primitive field in the chain is unset never matches, even on `!=` —
// the NULL-matching rewrite must not admit it. Maps are exempt: AIP-160 leaves
// undefined keys to the service, and `labels.k != "v"` matching unlabelled
// resources is the documented behavior.
func (t *Transpiler) traversalParent(e *expr.Expr) sqlExpr {
	selectExpr := e.GetSelectExpr()
	if selectExpr == nil {
		return nil
	}
	operand := selectExpr.GetOperand()
	if t.filter.CheckedExpr.GetTypeMap()[operand.GetId()].GetMapType() != nil {
		return nil
	}
	if identExpr := operand.GetIdentExpr(); identExpr != nil {
		return ident(identExpr.GetName())
	}
	path, root := t.extractSelectPath(operand)
	return rawSQL(buildJSONBObjectPath(root, path))
}

// zeroLiteral renders the SQL literal a NULL column of the given type stands
// for; ok is false for types whose NULL means absent. jsonb selects the
// JSONB rendering of enums (their name) over the column one (their number).
func (t *Transpiler) zeroLiteral(exprType *expr.Type, jsonb bool) (sqlExpr, bool) {
	if absent(exprType) {
		return nil, false
	}
	switch exprType.GetPrimitive() {
	case expr.Type_STRING:
		return rawSQL("''"), true
	case expr.Type_BOOL:
		return rawSQL("FALSE"), true
	case expr.Type_INT64, expr.Type_UINT64, expr.Type_DOUBLE:
		return rawSQL("0"), true
	}
	if exprType.GetMessageType() == "" {
		return nil, false
	}
	enumDescriptor := t.resolveEnumDescriptor(protoreflect.FullName(exprType.GetMessageType()))
	if enumDescriptor == nil {
		return nil, false
	}
	if !jsonb {
		return rawSQL("0"), true
	}
	zeroValue := enumDescriptor.Values().ByNumber(0)
	if zeroValue == nil {
		return nil, false
	}
	return rawSQL("'" + string(zeroValue.Name()) + "'"), true
}

// strict reports whether a boolean expression can never evaluate to NULL.
// Comparisons against a NULL column yield NULL, which AND/OR propagate
// harmlessly (a NULL conjunct or disjunct never admits a row) but NOT
// inverts into a silently dropped row — see notExpr.
func strict(e sqlExpr) bool {
	switch v := e.(type) {
	case paren:
		return strict(v.expr)
	case isNullExpr, coalesceHasKey, nullGuard, notExpr:
		return true
	case logicalOp:
		return strict(v.lhs) && strict(v.rhs)
	default:
		return false
	}
}

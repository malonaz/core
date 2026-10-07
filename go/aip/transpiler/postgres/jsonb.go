package postgres

import (
	"encoding/json"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func buildJSONBTextPath(root string, path []string) string {
	var sb strings.Builder
	sb.WriteString(root)
	for i, p := range path {
		if i < len(path)-1 {
			sb.WriteString("->'")
		} else {
			sb.WriteString("->>'")
		}
		sb.WriteString(p)
		sb.WriteString("'")
	}
	return sb.String()
}

func buildJSONBObjectPath(root string, path []string) string {
	var sb strings.Builder
	sb.WriteString(root)
	for _, p := range path {
		sb.WriteString("->'")
		sb.WriteString(p)
		sb.WriteString("'")
	}
	return sb.String()
}

// jsonbContains renders `column @> document`, which a GIN index serves. The
// document is a bound constant: built in SQL, it would be rebuilt per row.
type jsonbContains struct {
	column   sqlExpr
	document sqlExpr
}

func (c jsonbContains) SQL() string { return c.column.SQL() + " @> " + c.document.SQL() + "::jsonb" }
func (c jsonbContains) isBoolExpr() {}

// mapEntryContainment rewrites `map.key = "value"` as containment for
// non-empty string values: `= ""` also matches a missing key (see null.go).
func (t *Transpiler) mapEntryContainment(lhs, rhs *expr.Expr) (boolExpr, bool) {
	selectExpr := lhs.GetSelectExpr()
	if selectExpr == nil {
		return nil, false
	}
	operandType := t.filter.CheckedExpr.GetTypeMap()[selectExpr.GetOperand().GetId()]
	if operandType.GetMapType().GetValueType().GetPrimitive() != expr.Type_STRING {
		return nil, false
	}
	value, ok := getStringConstValue(rhs)
	if !ok || value == "" {
		return nil, false
	}
	path, root := t.extractSelectPath(lhs)
	var document any = value
	for i := len(path) - 1; i >= 0; i-- {
		document = map[string]any{path[i]: document}
	}
	// Marshalling string-keyed maps of strings cannot fail.
	bytes, _ := json.Marshal(document)
	return jsonbContains{column: ident(root), document: t.addParam(string(bytes))}, true
}

func buildJSONBTypedExpr(root string, path []string, exprType *expr.Type) rawSQL {
	textPath := buildJSONBTextPath(root, path)
	if exprType.GetWellKnown() == expr.Type_DURATION {
		return rawSQL("(REPLACE(" + textPath + ", 's', ''))::double precision")
	}
	castType := postgresTypeCast(exprType)
	if castType == "" {
		return rawSQL(textPath)
	}
	return rawSQL("(" + textPath + ")::" + castType)
}

func postgresTypeCast(exprType *expr.Type) string {
	switch exprType.GetPrimitive() {
	case expr.Type_BOOL:
		return "boolean"
	case expr.Type_INT64, expr.Type_UINT64:
		return "bigint"
	case expr.Type_DOUBLE:
		return "double precision"
	default:
		return ""
	}
}

func (t *Transpiler) extractSelectPath(e *expr.Expr) (path []string, root string) {
	current := e
	for {
		selectExpr := current.GetSelectExpr()
		if selectExpr == nil {
			break
		}
		path = append([]string{selectExpr.GetField()}, path...)
		operand := selectExpr.GetOperand()
		if identExpr := operand.GetIdentExpr(); identExpr != nil {
			root = identExpr.GetName()
			break
		}
		current = operand
	}
	return
}

func (t *Transpiler) isJSONBPath(e *expr.Expr) bool {
	if e.GetSelectExpr() != nil {
		return true
	}
	// The first '.' is the table prefix.
	return strings.Count(e.GetIdentExpr().GetName(), ".") > 1
}

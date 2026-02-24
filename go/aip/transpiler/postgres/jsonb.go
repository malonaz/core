package postgres

import (
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

func buildJSONBTypedExpr(root string, path []string, exprType *expr.Type) rawSQL {
	textPath := buildJSONBTextPath(root, path)
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
	if identExpr := e.GetIdentExpr(); identExpr != nil {
		return strings.Contains(identExpr.GetName(), ".")
	}
	return false
}

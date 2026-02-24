package postgres

import (
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func isWildcardPattern(s string) bool {
	return strings.HasPrefix(s, "*") || strings.HasSuffix(s, "*")
}

func toLIKEPattern(s string) string {
	return strings.ReplaceAll(s, "*", "%")
}

func getStringConstValue(e *expr.Expr) (string, bool) {
	if e.GetConstExpr() == nil {
		return "", false
	}
	strVal, ok := e.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return "", false
	}
	return strVal.StringValue, true
}

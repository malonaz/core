package postgres

import (
	"fmt"
	"time"

	"go.einride.tech/aip/filtering"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func isTimestamp(t *expr.Type) bool {
	return t.GetWellKnown() == expr.Type_TIMESTAMP
}

// timestampLiteral transpiles what a timestamp column is compared against: an
// RFC 3339 string, bare or wrapped in timestamp(). ok is false for anything
// else (another column). The time is parsed here and bound in UTC because
// neither Postgres nor the driver honour an offset against a `timestamp`
// column: Postgres ignores it in text, pgx drops it from a time.Time.
func (t *Transpiler) timestampLiteral(e *expr.Expr) (literal, bool, error) {
	if call := e.GetCallExpr(); call.GetFunction() == filtering.FunctionTimestamp {
		if len(call.GetArgs()) != 1 {
			return literal{}, false, fmt.Errorf("unexpected number of arguments to `%s`: %d", call.GetFunction(), len(call.GetArgs()))
		}
		e = call.GetArgs()[0]
	} else if e.GetConstExpr() == nil {
		return literal{}, false, nil
	}
	value, ok := getStringConstValue(e)
	if !ok {
		return literal{}, false, fmt.Errorf("a timestamp compares against an RFC 3339 string")
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return literal{}, false, fmt.Errorf("invalid timestamp %q: expected RFC 3339", value)
	}
	parsed = parsed.UTC()
	return literal{expr: t.addParam(parsed), value: parsed}, true, nil
}

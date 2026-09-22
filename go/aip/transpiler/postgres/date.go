package postgres

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	datepb "google.golang.org/genproto/googleapis/type/date"
)

// dateName keys the date type on the proto it renders.
var dateName = string((&datepb.Date{}).ProtoReflect().Descriptor().FullName())

// TypeDate is the filter type of a google.type.Date column. AIP-160 has no
// date literal, so a date is compared against a string, "2025-09-01", as
// timestamps are; dateLiteral parses it. An abstract type rather than the
// message type: a Date nested in a JSONB field is protojson's {year, month,
// day} object, keeps the message type, and is filtered by field.
var TypeDate = &expr.Type{TypeKind: &expr.Type_AbstractType_{AbstractType: &expr.Type_AbstractType{Name: dateName}}}

func isDate(t *expr.Type) bool {
	return t.GetAbstractType().GetName() == dateName
}

// dateLiteral transpiles the string literal a DATE column is compared
// against, so a malformed one is InvalidArgument here rather than a Postgres
// error downstream.
func (t *Transpiler) dateLiteral(e *expr.Expr) (literal, error) {
	value, ok := getStringConstValue(e)
	if !ok {
		return literal{}, fmt.Errorf("a date compares against a string literal, \"YYYY-MM-DD\"")
	}
	day, err := time.Parse(time.DateOnly, value)
	if err != nil {
		return literal{}, fmt.Errorf("invalid date %q: expected \"YYYY-MM-DD\"", value)
	}
	d := pgtype.Date{Time: day, Valid: true}
	return literal{expr: t.addParam(d), value: d}, nil
}

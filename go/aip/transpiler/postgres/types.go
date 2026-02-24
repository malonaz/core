package postgres

import (
	"fmt"
)

type sqlExpr interface {
	SQL() string
}

type boolExpr interface {
	sqlExpr
	isBoolExpr()
}

type rawSQL string

func (r rawSQL) SQL() string { return string(r) }
func (r rawSQL) isBoolExpr() {}

type ident string

func (i ident) SQL() string { return string(i) }
func (i ident) isBoolExpr() {}

type sqlParam string

func (p sqlParam) SQL() string { return string(p) }
func (p sqlParam) isBoolExpr() {}

type paren struct {
	expr sqlExpr
}

func (p paren) SQL() string { return "(" + p.expr.SQL() + ")" }
func (p paren) isBoolExpr() {}

type comparisonOp struct {
	lhs sqlExpr
	op  string
	rhs sqlExpr
}

func (c comparisonOp) SQL() string {
	return c.lhs.SQL() + " " + c.op + " " + c.rhs.SQL()
}
func (c comparisonOp) isBoolExpr() {}

type logicalOp struct {
	op  string
	lhs boolExpr
	rhs boolExpr
}

func (l logicalOp) SQL() string {
	if l.lhs == nil {
		return l.op + " " + l.rhs.SQL()
	}
	return l.lhs.SQL() + " " + l.op + " " + l.rhs.SQL()
}
func (l logicalOp) isBoolExpr() {}

type isNullExpr struct {
	lhs    sqlExpr
	negate bool
}

func (i isNullExpr) SQL() string {
	if i.negate {
		return i.lhs.SQL() + " IS NOT NULL"
	}
	return i.lhs.SQL() + " IS NULL"
}
func (i isNullExpr) isBoolExpr() {}

type anyExpr struct {
	value sqlExpr
	array sqlExpr
}

func (a anyExpr) SQL() string {
	return fmt.Sprintf("%s = ANY(%s)", a.value.SQL(), a.array.SQL())
}
func (a anyExpr) isBoolExpr() {}

type coalesceHasKey struct {
	field sqlExpr
	key   sqlExpr
}

func (c coalesceHasKey) SQL() string {
	return fmt.Sprintf("COALESCE(%s, '{}') ? %s", c.field.SQL(), c.key.SQL())
}
func (c coalesceHasKey) isBoolExpr() {}

const (
	opEq   = "="
	opNe   = "!="
	opLt   = "<"
	opLe   = "<="
	opGt   = ">"
	opGe   = ">="
	opLike = "LIKE"

	opAnd = "AND"
	opOr  = "OR"
	opNot = "NOT"

	opIsDistinctFrom = "IS DISTINCT FROM"
)

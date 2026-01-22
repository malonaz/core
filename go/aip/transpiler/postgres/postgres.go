package postgres

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.einride.tech/aip/filtering"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TranspileFilter(filter filtering.Filter, repeatedFields map[string]bool) (string, []interface{}, error) {
	t := &Transpiler{
		filter:         filter,
		params:         []interface{}{},
		repeatedFields: repeatedFields,
	}
	return t.Transpile()
}

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

type param string

func (p param) SQL() string { return "@" + string(p) }
func (p param) isBoolExpr() {}

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

type isOp struct {
	lhs    sqlExpr
	negate bool
}

func (i isOp) SQL() string {
	if i.negate {
		return i.lhs.SQL() + " IS NOT NULL"
	}
	return i.lhs.SQL() + " IS NULL"
}
func (i isOp) isBoolExpr() {}

const (
	opEq   = "="
	opNe   = "!="
	opLt   = "<"
	opLe   = "<="
	opGt   = ">"
	opGe   = ">="
	opLike = "LIKE"
)

const (
	opAnd = "AND"
	opOr  = "OR"
	opNot = "NOT"
)

type Transpiler struct {
	filter         filtering.Filter
	params         []interface{}
	paramCounter   int
	repeatedFields map[string]bool
}

func (t *Transpiler) Transpile() (string, []interface{}, error) {
	if t.filter.CheckedExpr == nil {
		return "", nil, nil
	}
	resultExpr, err := t.transpileExpr(t.filter.CheckedExpr.GetExpr())
	if err != nil {
		return "", nil, err
	}
	resultBoolExpr, ok := resultExpr.(boolExpr)
	if !ok {
		return "", nil, fmt.Errorf("not a bool expr")
	}
	sql := "WHERE " + strings.ReplaceAll(resultBoolExpr.SQL(), "@_param_", "$")
	return sql, t.params, nil
}

func (t *Transpiler) transpileExpr(e *expr.Expr) (sqlExpr, error) {
	switch e.GetExprKind().(type) {
	case *expr.Expr_CallExpr:
		result, err := t.transpileCallExpr(e)
		if err != nil {
			return nil, err
		}
		return paren{expr: result}, nil
	case *expr.Expr_IdentExpr:
		return t.transpileIdentExpr(e)
	case *expr.Expr_SelectExpr:
		return t.transpileSelectExpr(e)
	case *expr.Expr_ConstExpr:
		return t.transpileConstExpr(e)
	default:
		return nil, fmt.Errorf("unsupported expr: %v", e)
	}
}

func (t *Transpiler) transpileConstExpr(e *expr.Expr) (sqlExpr, error) {
	switch kind := e.GetConstExpr().GetConstantKind().(type) {
	case *expr.Constant_BoolValue:
		return t.param(kind.BoolValue), nil
	case *expr.Constant_DoubleValue:
		return t.param(kind.DoubleValue), nil
	case *expr.Constant_Int64Value:
		return t.param(kind.Int64Value), nil
	case *expr.Constant_StringValue:
		return t.param(kind.StringValue), nil
	case *expr.Constant_Uint64Value:
		return t.param(int64(kind.Uint64Value)), nil
	default:
		return nil, fmt.Errorf("unsupported const expr: %v", kind)
	}
}

func (t *Transpiler) transpileCallExpr(e *expr.Expr) (boolExpr, error) {
	switch e.GetCallExpr().GetFunction() {
	case filtering.FunctionHas:
		return t.transpileHasCallExpr(e)
	case filtering.FunctionEquals:
		if t.isSubstringMatchExpr(e) {
			return t.transpileSubstringMatchExpr(e)
		}
		return t.transpileComparisonCallExpr(e, opEq)
	case filtering.FunctionNotEquals:
		return t.transpileComparisonCallExpr(e, opNe)
	case filtering.FunctionLessThan:
		return t.transpileComparisonCallExpr(e, opLt)
	case filtering.FunctionLessEquals:
		return t.transpileComparisonCallExpr(e, opLe)
	case filtering.FunctionGreaterThan:
		return t.transpileComparisonCallExpr(e, opGt)
	case filtering.FunctionGreaterEquals:
		return t.transpileComparisonCallExpr(e, opGe)
	case filtering.FunctionAnd:
		return t.transpileBinaryLogicalCallExpr(e, opAnd)
	case filtering.FunctionOr:
		return t.transpileBinaryLogicalCallExpr(e, opOr)
	case filtering.FunctionNot:
		return t.transpileNotCallExpr(e)
	case filtering.FunctionTimestamp:
		return nil, fmt.Errorf("timestamp function must be used in comparison context")
	default:
		return nil, fmt.Errorf("unsupported function call: %s", e.GetCallExpr().GetFunction())
	}
}

func (t *Transpiler) transpileIdentExpr(e *expr.Expr) (sqlExpr, error) {
	identExpr := e.GetIdentExpr()
	identType, ok := t.filter.CheckedExpr.GetTypeMap()[e.GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of ident expr %d", e.GetId())
	}
	if messageType := identType.GetMessageType(); messageType != "" {
		if enumType, err := protoregistry.GlobalTypes.FindEnumByName(protoreflect.FullName(messageType)); err == nil {
			if enumValue := enumType.Descriptor().Values().ByName(protoreflect.Name(identExpr.GetName())); enumValue != nil {
				return t.param(int64(enumValue.Number())), nil
			}
		}
	}
	return ident(identExpr.GetName()), nil
}

func (t *Transpiler) transpileIdentExprForJSONB(e *expr.Expr) (sqlExpr, error) {
	identExpr := e.GetIdentExpr()
	identType, ok := t.filter.CheckedExpr.GetTypeMap()[e.GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of ident expr %d", e.GetId())
	}
	if messageType := identType.GetMessageType(); messageType != "" {
		if enumType, err := protoregistry.GlobalTypes.FindEnumByName(protoreflect.FullName(messageType)); err == nil {
			if enumValue := enumType.Descriptor().Values().ByName(protoreflect.Name(identExpr.GetName())); enumValue != nil {
				return t.param(identExpr.GetName()), nil
			}
		}
	}
	return ident(identExpr.GetName()), nil
}

func (t *Transpiler) transpileSelectExpr(e *expr.Expr) (sqlExpr, error) {
	path, root := t.extractSelectPath(e)

	exprType, ok := t.filter.CheckedExpr.GetTypeMap()[e.GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of select expr %d", e.GetId())
	}

	return t.buildJSONBPathExpr(root, path, exprType), nil
}

func (t *Transpiler) buildJSONBPathExpr(root string, path []string, exprType *expr.Type) rawSQL {
	var sb strings.Builder

	needsCast := true
	var castType string
	switch exprType.GetPrimitive() {
	case expr.Type_STRING:
		needsCast = false
	case expr.Type_BOOL:
		castType = "boolean"
	case expr.Type_INT64:
		castType = "bigint"
	case expr.Type_UINT64:
		castType = "bigint"
	case expr.Type_DOUBLE:
		castType = "double precision"
	default:
		needsCast = false
	}

	if needsCast {
		sb.WriteString("(")
	}
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
	if needsCast {
		sb.WriteString(")::")
		sb.WriteString(castType)
	}

	return rawSQL(sb.String())
}

func (t *Transpiler) buildJSONBPathExprRaw(root string, path []string) string {
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

func (t *Transpiler) transpileNotCallExpr(e *expr.Expr) (boolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 1 {
		return nil, fmt.Errorf("unexpected number of arguments to `%s` expression: %d", filtering.FunctionNot, len(callExpr.GetArgs()))
	}
	rhsExpr, err := t.transpileExpr(callExpr.GetArgs()[0])
	if err != nil {
		return nil, err
	}
	rhsBoolExpr, ok := rhsExpr.(boolExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected argument to `%s`: not a bool expr", filtering.FunctionNot)
	}
	return logicalOp{op: opNot, rhs: rhsBoolExpr}, nil
}

func (t *Transpiler) transpileComparisonCallExpr(e *expr.Expr, op string) (boolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 2 {
		return nil, fmt.Errorf("unexpected number of arguments to `%s`: %d", callExpr.GetFunction(), len(callExpr.GetArgs()))
	}

	lhs := callExpr.GetArgs()[0]
	rhs := callExpr.GetArgs()[1]

	lhsExpr, err := t.transpileExpr(lhs)
	if err != nil {
		return nil, err
	}

	if rhs.GetCallExpr() != nil && rhs.GetCallExpr().GetFunction() == filtering.FunctionTimestamp {
		rhsExpr, err := t.transpileTimestampCallExpr(rhs)
		if err != nil {
			return nil, err
		}
		return comparisonOp{lhs: lhsExpr, op: op, rhs: rhsExpr}, nil
	}

	var rhsExpr sqlExpr
	if lhs.GetSelectExpr() != nil && rhs.GetIdentExpr() != nil {
		rhsExpr, err = t.transpileIdentExprForJSONB(rhs)
	} else {
		rhsExpr, err = t.transpileExpr(rhs)
	}
	if err != nil {
		return nil, err
	}

	return comparisonOp{lhs: lhsExpr, op: op, rhs: rhsExpr}, nil
}

func (t *Transpiler) isSubstringMatchExpr(e *expr.Expr) bool {
	if len(e.GetCallExpr().GetArgs()) != 2 {
		return false
	}
	lhs := e.GetCallExpr().GetArgs()[0]
	if lhs.GetIdentExpr() == nil && lhs.GetSelectExpr() == nil {
		return false
	}
	rhs := e.GetCallExpr().GetArgs()[1]
	if rhs.GetConstExpr() == nil {
		return false
	}
	rhsStringExpr, ok := rhs.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return false
	}
	return strings.HasPrefix(rhsStringExpr.StringValue, "*") || strings.HasSuffix(rhsStringExpr.StringValue, "*")
}

func (t *Transpiler) transpileSubstringMatchExpr(e *expr.Expr) (boolExpr, error) {
	lhs := e.GetCallExpr().GetArgs()[0]
	rhs := e.GetCallExpr().GetArgs()[1]
	rhsString := rhs.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue).StringValue

	hasLeading := strings.HasPrefix(rhsString, "*")
	hasTrailing := strings.HasSuffix(rhsString, "*")

	trimmed := rhsString
	if hasLeading {
		trimmed = trimmed[1:]
	}
	if hasTrailing {
		trimmed = trimmed[:len(trimmed)-1]
	}

	if strings.Contains(trimmed, "*") {
		return nil, fmt.Errorf("wildcard only supported in leading or trailing positions")
	}

	var lhsExpr sqlExpr
	var err error
	if lhs.GetSelectExpr() != nil {
		lhsExpr, err = t.transpileSelectExpr(lhs)
		if err != nil {
			return nil, err
		}
	} else if lhs.GetIdentExpr() != nil {
		lhsExpr = ident(lhs.GetIdentExpr().GetName())
	} else {
		return nil, fmt.Errorf("unsupported LHS for substring match")
	}

	likePattern := strings.ReplaceAll(rhsString, "*", "%")

	return comparisonOp{
		lhs: lhsExpr,
		op:  opLike,
		rhs: t.param(likePattern),
	}, nil
}

func (t *Transpiler) transpileHasCallExpr(e *expr.Expr) (boolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 2 {
		return nil, fmt.Errorf("unexpected number of arguments to `:` expression: %d", len(callExpr.GetArgs()))
	}
	lhsExpr := callExpr.GetArgs()[0]
	rhsExpr := callExpr.GetArgs()[1]

	lhsType, ok := t.filter.CheckedExpr.GetTypeMap()[lhsExpr.GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of lhs expr %d", lhsExpr.GetId())
	}

	if constExpr := rhsExpr.GetConstExpr(); constExpr != nil {
		if strVal, ok := constExpr.GetConstantKind().(*expr.Constant_StringValue); ok && strVal.StringValue == "*" {
			lhs, err := t.transpileExpr(lhsExpr)
			if err != nil {
				return nil, err
			}
			return isOp{lhs: lhs, negate: true}, nil
		}
	}

	if listType := lhsType.GetListType(); listType != nil {
		return t.transpileHasOnRepeated(lhsExpr, rhsExpr, listType)
	}

	if lhsType.GetMapType() != nil {
		return t.transpileHasOnMap(lhsExpr, rhsExpr)
	}

	if lhsExpr.GetSelectExpr() != nil {
		if t.traversesThroughRepeatedField(lhsExpr) {
			return t.transpileHasOnRepeatedNested(lhsExpr, rhsExpr)
		}
		return t.transpileHasOnSelect(lhsExpr, rhsExpr)
	}

	return nil, fmt.Errorf("unsupported type for `:` operator")
}

func (t *Transpiler) traversesThroughRepeatedField(e *expr.Expr) bool {
	current := e
	for {
		selectExpr := current.GetSelectExpr()
		if selectExpr == nil {
			return false
		}
		operand := selectExpr.GetOperand()
		operandType, ok := t.filter.CheckedExpr.GetTypeMap()[operand.GetId()]
		if ok && operandType.GetListType() != nil {
			return true
		}
		if identExpr := operand.GetIdentExpr(); identExpr != nil {
			if t.repeatedFields != nil && t.repeatedFields[identExpr.GetName()] {
				return true
			}
		}
		current = operand
	}
}

func (t *Transpiler) transpileHasOnMap(lhsExpr, rhsExpr *expr.Expr) (boolExpr, error) {
	lhs, err := t.transpileExpr(lhsExpr)
	if err != nil {
		return nil, err
	}
	rhs, err := t.transpileExpr(rhsExpr)
	if err != nil {
		return nil, err
	}
	return rawSQL(fmt.Sprintf("%s ? %s", lhs.SQL(), rhs.SQL())), nil
}

func (t *Transpiler) transpileHasOnSelect(lhsExpr, rhsExpr *expr.Expr) (boolExpr, error) {
	lhs, err := t.transpileExpr(lhsExpr)
	if err != nil {
		return nil, err
	}

	var rhs sqlExpr
	if rhsExpr.GetIdentExpr() != nil {
		rhs, err = t.transpileIdentExprForJSONB(rhsExpr)
	} else {
		rhs, err = t.transpileExpr(rhsExpr)
	}
	if err != nil {
		return nil, err
	}
	return comparisonOp{lhs: lhs, op: opEq, rhs: rhs}, nil
}

func (t *Transpiler) transpileHasOnRepeated(lhsExpr, rhsExpr *expr.Expr, listType *expr.Type_ListType) (boolExpr, error) {
	if lhsExpr.GetIdentExpr() != nil && listType.GetElemType().GetPrimitive() != expr.Type_PRIMITIVE_TYPE_UNSPECIFIED {
		lhs, err := t.transpileExpr(lhsExpr)
		if err != nil {
			return nil, err
		}
		rhs, err := t.transpileExpr(rhsExpr)
		if err != nil {
			return nil, err
		}
		return rawSQL(fmt.Sprintf("%s = ANY(%s)", rhs.SQL(), lhs.SQL())), nil
	}

	if lhsExpr.GetSelectExpr() != nil {
		return t.transpileHasOnRepeatedNested(lhsExpr, rhsExpr)
	}

	return nil, fmt.Errorf("unsupported repeated field type for `:` operator")
}

func (t *Transpiler) transpileHasOnRepeatedNested(lhsExpr, rhsExpr *expr.Expr) (boolExpr, error) {
	var fieldPath []string
	var repeatedField string
	current := lhsExpr

	for {
		selectExpr := current.GetSelectExpr()
		if selectExpr == nil {
			break
		}
		fieldPath = append([]string{selectExpr.GetField()}, fieldPath...)
		operand := selectExpr.GetOperand()

		operandType, ok := t.filter.CheckedExpr.GetTypeMap()[operand.GetId()]
		isRepeated := ok && operandType.GetListType() != nil
		if !isRepeated {
			if identExpr := operand.GetIdentExpr(); identExpr != nil {
				isRepeated = t.repeatedFields != nil && t.repeatedFields[identExpr.GetName()]
			}
		}

		if isRepeated {
			if identExpr := operand.GetIdentExpr(); identExpr != nil {
				repeatedField = identExpr.GetName()
				break
			}
			if operand.GetSelectExpr() != nil {
				nestedPath, nestedRoot := t.extractSelectPath(operand)
				repeatedField = nestedRoot
				for _, p := range nestedPath {
					repeatedField += "->'" + p + "'"
				}
				break
			}
		}
		current = operand
	}

	if repeatedField == "" {
		return nil, fmt.Errorf("could not find repeated field in nested has expression")
	}

	rhs, err := t.transpileExpr(rhsExpr)
	if err != nil {
		return nil, err
	}

	elemPath := t.buildJSONBPathExprRaw("_elem", fieldPath)

	existsSQL := fmt.Sprintf("EXISTS(SELECT 1 FROM jsonb_array_elements(%s) AS _elem WHERE %s = %s)",
		repeatedField, elemPath, rhs.SQL())

	return rawSQL(existsSQL), nil
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

func (t *Transpiler) transpileBinaryLogicalCallExpr(e *expr.Expr, op string) (boolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 2 {
		return nil, fmt.Errorf("unexpected number of arguments to `%s`: %d", callExpr.GetFunction(), len(callExpr.GetArgs()))
	}
	lhsExpr, err := t.transpileExpr(callExpr.GetArgs()[0])
	if err != nil {
		return nil, err
	}
	rhsExpr, err := t.transpileExpr(callExpr.GetArgs()[1])
	if err != nil {
		return nil, err
	}
	lhsBoolExpr, ok := lhsExpr.(boolExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected arguments to `%s`: lhs not a bool expr", callExpr.GetFunction())
	}
	rhsBoolExpr, ok := rhsExpr.(boolExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected arguments to `%s` rhs not a bool expr", callExpr.GetFunction())
	}
	return logicalOp{op: op, lhs: lhsBoolExpr, rhs: rhsBoolExpr}, nil
}

func (t *Transpiler) transpileTimestampCallExpr(e *expr.Expr) (sqlExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 1 {
		return nil, fmt.Errorf("unexpected number of arguments to `%s`: %d", callExpr.GetFunction(), len(callExpr.GetArgs()))
	}
	constArg, ok := callExpr.GetArgs()[0].GetExprKind().(*expr.Expr_ConstExpr)
	if !ok {
		return nil, fmt.Errorf("expected constant string arg to %s", callExpr.GetFunction())
	}
	stringArg, ok := constArg.ConstExpr.GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return nil, fmt.Errorf("expected constant string arg to %s", callExpr.GetFunction())
	}
	timeArg, err := time.Parse(time.RFC3339, stringArg.StringValue)
	if err != nil {
		return nil, fmt.Errorf("invalid string arg to %s: %w", callExpr.GetFunction(), err)
	}
	return t.param(timeArg), nil
}

func (t *Transpiler) param(value interface{}) param {
	p := t.nextParam()
	t.params = append(t.params, value)
	return param(p)
}

func (t *Transpiler) nextParam() string {
	t.paramCounter++
	return "_param_" + strconv.Itoa(t.paramCounter)
}

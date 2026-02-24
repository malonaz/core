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

func TranspileFilter(filter filtering.Filter) (string, []any, error) {
	t := &Transpiler{filter: filter, params: []any{}}
	return t.Transpile()
}

type Transpiler struct {
	filter       filtering.Filter
	params       []any
	paramCounter int
}

func (t *Transpiler) Transpile() (string, []any, error) {
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
	return "WHERE " + resultBoolExpr.SQL(), t.params, nil
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

func (t *Transpiler) transpileConstExpr(e *expr.Expr) (sqlExpr, error) {
	switch kind := e.GetConstExpr().GetConstantKind().(type) {
	case *expr.Constant_BoolValue:
		return t.addParam(kind.BoolValue), nil
	case *expr.Constant_DoubleValue:
		return t.addParam(kind.DoubleValue), nil
	case *expr.Constant_Int64Value:
		return t.addParam(kind.Int64Value), nil
	case *expr.Constant_StringValue:
		return t.addParam(kind.StringValue), nil
	case *expr.Constant_Uint64Value:
		return t.addParam(int64(kind.Uint64Value)), nil
	default:
		return nil, fmt.Errorf("unsupported const expr: %v", kind)
	}
}

func (t *Transpiler) transpileIdentExpr(e *expr.Expr) (sqlExpr, error) {
	identExpr := e.GetIdentExpr()
	identType, ok := t.filter.CheckedExpr.GetTypeMap()[e.GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of ident expr %d", e.GetId())
	}
	if enumValue, ok := t.resolveEnumValue(identType, identExpr.GetName()); ok {
		return t.addParam(int64(enumValue)), nil
	}
	return ident(identExpr.GetName()), nil
}

func (t *Transpiler) transpileIdentExprForJSONB(e *expr.Expr) (sqlExpr, error) {
	identExpr := e.GetIdentExpr()
	identType, ok := t.filter.CheckedExpr.GetTypeMap()[e.GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of ident expr %d", e.GetId())
	}
	if _, ok := t.resolveEnumValue(identType, identExpr.GetName()); ok {
		return t.addParam(identExpr.GetName()), nil
	}
	return ident(identExpr.GetName()), nil
}

func (t *Transpiler) resolveEnumValue(identType *expr.Type, name string) (protoreflect.EnumNumber, bool) {
	messageType := identType.GetMessageType()
	if messageType == "" {
		return 0, false
	}
	enumType, err := protoregistry.GlobalTypes.FindEnumByName(protoreflect.FullName(messageType))
	if err != nil {
		return 0, false
	}
	enumValue := enumType.Descriptor().Values().ByName(protoreflect.Name(name))
	if enumValue == nil {
		return 0, false
	}
	return enumValue.Number(), true
}

func (t *Transpiler) transpileSelectExpr(e *expr.Expr) (sqlExpr, error) {
	path, root := t.extractSelectPath(e)
	exprType, ok := t.filter.CheckedExpr.GetTypeMap()[e.GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of select expr %d", e.GetId())
	}
	return buildJSONBTypedExpr(root, path, exprType), nil
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

	if op == opNe && t.isJSONBPath(lhs) {
		op = opIsDistinctFrom
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
	val, ok := getStringConstValue(e.GetCallExpr().GetArgs()[1])
	if !ok {
		return false
	}
	return isWildcardPattern(val)
}

func (t *Transpiler) transpileSubstringMatchExpr(e *expr.Expr) (boolExpr, error) {
	lhs := e.GetCallExpr().GetArgs()[0]
	rhsString, _ := getStringConstValue(e.GetCallExpr().GetArgs()[1])

	trimmed := strings.TrimPrefix(strings.TrimSuffix(rhsString, "*"), "*")
	if strings.Contains(trimmed, "*") {
		return nil, fmt.Errorf("wildcard only supported in leading or trailing positions")
	}

	var lhsExpr sqlExpr
	var err error
	switch {
	case lhs.GetSelectExpr() != nil:
		lhsExpr, err = t.transpileSelectExpr(lhs)
	case lhs.GetIdentExpr() != nil:
		lhsExpr = ident(lhs.GetIdentExpr().GetName())
	default:
		return nil, fmt.Errorf("unsupported LHS for substring match")
	}
	if err != nil {
		return nil, err
	}

	return comparisonOp{
		lhs: lhsExpr,
		op:  opLike,
		rhs: t.addParam(toLIKEPattern(rhsString)),
	}, nil
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
	return t.addParam(timeArg), nil
}

func (t *Transpiler) addParam(value any) sqlParam {
	t.paramCounter++
	t.params = append(t.params, value)
	return sqlParam("$" + strconv.Itoa(t.paramCounter))
}

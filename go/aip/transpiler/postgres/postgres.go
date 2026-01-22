package postgres

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip/filtering"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

const (
	FunctionIsNull       = "ISNULL"
	jsonbPathPlaceholder = "__jsonb_path__"
)

// TranspileFilter transpiles a parsed AIP filter expression to a spansql.BoolExpr, and
// parameters used in the expression.
// The parameter map is nil if the expression does not contain any parameters.
func TranspileFilter(filter filtering.Filter) (string, []interface{}, error) {
	t := &Transpiler{
		filter: filter,
		params: []interface{}{},
	}
	return t.Transpile()
}

type Transpiler struct {
	filter       filtering.Filter
	params       []interface{}
	paramCounter int
}

func (t *Transpiler) Transpile() (string, []interface{}, error) {
	if t.filter.CheckedExpr == nil {
		return "", nil, nil
	}
	resultExpr, err := t.transpileExpr(t.filter.CheckedExpr.GetExpr())
	if err != nil {
		return "", nil, err
	}
	resultBoolExpr, ok := resultExpr.(spansql.BoolExpr)
	if !ok {
		return "", nil, fmt.Errorf("not a bool expr")
	}
	sql := "WHERE " + strings.ReplaceAll(resultBoolExpr.SQL(), "@_param_", "$")
	sql = strings.ReplaceAll(sql, "@"+jsonbPathPlaceholder, "")
	return sql, t.params, nil
}

func (t *Transpiler) transpileExpr(e *expr.Expr) (spansql.Expr, error) {
	switch e.GetExprKind().(type) {
	case *expr.Expr_CallExpr:
		result, err := t.transpileCallExpr(e)
		if err != nil {
			return nil, err
		}
		return spansql.Paren{Expr: result}, nil
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

func (t *Transpiler) transpileConstExpr(e *expr.Expr) (spansql.Expr, error) {
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

func (t *Transpiler) transpileCallExpr(e *expr.Expr) (spansql.Expr, error) {
	switch e.GetCallExpr().GetFunction() {
	case filtering.FunctionHas:
		return t.transpileHasCallExpr(e)
	case filtering.FunctionEquals:
		if t.isSubstringMatchExpr(e) {
			return t.transpileSubstringMatchExpr(e)
		}
		return t.transpileComparisonCallExpr(e, spansql.Eq)
	case filtering.FunctionNotEquals:
		return t.transpileComparisonCallExpr(e, spansql.Ne)
	case filtering.FunctionLessThan:
		return t.transpileComparisonCallExpr(e, spansql.Lt)
	case filtering.FunctionLessEquals:
		return t.transpileComparisonCallExpr(e, spansql.Le)
	case filtering.FunctionGreaterThan:
		return t.transpileComparisonCallExpr(e, spansql.Gt)
	case filtering.FunctionGreaterEquals:
		return t.transpileComparisonCallExpr(e, spansql.Ge)
	case filtering.FunctionAnd:
		return t.transpileBinaryLogicalCallExpr(e, spansql.And)
	case filtering.FunctionOr:
		return t.transpileBinaryLogicalCallExpr(e, spansql.Or)
	case filtering.FunctionNot:
		return t.transpileNotCallExpr(e)
	case filtering.FunctionTimestamp:
		return t.transpileTimestampCallExpr(e)
	case FunctionIsNull:
		return t.transpileIsNullCallExpr(e)
	default:
		return nil, fmt.Errorf("unsupported function call: %s", e.GetCallExpr().GetFunction())
	}
}

func (t *Transpiler) transpileIdentExpr(e *expr.Expr) (spansql.Expr, error) {
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
	return spansql.ID(identExpr.GetName()), nil
}

func (t *Transpiler) transpileSelectExpr(e *expr.Expr) (spansql.Expr, error) {
	var path []string
	var root string
	current := e
	for {
		selectExpr := current.GetSelectExpr()
		if selectExpr == nil {
			return nil, fmt.Errorf("expected SelectExpr in chain")
		}
		path = append([]string{selectExpr.GetField()}, path...)
		operand := selectExpr.GetOperand()
		if identExpr := operand.GetIdentExpr(); identExpr != nil {
			root = identExpr.GetName()
			break
		}
		if operand.GetSelectExpr() != nil {
			current = operand
			continue
		}
		return nil, fmt.Errorf("unsupported select expr operand type")
	}

	exprType, ok := t.filter.CheckedExpr.GetTypeMap()[e.GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of select expr %d", e.GetId())
	}

	return t.buildJSONBPathExpr(root, path, exprType), nil
}

func (t *Transpiler) buildJSONBPathExpr(root string, path []string, exprType *expr.Type) spansql.Param {
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

	sb.WriteString(jsonbPathPlaceholder)
	if needsCast {
		sb.WriteString("(")
	}
	sb.WriteString(root)
	for i, p := range path {
		if i < len(path)-1 {
			sb.WriteString("->")
		} else {
			sb.WriteString("->>")
		}
		sb.WriteString("'")
		sb.WriteString(p)
		sb.WriteString("'")
	}
	if needsCast {
		sb.WriteString(")::")
		sb.WriteString(castType)
	}

	return spansql.Param(sb.String())
}

func (t *Transpiler) transpileNotCallExpr(e *expr.Expr) (spansql.BoolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 1 {
		return nil, fmt.Errorf("unexpected number of arguments to `%s` expression: %d", filtering.FunctionNot, len(callExpr.GetArgs()))
	}
	rhsExpr, err := t.transpileExpr(callExpr.GetArgs()[0])
	if err != nil {
		return nil, err
	}
	rhsBoolExpr, ok := rhsExpr.(spansql.BoolExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected argument to `%s`: not a bool expr", filtering.FunctionNot)
	}
	return spansql.LogicalOp{Op: spansql.Not, RHS: rhsBoolExpr}, nil
}

func (t *Transpiler) transpileComparisonCallExpr(e *expr.Expr, op spansql.ComparisonOperator) (spansql.BoolExpr, error) {
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
	return spansql.ComparisonOp{Op: op, LHS: lhsExpr, RHS: rhsExpr}, nil
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

func (t *Transpiler) transpileSubstringMatchExpr(e *expr.Expr) (spansql.BoolExpr, error) {
	lhs := e.GetCallExpr().GetArgs()[0]
	rhs := e.GetCallExpr().GetArgs()[1]
	rhsString := rhs.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue).StringValue
	if strings.Contains(strings.TrimSuffix(strings.TrimPrefix(rhsString, "*"), "*"), "*") {
		return nil, fmt.Errorf("wildcard only supported in leading or trailing positions")
	}

	var lhsExpr spansql.Expr
	var err error
	if lhs.GetSelectExpr() != nil {
		lhsExpr, err = t.transpileSelectExpr(lhs)
		if err != nil {
			return nil, err
		}
	} else if lhs.GetIdentExpr() != nil {
		lhsExpr = spansql.ID(lhs.GetIdentExpr().GetName())
	} else {
		return nil, fmt.Errorf("unsupported LHS for substring match")
	}

	return spansql.ComparisonOp{
		Op:  spansql.Like,
		LHS: lhsExpr,
		RHS: t.param(strings.ReplaceAll(rhsString, "*", "%")),
	}, nil
}

func (t *Transpiler) transpileIsNullCallExpr(e *expr.Expr) (spansql.BoolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.Args) != 1 {
		return nil, fmt.Errorf("unexpected number of arguments to `%s`: %d", callExpr.GetFunction(), len(callExpr.Args))
	}
	lhs, err := t.transpileExpr(callExpr.Args[0])
	if err != nil {
		return nil, err
	}
	return spansql.IsOp{LHS: lhs, RHS: spansql.NullLiteral(0)}, nil
}

func (t *Transpiler) transpileBinaryLogicalCallExpr(e *expr.Expr, op spansql.LogicalOperator) (spansql.BoolExpr, error) {
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
	lhsBoolExpr, ok := lhsExpr.(spansql.BoolExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected arguments to `%s`: lhs not a bool expr", callExpr.GetFunction())
	}
	rhsBoolExpr, ok := rhsExpr.(spansql.BoolExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected arguments to `%s` rhs not a bool expr", callExpr.GetFunction())
	}
	return spansql.LogicalOp{Op: op, LHS: lhsBoolExpr, RHS: rhsBoolExpr}, nil
}

func (t *Transpiler) transpileHasCallExpr(e *expr.Expr) (spansql.BoolExpr, error) {
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

	// Handle wildcard existence check (field:*)
	if constExpr := rhsExpr.GetConstExpr(); constExpr != nil {
		if strVal, ok := constExpr.GetConstantKind().(*expr.Constant_StringValue); ok && strVal.StringValue == "*" {
			lhs, err := t.transpileExpr(lhsExpr)
			if err != nil {
				return nil, err
			}
			return spansql.IsOp{LHS: lhs, Neg: true, RHS: spansql.NullLiteral(0)}, nil
		}
	}

	// Handle repeated fields
	if listType := lhsType.GetListType(); listType != nil {
		return t.transpileHasOnRepeated(lhsExpr, rhsExpr, listType)
	}

	// Handle map field key existence check (m:foo)
	if lhsType.GetMapType() != nil {
		lhs, err := t.transpileExpr(lhsExpr)
		if err != nil {
			return nil, err
		}
		rhs, err := t.transpileExpr(rhsExpr)
		if err != nil {
			return nil, err
		}
		return spansql.Paren{Expr: spansql.ID(fmt.Sprintf("%s ? %s", lhs.SQL(), rhs.SQL()))}, nil
	}

	// Handle message field value check (m.foo:42)
	if lhsType.GetMessageType() != "" {
		lhs, err := t.transpileExpr(lhsExpr)
		if err != nil {
			return nil, err
		}
		rhs, err := t.transpileExpr(rhsExpr)
		if err != nil {
			return nil, err
		}
		return spansql.ComparisonOp{Op: spansql.Eq, LHS: lhs, RHS: rhs}, nil
	}

	return nil, fmt.Errorf("unsupported type for `:` operator")
}

func (t *Transpiler) transpileHasCallExprOld(e *expr.Expr) (spansql.BoolExpr, error) {
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

	// Handle wildcard existence check (field:*)
	if constExpr := rhsExpr.GetConstExpr(); constExpr != nil {
		if strVal, ok := constExpr.GetConstantKind().(*expr.Constant_StringValue); ok && strVal.StringValue == "*" {
			lhs, err := t.transpileExpr(lhsExpr)
			if err != nil {
				return nil, err
			}
			return spansql.IsOp{LHS: lhs, Neg: true, RHS: spansql.NullLiteral(0)}, nil
		}
	}

	// Handle repeated fields
	if listType := lhsType.GetListType(); listType != nil {
		return t.transpileHasOnRepeated(lhsExpr, rhsExpr, listType)
	}

	// Handle map/message field value check (m.foo:42)
	if lhsType.GetMapType() != nil || lhsType.GetMessageType() != "" {
		lhs, err := t.transpileExpr(lhsExpr)
		if err != nil {
			return nil, err
		}
		rhs, err := t.transpileExpr(rhsExpr)
		if err != nil {
			return nil, err
		}
		return spansql.ComparisonOp{Op: spansql.Eq, LHS: lhs, RHS: rhs}, nil
	}

	return nil, fmt.Errorf("unsupported type for `:` operator")
}

func (t *Transpiler) transpileHasOnRepeated(lhsExpr, rhsExpr *expr.Expr, listType *expr.Type_ListType) (spansql.BoolExpr, error) {
	// Simple case: repeated primitive with ident (r:42)
	if lhsExpr.GetIdentExpr() != nil && listType.GetElemType().GetPrimitive() != expr.Type_PRIMITIVE_TYPE_UNSPECIFIED {
		lhs, err := t.transpileExpr(lhsExpr)
		if err != nil {
			return nil, err
		}
		rhs, err := t.transpileExpr(rhsExpr)
		if err != nil {
			return nil, err
		}
		return spansql.InOp{Unnest: true, LHS: rhs, RHS: []spansql.Expr{lhs}}, nil
	}

	// Nested case: r.foo:42 - SelectExpr on repeated message
	if lhsExpr.GetSelectExpr() != nil {
		return t.transpileHasOnRepeatedNested(lhsExpr, rhsExpr)
	}

	return nil, fmt.Errorf("unsupported repeated field type for `:` operator")
}

func (t *Transpiler) transpileHasOnRepeatedNested(lhsExpr, rhsExpr *expr.Expr) (spansql.BoolExpr, error) {
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
		if ok && operandType.GetListType() != nil {
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

	var elemPath strings.Builder
	elemPath.WriteString("_elem")
	for i, p := range fieldPath {
		if i < len(fieldPath)-1 {
			elemPath.WriteString("->'")
		} else {
			elemPath.WriteString("->>'")
		}
		elemPath.WriteString(p)
		elemPath.WriteString("'")
	}

	existsSQL := fmt.Sprintf("EXISTS(SELECT 1 FROM jsonb_array_elements(%s) AS _elem WHERE %s = %s)",
		repeatedField, elemPath.String(), rhs.SQL())

	return spansql.Paren{Expr: spansql.ID(existsSQL)}, nil
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

func (t *Transpiler) transpileTimestampCallExpr(e *expr.Expr) (spansql.Expr, error) {
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

func (t *Transpiler) param(param interface{}) spansql.Param {
	p := t.nextParam()
	t.params = append(t.params, param)
	return spansql.Param(p)
}

func (t *Transpiler) nextParam() string {
	param := "_param_" + strconv.Itoa(t.paramCounter+1)
	t.paramCounter++
	return param
}

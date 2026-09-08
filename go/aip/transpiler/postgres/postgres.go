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

func TranspileFilter(filter filtering.Filter, opts ...TranspilerOption) (string, []any, error) {
	t := &Transpiler{filter: filter, params: []any{}}
	for _, opt := range opts {
		opt(t)
	}
	return t.Transpile()
}

// TranspilerOption customizes a Transpiler.
type TranspilerOption func(*Transpiler)

// WithInlineParams renders constants as SQL literals instead of $n
// placeholders, producing a static clause with no bound parameters — for
// callers baking SQL at code-generation time.
func WithInlineParams() TranspilerOption {
	return func(t *Transpiler) { t.inlineParams = true }
}

// WithEnumResolver overrides enum resolution, which defaults to the global
// type registry — for callers operating on descriptors that are not linked
// into the binary (e.g. code generators).
func WithEnumResolver(resolve func(protoreflect.FullName) protoreflect.EnumDescriptor) TranspilerOption {
	return func(t *Transpiler) { t.resolveEnum = resolve }
}

type Transpiler struct {
	filter       filtering.Filter
	params       []any
	paramCounter int
	inlineParams bool
	resolveEnum  func(protoreflect.FullName) protoreflect.EnumDescriptor
	// err records a literal-rendering failure encountered mid-walk, surfaced
	// by Transpile: addParam call sites cannot return errors.
	err error
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
	if t.err != nil {
		return "", nil, t.err
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
	case filtering.FunctionDuration:
		return nil, fmt.Errorf("duration function must be used in comparison context")
	default:
		return nil, fmt.Errorf("unsupported function call: %s", e.GetCallExpr().GetFunction())
	}
}

func (t *Transpiler) transpileConstExpr(e *expr.Expr) (sqlExpr, error) {
	l, err := t.constLiteral(e)
	if err != nil {
		return nil, err
	}
	return l.expr, nil
}

func (t *Transpiler) transpileIdentExpr(e *expr.Expr) (sqlExpr, error) {
	if l, ok, err := t.identLiteral(e, false); err != nil || ok {
		return l.expr, err
	}
	return ident(e.GetIdentExpr().GetName()), nil
}

// identValue resolves an ident that denotes a value rather than a column: an
// enum value name (as its number) or a bool keyword.
func (t *Transpiler) identValue(e *expr.Expr) (any, bool, error) {
	identExpr := e.GetIdentExpr()
	identType, ok := t.filter.CheckedExpr.GetTypeMap()[e.GetId()]
	if !ok {
		return nil, false, fmt.Errorf("unknown type of ident expr %d", e.GetId())
	}
	if enumValue, ok := t.resolveEnumValue(identType, identExpr.GetName()); ok {
		return int64(enumValue), true, nil
	}
	if identType.GetPrimitive() == expr.Type_BOOL {
		switch identExpr.GetName() {
		case "true":
			return true, true, nil
		case "false":
			return false, true, nil
		}
	}
	return nil, false, nil
}

// identLiteral transpiles a value ident. jsonb renders enums by name, as
// protojson stores them, instead of by number, as columns do.
func (t *Transpiler) identLiteral(e *expr.Expr, jsonb bool) (literal, bool, error) {
	value, ok, err := t.identValue(e)
	if err != nil || !ok {
		return literal{}, false, err
	}
	if _, isEnum := value.(int64); isEnum && jsonb {
		return literal{expr: t.addParam(e.GetIdentExpr().GetName()), value: value}, true, nil
	}
	return literal{expr: t.addParam(value), value: value}, true, nil
}

// constLiteral transpiles a constant into a literal.
func (t *Transpiler) constLiteral(e *expr.Expr) (literal, error) {
	switch kind := e.GetConstExpr().GetConstantKind().(type) {
	case *expr.Constant_BoolValue:
		return literal{expr: t.addParam(kind.BoolValue), value: kind.BoolValue}, nil
	case *expr.Constant_DoubleValue:
		return literal{expr: t.addParam(kind.DoubleValue), value: kind.DoubleValue}, nil
	case *expr.Constant_Int64Value:
		return literal{expr: t.addParam(kind.Int64Value), value: kind.Int64Value}, nil
	case *expr.Constant_StringValue:
		return literal{expr: t.addParam(kind.StringValue), value: kind.StringValue}, nil
	case *expr.Constant_Uint64Value:
		return literal{expr: t.addParam(int64(kind.Uint64Value)), value: int64(kind.Uint64Value)}, nil
	default:
		return literal{}, fmt.Errorf("unsupported const expr: %v", kind)
	}
}

// operandLiteral transpiles the right-hand side of a comparison whose left-hand
// side is a column: a constant, an enum value, a bool keyword, or a
// timestamp()/duration() call. ok is false when it is another column.
func (t *Transpiler) operandLiteral(rhs *expr.Expr, jsonbColumn bool) (literal, bool, error) {
	switch {
	case rhs.GetConstExpr() != nil:
		l, err := t.constLiteral(rhs)
		return l, err == nil, err
	case rhs.GetIdentExpr() != nil:
		return t.identLiteral(rhs, jsonbColumn)
	case rhs.GetCallExpr() != nil:
		switch rhs.GetCallExpr().GetFunction() {
		case filtering.FunctionTimestamp:
			l, err := t.transpileTimestampCallExpr(rhs)
			return l, err == nil, err
		case filtering.FunctionDuration:
			l, err := t.transpileDurationCallExpr(rhs, jsonbColumn)
			return l, err == nil, err
		}
	}
	return literal{}, false, nil
}

// isColumn reports whether an expression references a stored field.
func (t *Transpiler) isColumn(e *expr.Expr) (bool, error) {
	if e.GetSelectExpr() != nil {
		return true, nil
	}
	if e.GetIdentExpr() == nil {
		return false, nil
	}
	_, isValue, err := t.identValue(e)
	return !isValue, err
}

func (t *Transpiler) resolveEnumValue(identType *expr.Type, name string) (protoreflect.EnumNumber, bool) {
	messageType := identType.GetMessageType()
	if messageType == "" {
		return 0, false
	}
	enumDescriptor := t.resolveEnumDescriptor(protoreflect.FullName(messageType))
	if enumDescriptor == nil {
		return 0, false
	}
	enumValue := enumDescriptor.Values().ByName(protoreflect.Name(name))
	if enumValue == nil {
		return 0, false
	}
	return enumValue.Number(), true
}

func (t *Transpiler) resolveEnumDescriptor(fullName protoreflect.FullName) protoreflect.EnumDescriptor {
	if t.resolveEnum != nil {
		return t.resolveEnum(fullName)
	}
	enumType, err := protoregistry.GlobalTypes.FindEnumByName(fullName)
	if err != nil {
		return nil
	}
	return enumType.Descriptor()
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

	// NULL semantics apply to `column op literal`; anything else (a literal on
	// the left, column against column) is rendered as written.
	lhsIsColumn, err := t.isColumn(lhs)
	if err != nil {
		return nil, err
	}
	var l literal
	rhsIsLiteral := false
	if lhsIsColumn {
		if l, rhsIsLiteral, err = t.operandLiteral(rhs, t.isJSONBPath(lhs)); err != nil {
			return nil, err
		}
	}
	if !rhsIsLiteral {
		rhsExpr, err := t.transpileExpr(rhs)
		if err != nil {
			return nil, err
		}
		return comparisonOp{lhs: lhsExpr, op: op, rhs: rhsExpr}, nil
	}
	lhsType, ok := t.filter.CheckedExpr.GetTypeMap()[lhs.GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of lhs expr %d", lhs.GetId())
	}
	return nullAware(lhsExpr, lhsType, op, l), nil
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
	if strings.Trim(rhsString, "*") == "" {
		return nil, fmt.Errorf("wildcard pattern must contain non-wildcard characters")
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
	return notExpr{operand: rhsBoolExpr}, nil
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

func (t *Transpiler) transpileTimestampCallExpr(e *expr.Expr) (literal, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 1 {
		return literal{}, fmt.Errorf("unexpected number of arguments to `%s`: %d", callExpr.GetFunction(), len(callExpr.GetArgs()))
	}
	constArg, ok := callExpr.GetArgs()[0].GetExprKind().(*expr.Expr_ConstExpr)
	if !ok {
		return literal{}, fmt.Errorf("expected constant string arg to %s", callExpr.GetFunction())
	}
	stringArg, ok := constArg.ConstExpr.GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return literal{}, fmt.Errorf("expected constant string arg to %s", callExpr.GetFunction())
	}
	timeArg, err := time.Parse(time.RFC3339, stringArg.StringValue)
	if err != nil {
		return literal{}, fmt.Errorf("invalid string arg to %s: %w", callExpr.GetFunction(), err)
	}
	return literal{expr: t.addParam(timeArg), value: timeArg}, nil
}

// transpileDurationCallExpr renders a duration() call; asSeconds selects the
// JSONB representation (protojson's "1.5s" is stored stripped to a double).
func (t *Transpiler) transpileDurationCallExpr(e *expr.Expr, asSeconds bool) (literal, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 1 {
		return literal{}, fmt.Errorf("unexpected number of arguments to `%s`: %d", callExpr.GetFunction(), len(callExpr.GetArgs()))
	}
	constArg, ok := callExpr.GetArgs()[0].GetExprKind().(*expr.Expr_ConstExpr)
	if !ok {
		return literal{}, fmt.Errorf("expected constant string arg to %s", callExpr.GetFunction())
	}
	stringArg, ok := constArg.ConstExpr.GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return literal{}, fmt.Errorf("expected constant string arg to %s", callExpr.GetFunction())
	}
	durationArg, err := time.ParseDuration(stringArg.StringValue)
	if err != nil {
		return literal{}, fmt.Errorf("invalid string arg to %s: %w", callExpr.GetFunction(), err)
	}
	if asSeconds {
		return literal{expr: t.addParam(durationArg.Seconds()), value: durationArg}, nil
	}
	return literal{expr: t.addParam(durationArg), value: durationArg}, nil
}

func (t *Transpiler) addParam(value any) sqlParam {
	if t.inlineParams {
		literal, err := sqlLiteral(value)
		if err != nil && t.err == nil {
			t.err = err
		}
		return sqlParam(literal)
	}
	t.paramCounter++
	t.params = append(t.params, value)
	return sqlParam("$" + strconv.Itoa(t.paramCounter))
}

// sqlLiteral renders a constant as a SQL literal, for inline-params mode.
func sqlLiteral(value any) (string, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return "TRUE", nil
		}
		return "FALSE", nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'", nil
	case time.Time:
		return "'" + v.UTC().Format(time.RFC3339Nano) + "'::timestamptz", nil
	case time.Duration:
		return fmt.Sprintf("'%g seconds'::interval", v.Seconds()), nil
	default:
		return "", fmt.Errorf("cannot render %T as a SQL literal", value)
	}
}

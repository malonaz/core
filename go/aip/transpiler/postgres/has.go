package postgres

import (
	"fmt"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

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

	if val, ok := getStringConstValue(rhsExpr); ok && val == "*" {
		return t.transpilePresenceCheck(lhsExpr, lhsType)
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

func (t *Transpiler) transpilePresenceCheck(lhsExpr *expr.Expr, lhsType *expr.Type) (boolExpr, error) {
	if lhsType.GetPrimitive() == expr.Type_STRING {
		lhs, err := t.transpileExpr(lhsExpr)
		if err != nil {
			return nil, err
		}
		return logicalOp{
			op:  opAnd,
			lhs: isNullExpr{lhs: lhs, negate: true},
			rhs: comparisonOp{lhs: lhs, op: opNe, rhs: rawSQL("''")},
		}, nil
	}

	if lhsType.GetListType() != nil {
		return t.transpileRepeatedPresenceCheck(lhsExpr)
	}

	if lhsType.GetMapType() != nil {
		lhs, err := t.transpileExpr(lhsExpr)
		if err != nil {
			return nil, err
		}
		return logicalOp{
			op:  opAnd,
			lhs: isNullExpr{lhs: lhs, negate: true},
			rhs: comparisonOp{lhs: lhs, op: opNe, rhs: rawSQL("'{}'::jsonb")},
		}, nil
	}

	lhs, err := t.transpileExpr(lhsExpr)
	if err != nil {
		return nil, err
	}
	return isNullExpr{lhs: lhs, negate: true}, nil
}

func (t *Transpiler) transpileRepeatedPresenceCheck(lhsExpr *expr.Expr) (boolExpr, error) {
	if lhsExpr.GetIdentExpr() != nil {
		lhs, err := t.transpileExpr(lhsExpr)
		if err != nil {
			return nil, err
		}
		return logicalOp{
			op:  opAnd,
			lhs: isNullExpr{lhs: lhs, negate: true},
			rhs: comparisonOp{
				lhs: rawSQL(fmt.Sprintf("COALESCE(array_length(%s, 1), 0)", lhs.SQL())),
				op:  opGt,
				rhs: rawSQL("0"),
			},
		}, nil
	}

	path, root := t.extractSelectPath(lhsExpr)
	jsonbPath := buildJSONBObjectPath(root, path)
	return logicalOp{
		op:  opAnd,
		lhs: isNullExpr{lhs: rawSQL(jsonbPath), negate: true},
		rhs: comparisonOp{
			lhs: rawSQL(fmt.Sprintf("jsonb_array_length(%s)", jsonbPath)),
			op:  opGt,
			rhs: rawSQL("0"),
		},
	}, nil
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
	return coalesceHasKey{field: lhs, key: rhs}, nil
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
		return t.transpileHasOnNativeArray(lhsExpr, rhsExpr)
	}
	if lhsExpr.GetSelectExpr() != nil {
		return t.transpileHasOnRepeatedNested(lhsExpr, rhsExpr)
	}
	return nil, fmt.Errorf("unsupported repeated field type for `:` operator")
}

func (t *Transpiler) transpileHasOnNativeArray(lhsExpr, rhsExpr *expr.Expr) (boolExpr, error) {
	if val, ok := getStringConstValue(rhsExpr); ok && isWildcardPattern(val) {
		lhs, err := t.transpileExpr(lhsExpr)
		if err != nil {
			return nil, err
		}
		paramExpr := t.addParam(toLIKEPattern(val))
		return rawSQL(fmt.Sprintf("EXISTS(SELECT 1 FROM unnest(%s) AS _elem WHERE _elem LIKE %s::text)",
			lhs.SQL(), paramExpr.SQL())), nil
	}

	lhs, err := t.transpileExpr(lhsExpr)
	if err != nil {
		return nil, err
	}
	rhs, err := t.transpileExpr(rhsExpr)
	if err != nil {
		return nil, err
	}
	return anyExpr{value: rhs, array: lhs}, nil
}

func (t *Transpiler) transpileHasOnRepeatedNested(lhsExpr, rhsExpr *expr.Expr) (boolExpr, error) {
	repeatedField, fieldPath := t.findRepeatedFieldRoot(lhsExpr)
	if repeatedField == "" {
		return t.transpileHasOnJSONBArray(lhsExpr, rhsExpr)
	}

	if val, ok := getStringConstValue(rhsExpr); ok && isWildcardPattern(val) {
		paramExpr := t.addParam(toLIKEPattern(val))
		if len(fieldPath) == 0 {
			return rawSQL(fmt.Sprintf("EXISTS(SELECT 1 FROM jsonb_array_elements_text(%s) AS _elem WHERE _elem LIKE %s::text)",
				repeatedField, paramExpr.SQL())), nil
		}
		elemPath := buildJSONBTextPath("_elem", fieldPath)
		return rawSQL(fmt.Sprintf("EXISTS(SELECT 1 FROM jsonb_array_elements(%s) AS _elem WHERE %s LIKE %s::text)",
			repeatedField, elemPath, paramExpr.SQL())), nil
	}

	rhs, err := t.transpileExpr(rhsExpr)
	if err != nil {
		return nil, err
	}
	if len(fieldPath) == 0 {
		return rawSQL(fmt.Sprintf("EXISTS(SELECT 1 FROM jsonb_array_elements_text(%s) AS _elem WHERE _elem = %s::text)",
			repeatedField, rhs.SQL())), nil
	}
	elemPath := buildJSONBTextPath("_elem", fieldPath)
	return rawSQL(fmt.Sprintf("EXISTS(SELECT 1 FROM jsonb_array_elements(%s) AS _elem WHERE %s = %s)",
		repeatedField, elemPath, rhs.SQL())), nil
}

func (t *Transpiler) transpileHasOnJSONBArray(lhsExpr, rhsExpr *expr.Expr) (boolExpr, error) {
	path, root := t.extractSelectPath(lhsExpr)
	if root == "" || len(path) == 0 {
		return nil, fmt.Errorf("could not find repeated field in nested has expression")
	}
	arrayExpr := buildJSONBObjectPath(root, path)

	if val, ok := getStringConstValue(rhsExpr); ok && isWildcardPattern(val) {
		paramExpr := t.addParam(toLIKEPattern(val))
		return rawSQL(fmt.Sprintf("EXISTS(SELECT 1 FROM jsonb_array_elements_text(%s) AS _elem WHERE _elem LIKE %s::text)",
			arrayExpr, paramExpr.SQL())), nil
	}

	rhs, err := t.transpileExpr(rhsExpr)
	if err != nil {
		return nil, err
	}
	return rawSQL(fmt.Sprintf("EXISTS(SELECT 1 FROM jsonb_array_elements_text(%s) AS _elem WHERE _elem = %s::text)",
		arrayExpr, rhs.SQL())), nil
}

func (t *Transpiler) findRepeatedFieldRoot(e *expr.Expr) (repeatedField string, fieldPath []string) {
	current := e
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
				return identExpr.GetName(), fieldPath
			}
			if operand.GetSelectExpr() != nil {
				nestedPath, nestedRoot := t.extractSelectPath(operand)
				return buildJSONBObjectPath(nestedRoot, nestedPath), fieldPath
			}
		}
		current = operand
	}
	return "", nil
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
		current = operand
	}
}

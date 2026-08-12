package pbjson

import (
	"errors"
	"fmt"
	"strings"

	validate "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/reflect/protoreflect"

	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/pbutil"
)

// getFieldRules returns the buf.validate rules attached to a field, or nil when the field is unconstrained.
func getFieldRules(fieldDescriptor protoreflect.FieldDescriptor) (*validate.FieldRules, error) {
	fieldRules, err := pbutil.GetExtension[*validate.FieldRules](fieldDescriptor.Options(), validate.E_Field)
	if err != nil {
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting field rules: %w", err)
	}
	return fieldRules, nil
}

// isRequiredRule reports whether buf.validate marks this field as required. This complements
// google.api.field_behavior: either source marking a field required makes it required.
func isRequiredRule(fieldRules *validate.FieldRules) bool {
	return fieldRules.GetRequired()
}

// applyScalarRules translates the scalar buf.validate rules of a field onto its schema, so the
// generated tool schema is the single source of truth rather than hand-written comments.
func applyScalarRules(schema *jsonpb.Schema, fieldDescriptor protoreflect.FieldDescriptor, fieldRules *validate.FieldRules) {
	if fieldRules == nil {
		return
	}
	applyStringRules(schema, fieldRules.GetString())
	applyNumericRules(schema, fieldRules)
	applyEnumRules(schema, fieldDescriptor, fieldRules.GetEnum())
}

func applyStringRules(schema *jsonpb.Schema, stringRules *validate.StringRules) {
	if stringRules == nil {
		return
	}
	schema.MinLength = int32(stringRules.GetMinLen())
	schema.MaxLength = int32(stringRules.GetMaxLen())
	schema.Pattern = stringRules.GetPattern()
	if len(stringRules.GetIn()) > 0 {
		schema.Enum = stringRules.GetIn()
	}
	// Well-known formats are a oneof, so at most one of these is ever set.
	switch {
	case stringRules.GetUuid():
		schema.Format = "uuid"
	case stringRules.GetEmail():
		schema.Format = "email"
	case stringRules.GetUri():
		schema.Format = "uri"
	}
}

func applyNumericRules(schema *jsonpb.Schema, fieldRules *validate.FieldRules) {
	// Bounds are modelled as oneofs (GreaterThan/LessThan), so we type-switch rather than
	// use the getters, which cannot distinguish "unset" from "zero".
	switch {
	case fieldRules.GetInt32() != nil:
		rules := fieldRules.GetInt32()
		switch greaterThan := rules.GetGreaterThan().(type) {
		case *validate.Int32Rules_Gte:
			schema.Minimum = float64(greaterThan.Gte)
		case *validate.Int32Rules_Gt:
			schema.ExclusiveMinimum = float64(greaterThan.Gt)
		}
		switch lessThan := rules.GetLessThan().(type) {
		case *validate.Int32Rules_Lte:
			schema.Maximum = float64(lessThan.Lte)
		case *validate.Int32Rules_Lt:
			schema.ExclusiveMaximum = float64(lessThan.Lt)
		}
	case fieldRules.GetInt64() != nil:
		rules := fieldRules.GetInt64()
		switch greaterThan := rules.GetGreaterThan().(type) {
		case *validate.Int64Rules_Gte:
			schema.Minimum = float64(greaterThan.Gte)
		case *validate.Int64Rules_Gt:
			schema.ExclusiveMinimum = float64(greaterThan.Gt)
		}
		switch lessThan := rules.GetLessThan().(type) {
		case *validate.Int64Rules_Lte:
			schema.Maximum = float64(lessThan.Lte)
		case *validate.Int64Rules_Lt:
			schema.ExclusiveMaximum = float64(lessThan.Lt)
		}
	case fieldRules.GetUint32() != nil:
		rules := fieldRules.GetUint32()
		switch greaterThan := rules.GetGreaterThan().(type) {
		case *validate.UInt32Rules_Gte:
			schema.Minimum = float64(greaterThan.Gte)
		case *validate.UInt32Rules_Gt:
			schema.ExclusiveMinimum = float64(greaterThan.Gt)
		}
		switch lessThan := rules.GetLessThan().(type) {
		case *validate.UInt32Rules_Lte:
			schema.Maximum = float64(lessThan.Lte)
		case *validate.UInt32Rules_Lt:
			schema.ExclusiveMaximum = float64(lessThan.Lt)
		}
	case fieldRules.GetUint64() != nil:
		rules := fieldRules.GetUint64()
		switch greaterThan := rules.GetGreaterThan().(type) {
		case *validate.UInt64Rules_Gte:
			schema.Minimum = float64(greaterThan.Gte)
		case *validate.UInt64Rules_Gt:
			schema.ExclusiveMinimum = float64(greaterThan.Gt)
		}
		switch lessThan := rules.GetLessThan().(type) {
		case *validate.UInt64Rules_Lte:
			schema.Maximum = float64(lessThan.Lte)
		case *validate.UInt64Rules_Lt:
			schema.ExclusiveMaximum = float64(lessThan.Lt)
		}
	case fieldRules.GetFloat() != nil:
		rules := fieldRules.GetFloat()
		switch greaterThan := rules.GetGreaterThan().(type) {
		case *validate.FloatRules_Gte:
			schema.Minimum = float64(greaterThan.Gte)
		case *validate.FloatRules_Gt:
			schema.ExclusiveMinimum = float64(greaterThan.Gt)
		}
		switch lessThan := rules.GetLessThan().(type) {
		case *validate.FloatRules_Lte:
			schema.Maximum = float64(lessThan.Lte)
		case *validate.FloatRules_Lt:
			schema.ExclusiveMaximum = float64(lessThan.Lt)
		}
	case fieldRules.GetDouble() != nil:
		rules := fieldRules.GetDouble()
		switch greaterThan := rules.GetGreaterThan().(type) {
		case *validate.DoubleRules_Gte:
			schema.Minimum = greaterThan.Gte
		case *validate.DoubleRules_Gt:
			schema.ExclusiveMinimum = greaterThan.Gt
		}
		switch lessThan := rules.GetLessThan().(type) {
		case *validate.DoubleRules_Lte:
			schema.Maximum = lessThan.Lte
		case *validate.DoubleRules_Lt:
			schema.ExclusiveMaximum = lessThan.Lt
		}
	}
}

// applyEnumRules narrows the enum values the model may pick, honouring `in` and `not_in`.
// The common `not_in: [0]` case removes the UNSPECIFIED sentinel so the model cannot select it.
func applyEnumRules(schema *jsonpb.Schema, fieldDescriptor protoreflect.FieldDescriptor, enumRules *validate.EnumRules) {
	if enumRules == nil || fieldDescriptor.Kind() != protoreflect.EnumKind {
		return
	}
	allowedNumberSet := make(map[int32]bool, len(enumRules.GetIn()))
	for _, number := range enumRules.GetIn() {
		allowedNumberSet[number] = true
	}
	deniedNumberSet := make(map[int32]bool, len(enumRules.GetNotIn()))
	for _, number := range enumRules.GetNotIn() {
		deniedNumberSet[number] = true
	}
	if len(allowedNumberSet) == 0 && len(deniedNumberSet) == 0 {
		return
	}
	// Rebuild from the descriptor so we can match rules, which are expressed as enum numbers,
	// against the names emitted in the schema.
	var enumValueNames []string
	values := fieldDescriptor.Enum().Values()
	for i := 0; i < values.Len(); i++ {
		value := values.Get(i)
		number := int32(value.Number())
		if len(allowedNumberSet) > 0 && !allowedNumberSet[number] {
			continue
		}
		if deniedNumberSet[number] {
			continue
		}
		enumValueNames = append(enumValueNames, string(value.Name()))
	}
	schema.Enum = enumValueNames
}

// applyRepeatedRules translates list cardinality and per-item rules onto an array schema.
func applyRepeatedRules(schema *jsonpb.Schema, fieldDescriptor protoreflect.FieldDescriptor, fieldRules *validate.FieldRules) {
	repeatedRules := fieldRules.GetRepeated()
	if repeatedRules == nil {
		return
	}
	schema.MinItems = int32(repeatedRules.GetMinItems())
	schema.MaxItems = int32(repeatedRules.GetMaxItems())
	if schema.Items != nil {
		applyScalarRules(schema.Items, fieldDescriptor, repeatedRules.GetItems())
	}
}

// describeConstraints renders the rules as prose appended to the description. Several providers
// ignore JSON Schema keywords like minItems in tool schemas, so we restate them for the model.
func describeConstraints(schema *jsonpb.Schema) string {
	var parts []string
	if schema.MinItems > 0 && schema.MaxItems > 0 {
		parts = append(parts, fmt.Sprintf("must contain between %d and %d items", schema.MinItems, schema.MaxItems))
	} else if schema.MinItems > 0 {
		parts = append(parts, fmt.Sprintf("must contain at least %d item(s)", schema.MinItems))
	} else if schema.MaxItems > 0 {
		parts = append(parts, fmt.Sprintf("must contain at most %d item(s)", schema.MaxItems))
	}
	if schema.MinLength > 0 && schema.MaxLength > 0 {
		parts = append(parts, fmt.Sprintf("length must be between %d and %d", schema.MinLength, schema.MaxLength))
	} else if schema.MinLength > 0 {
		parts = append(parts, fmt.Sprintf("length must be at least %d", schema.MinLength))
	} else if schema.MaxLength > 0 {
		parts = append(parts, fmt.Sprintf("length must be at most %d", schema.MaxLength))
	}
	if schema.Pattern != "" {
		parts = append(parts, fmt.Sprintf("must match the pattern %q", schema.Pattern))
	}
	if len(parts) == 0 {
		return ""
	}
	return "Constraints: " + strings.Join(parts, ", ") + "."
}

// appendDescription joins a constraint sentence onto an existing description.
func appendDescription(description, suffix string) string {
	if suffix == "" {
		return description
	}
	if description == "" {
		return suffix
	}
	return strings.TrimRight(description, " ") + " " + suffix
}

// describeOneofs documents oneof groups, which have no JSON Schema equivalent, so the model
// knows exactly one member may be set and which ones are mandatory.
func describeOneofs(messageDescriptor protoreflect.MessageDescriptor) (string, error) {
	oneofs := messageDescriptor.Oneofs()
	var sentences []string
	for i := 0; i < oneofs.Len(); i++ {
		oneof := oneofs.Get(i)
		if oneof.IsSynthetic() {
			continue
		}
		var fieldNames []string
		fields := oneof.Fields()
		for j := 0; j < fields.Len(); j++ {
			fieldNames = append(fieldNames, string(fields.Get(j).Name()))
		}
		if len(fieldNames) == 0 {
			continue
		}
		oneofRules, err := pbutil.GetExtension[*validate.OneofRules](oneof.Options(), validate.E_Oneof)
		if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
			return "", fmt.Errorf("getting oneof rules %q: %w", oneof.FullName(), err)
		}
		qualifier := "at most one"
		if oneofRules.GetRequired() {
			qualifier = "exactly one"
		}
		sentences = append(sentences, fmt.Sprintf("Set %s of %s.", qualifier, strings.Join(fieldNames, ", ")))
	}
	return strings.Join(sentences, " "), nil
}

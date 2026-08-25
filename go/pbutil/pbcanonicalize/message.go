package pbcanonicalize

import (
	"errors"
	"fmt"
	"strings"

	validatepb "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	canonicalizepb "github.com/malonaz/core/genproto/canonicalize/v1"
	"github.com/malonaz/core/go/canonicalize"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

type canonicalizationError struct {
	// Request-relative path to the offending field.
	path *validatepb.FieldPath
	// Rule identifier in protovalidate's custom-rule style, e.g.
	// "canonicalize.phone_number".
	ruleID      string
	description string
}

func (e *canonicalizationError) Error() string {
	return fmt.Sprintf("field %s: %s", pathString(e.path), e.description)
}

func Message(message proto.Message) error {
	err := canonicalizeMessage(message)
	if err == nil {
		return nil
	}
	var canonicalizationErr *canonicalizationError
	if !errors.As(err, &canonicalizationErr) {
		return status.Errorf(codes.Internal, "unexpected canonicalization error: %v", err).Err()
	}
	// Canonicalization is field validation with custom rules: emit the same
	// buf.validate.Violations detail the protovalidate middleware attaches,
	// so clients handle every field error in one dialect...
	violations := &validatepb.Violations{
		Violations: []*validatepb.Violation{{
			Field:   canonicalizationErr.path,
			RuleId:  proto.String(canonicalizationErr.ruleID),
			Message: proto.String(canonicalizationErr.description),
		}},
	}
	// ...and keep the google.rpc.BadRequest detail for AIP-193 consumers,
	// with the request-relative path the AIP expects.
	badRequest := &errdetails.BadRequest{
		FieldViolations: []*errdetails.BadRequest_FieldViolation{{
			Field:       pathString(canonicalizationErr.path),
			Description: canonicalizationErr.description,
		}},
	}
	return status.Errorf(codes.InvalidArgument, "canonicalization failed: %s", canonicalizationErr.description).
		WithDetails(violations, badRequest).
		Err()
}

func canonicalizeMessage(message proto.Message) error {
	var canonicalizeErr error
	protorange.Range(message.ProtoReflect(), func(values protopath.Values) error {
		last := values.Index(-1)
		step := last.Step

		var fieldDescriptor protoreflect.FieldDescriptor
		switch step.Kind() {
		case protopath.FieldAccessStep:
			fieldDescriptor = step.FieldDescriptor()
			if fieldDescriptor.IsList() || fieldDescriptor.IsMap() {
				return nil
			}
		case protopath.ListIndexStep:
			prev := values.Index(-2).Step
			if prev.Kind() != protopath.FieldAccessStep {
				return nil
			}
			fieldDescriptor = prev.FieldDescriptor()
		default:
			return nil
		}

		if fieldDescriptor.Kind() != protoreflect.StringKind {
			return nil
		}

		field, err := pbutil.GetExtension[*canonicalizepb.Field](fieldDescriptor.Options(), canonicalizepb.E_Field)
		if err != nil {
			if errors.Is(err, pbutil.ErrExtensionNotFound) {
				return nil
			}
			canonicalizeErr = &canonicalizationError{
				path:        fieldPath(values.Path),
				ruleID:      "canonicalize",
				description: fmt.Sprintf("get extension: %v", err),
			}
			return protorange.Terminate
		}
		if field == nil {
			canonicalizeErr = &canonicalizationError{
				path:        fieldPath(values.Path),
				ruleID:      "canonicalize",
				description: "expected non-nil field rules",
			}
			return protorange.Terminate
		}

		stringValue := last.Value.String()
		if stringValue == "" {
			return nil
		}

		var canonicalized string
		switch field.GetRule().(type) {
		case *canonicalizepb.Field_EmailAddress:
			canonicalized = canonicalize.EmailAddress(stringValue)
		case *canonicalizepb.Field_PhoneNumber:
			result, err := canonicalize.PhoneNumber(stringValue, canonicalize.RegionCodeUS)
			if err != nil {
				canonicalizeErr = &canonicalizationError{
					path:        fieldPath(values.Path),
					ruleID:      "canonicalize.phone_number",
					description: err.Error(),
				}
				return protorange.Terminate
			}
			canonicalized = result
		default:
			return nil
		}

		switch step.Kind() {
		case protopath.ListIndexStep:
			values.Index(-2).Value.List().Set(step.ListIndex(), protoreflect.ValueOfString(canonicalized))
		default:
			values.Index(-2).Value.Message().Set(fieldDescriptor, protoreflect.ValueOfString(canonicalized))
		}
		return nil
	})
	return canonicalizeErr
}

// fieldPath converts a protorange traversal path into a request-relative
// buf.validate.FieldPath, mirroring the paths protovalidate emits so both
// middlewares speak the same violation dialect.
func fieldPath(path protopath.Path) *validatepb.FieldPath {
	var elements []*validatepb.FieldPathElement
	for _, step := range path {
		switch step.Kind() {
		case protopath.FieldAccessStep:
			fieldDescriptor := step.FieldDescriptor()
			elements = append(elements, &validatepb.FieldPathElement{
				FieldNumber: proto.Int32(int32(fieldDescriptor.Number())),
				FieldName:   proto.String(string(fieldDescriptor.Name())),
				// protoreflect.Kind values coincide with the descriptor enum.
				FieldType: descriptorpb.FieldDescriptorProto_Type(fieldDescriptor.Kind()).Enum(),
			})
		case protopath.ListIndexStep:
			if len(elements) == 0 {
				continue
			}
			elements[len(elements)-1].Subscript = &validatepb.FieldPathElement_Index{Index: uint64(step.ListIndex())}
		case protopath.MapIndexStep:
			if len(elements) == 0 {
				continue
			}
			setMapKeySubscript(elements[len(elements)-1], step.MapIndex())
		}
	}
	return &validatepb.FieldPath{Elements: elements}
}

// The subscript oneof's interface type is unexported: set, don't return.
func setMapKeySubscript(element *validatepb.FieldPathElement, key protoreflect.MapKey) {
	switch value := key.Interface().(type) {
	case string:
		element.Subscript = &validatepb.FieldPathElement_StringKey{StringKey: value}
	case bool:
		element.Subscript = &validatepb.FieldPathElement_BoolKey{BoolKey: value}
	case int32:
		element.Subscript = &validatepb.FieldPathElement_IntKey{IntKey: int64(value)}
	case int64:
		element.Subscript = &validatepb.FieldPathElement_IntKey{IntKey: value}
	case uint32:
		element.Subscript = &validatepb.FieldPathElement_UintKey{UintKey: uint64(value)}
	case uint64:
		element.Subscript = &validatepb.FieldPathElement_UintKey{UintKey: value}
	}
}

// pathString renders a FieldPath in protovalidate's human-readable form
// ("author.phone_number", "phone_numbers[1]", `labels["key"]`), which is
// also the request-relative path AIP-193 expects in BadRequest.
func pathString(path *validatepb.FieldPath) string {
	var builder strings.Builder
	for i, element := range path.GetElements() {
		if i > 0 {
			builder.WriteString(".")
		}
		builder.WriteString(element.GetFieldName())
		switch subscript := element.GetSubscript().(type) {
		case *validatepb.FieldPathElement_Index:
			fmt.Fprintf(&builder, "[%d]", subscript.Index)
		case *validatepb.FieldPathElement_BoolKey:
			fmt.Fprintf(&builder, "[%t]", subscript.BoolKey)
		case *validatepb.FieldPathElement_IntKey:
			fmt.Fprintf(&builder, "[%d]", subscript.IntKey)
		case *validatepb.FieldPathElement_UintKey:
			fmt.Fprintf(&builder, "[%d]", subscript.UintKey)
		case *validatepb.FieldPathElement_StringKey:
			fmt.Fprintf(&builder, "[%q]", subscript.StringKey)
		}
	}
	return builder.String()
}

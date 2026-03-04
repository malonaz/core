package pbcanonicalize

import (
	"errors"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"

	canonicalizepb "github.com/malonaz/core/genproto/canonicalize/v1"
	"github.com/malonaz/core/go/canonicalize"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

type canonicalizationError struct {
	field       string
	description string
}

func (e *canonicalizationError) Error() string {
	return fmt.Sprintf("field %s: %s", e.field, e.description)
}

func Message(message proto.Message) error {
	if err := canonicalizeMessage(message); err != nil {
		var canonicalizationError *canonicalizationError
		if !errors.As(err, &canonicalizationError) {
			return status.Errorf(codes.Internal, "unexpected canonicalization error: %v", err).Err()
		}
		badRequest := &errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       canonicalizationError.field,
					Description: canonicalizationError.description,
				},
			},
		}
		return status.Errorf(codes.InvalidArgument, "canonicalization failed: %s", canonicalizationError.description).
			WithDetails(badRequest).
			Err()
	}
	return nil
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
				field:       string(fieldDescriptor.FullName()),
				description: fmt.Sprintf("get extension: %v", err),
			}
			return protorange.Terminate
		}
		if field == nil {
			canonicalizeErr = &canonicalizationError{
				field:       string(fieldDescriptor.FullName()),
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
					field:       string(fieldDescriptor.FullName()),
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

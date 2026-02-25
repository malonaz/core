package pbutil

import (
	"errors"
	"fmt"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type FieldBehavior struct {
	Optional        bool
	Required        bool
	OutputOnly      bool
	InputOnly       bool
	Immutable       bool
	UnorderedList   bool
	NonEmptyDefault bool
	Identifier      bool
}

func GetFieldBehavior(field protoreflect.FieldDescriptor) (*FieldBehavior, error) {
	fieldBehavior := &FieldBehavior{}
	fieldBehaviors, err := GetExtension[[]annotations.FieldBehavior](field.Options(), annotations.E_FieldBehavior)
	if err != nil {
		if errors.Is(err, ErrExtensionNotFound) {
			return fieldBehavior, nil
		}
		return nil, err
	}

	for _, fb := range fieldBehaviors {
		switch fb {
		case annotations.FieldBehavior_OPTIONAL:
			fieldBehavior.Optional = true
		case annotations.FieldBehavior_REQUIRED:
			fieldBehavior.Required = true
		case annotations.FieldBehavior_OUTPUT_ONLY:
			fieldBehavior.OutputOnly = true
		case annotations.FieldBehavior_INPUT_ONLY:
			fieldBehavior.InputOnly = true
		case annotations.FieldBehavior_IMMUTABLE:
			fieldBehavior.Immutable = true
		case annotations.FieldBehavior_UNORDERED_LIST:
			fieldBehavior.UnorderedList = true
		case annotations.FieldBehavior_NON_EMPTY_DEFAULT:
			fieldBehavior.NonEmptyDefault = true
		case annotations.FieldBehavior_IDENTIFIER:
			fieldBehavior.Identifier = true
		default:
			return nil, fmt.Errorf("unsupported field behavior %s", fb)
		}
	}
	return fieldBehavior, nil
}

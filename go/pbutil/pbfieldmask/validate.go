package pbfieldmask

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// validate validates that the paths in the provided field mask are syntactically valid and
// refer to known fields in the specified message type.
func validate(fm *fieldmaskpb.FieldMask, m proto.Message) error {
	if slices.Contains(fm.GetPaths(), WildcardPath) {
		if len(fm.GetPaths()) != 1 {
			return fmt.Errorf("invalid field path: '*' must not be used with other paths")
		}
		return nil
	}
	md0 := m.ProtoReflect().Descriptor()
	for _, path := range fm.GetPaths() {
		if err := validatePath(md0, path); err != nil {
			return err
		}
	}
	return nil
}

func validatePath(md protoreflect.MessageDescriptor, path string) error {
	segments := splitPathSegments(path)
	for i, segment := range segments {
		if md == nil {
			return fmt.Errorf("invalid field path: %s", path)
		}

		fd := md.Fields().ByName(protoreflect.Name(segment))
		if fd == nil {
			// Could be a map key — check if the parent was a string-keyed map.
			// This case is handled below when we detect a map field and skip
			// validation of the key segment. If we reach here, it's invalid.
			return fmt.Errorf("invalid field path: %s", path)
		}

		if fd.IsMap() && fd.MapKey().Kind() == protoreflect.StringKind {
			// Remaining segments after the map field name are map key + optional value subpaths.
			if i+1 >= len(segments) {
				// Path ends at the map field itself — valid (full replacement).
				return nil
			}
			// Next segment is treated as a map key (always valid for string keys).
			keyIdx := i + 1
			if keyIdx+1 >= len(segments) {
				// Path is "map_field.key" — valid.
				return nil
			}
			// Deeper path into map value — value must be a message.
			valueMd := fd.MapValue().Message()
			if valueMd == nil {
				return fmt.Errorf("invalid field path: %s", path)
			}
			return validatePath(valueMd, rejoinSegments(segments[keyIdx+1:]))
		}

		// Advance into nested message.
		md = fd.Message()
		if fd.IsMap() {
			md = fd.MapValue().Message()
		}
	}
	return nil
}

func rejoinSegments(segments []string) string {
	if len(segments) == 0 {
		return ""
	}
	result := segments[0]
	for _, s := range segments[1:] {
		result += "." + s
	}
	return result
}

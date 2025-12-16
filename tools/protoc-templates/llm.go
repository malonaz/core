package main

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"

	llmpb "github.com/malonaz/core/genproto/codegen/llm/v1"
)

// ParsedLLMMessage contains preprocessed LLM documentation info for a message.
type ParsedLLMMessage struct {
	Message        *protogen.Message
	Opts           *llmpb.MessageOpts
	Generate       bool
	FieldMaskPaths bool

	// DocumentedFields contains the fields that should be documented.
	// If All is true, this contains all fields. Otherwise, it contains
	// only the fields specified in the Opts.Fields list.
	DocumentedFields []*protogen.Field

	// FullFieldMaskPaths contains the complete field mask paths including
	// nested paths like "source.some_field".
	FullFieldMaskPaths []string

	// DocumentedFieldNames is a set of field names for quick lookup.
	DocumentedFieldNames map[string]bool
}

// ShouldDocumentField returns true if the given field should be documented.
func (p *ParsedLLMMessage) ShouldDocumentField(field *protogen.Field) bool {
	if p.Opts.All {
		return true
	}
	return p.DocumentedFieldNames[string(field.Desc.Name())]
}

// getLLMOpts extracts the LLM message options from a protogen.Message.
func getLLMOpts(message *protogen.Message) (*llmpb.MessageOpts, error) {
	options := message.Desc.Options()
	if options == nil {
		return nil, nil
	}

	if !proto.HasExtension(options, llmpb.E_Document) {
		return nil, nil
	}

	ext := proto.GetExtension(options, llmpb.E_Document)
	opts, ok := ext.(*llmpb.MessageOpts)
	if !ok {
		return nil, fmt.Errorf("message %s has invalid llm.document annotation", message.Desc.FullName())
	}

	return opts, nil
}

// parseLLMMessage parses a message's LLM options and returns a preprocessed structure.
func parseLLMMessage(message *protogen.Message) (*ParsedLLMMessage, error) {
	opts, err := getLLMOpts(message)
	if err != nil {
		return nil, err
	}

	if opts == nil {
		return nil, nil
	}

	// If neither generate nor field_mask_paths is set, nothing to do
	if !opts.Generate && !opts.FieldMaskPaths && len(opts.Fields) == 0 && !opts.All {
		return nil, nil
	}

	parsed := &ParsedLLMMessage{
		Message:              message,
		Opts:                 opts,
		Generate:             opts.Generate,
		FieldMaskPaths:       opts.FieldMaskPaths,
		DocumentedFieldNames: make(map[string]bool),
	}

	// Build the set of documented field names
	if opts.All {
		for _, field := range message.Fields {
			parsed.DocumentedFieldNames[string(field.Desc.Name())] = true
		}
	} else {
		for _, fieldName := range opts.Fields {
			parsed.DocumentedFieldNames[fieldName] = true
		}
	}

	// Validate that all specified fields exist
	if err := parsed.validateFields(); err != nil {
		return nil, err
	}

	// Collect documented fields
	parsed.DocumentedFields = parsed.collectDocumentedFields()

	// Compute full field mask paths
	paths, err := parsed.computeFieldMaskPaths("")
	if err != nil {
		return nil, err
	}
	parsed.FullFieldMaskPaths = paths

	return parsed, nil
}

// validateFields checks that all specified field names exist in the message.
func (p *ParsedLLMMessage) validateFields() error {
	if p.Opts.All {
		return nil
	}

	validFields := make(map[string]bool)
	for _, field := range p.Message.Fields {
		validFields[string(field.Desc.Name())] = true
	}

	for _, fieldName := range p.Opts.Fields {
		if !validFields[fieldName] {
			validFieldNames := make([]string, 0, len(p.Message.Fields))
			for _, f := range p.Message.Fields {
				validFieldNames = append(validFieldNames, string(f.Desc.Name()))
			}
			return fmt.Errorf("invalid field '%s' specified in document.fields for message '%s'. Valid fields are: %v",
				fieldName, p.Message.Desc.Name(), validFieldNames)
		}
	}

	return nil
}

// collectDocumentedFields returns the fields that should be documented.
func (p *ParsedLLMMessage) collectDocumentedFields() []*protogen.Field {
	var fields []*protogen.Field
	for _, field := range p.Message.Fields {
		if p.ShouldDocumentField(field) {
			fields = append(fields, field)
		}
	}
	return fields
}

// computeFieldMaskPaths computes the full field mask paths recursively.
// The prefix is prepended to all paths (used for nested messages).
func (p *ParsedLLMMessage) computeFieldMaskPaths(prefix string) ([]string, error) {
	var paths []string
	processedOneofs := make(map[string]bool)

	for _, field := range p.Message.Fields {
		if !p.ShouldDocumentField(field) {
			continue
		}

		fieldName := string(field.Desc.Name())
		fullPath := fieldName
		if prefix != "" {
			fullPath = prefix + "." + fieldName
		}

		// Handle oneof fields - process all fields in the oneof together
		if field.Oneof != nil && !field.Oneof.Desc.IsSynthetic() {
			oneofName := string(field.Oneof.Desc.Name())
			if processedOneofs[oneofName] {
				continue
			}
			processedOneofs[oneofName] = true

			// Process all fields in this oneof
			for _, oneofField := range field.Oneof.Fields {
				oneofFieldName := string(oneofField.Desc.Name())
				if !p.DocumentedFieldNames[oneofFieldName] && !p.Opts.All {
					continue
				}

				oneofFullPath := oneofFieldName
				if prefix != "" {
					oneofFullPath = prefix + "." + oneofFieldName
				}

				// Check if this oneof field has a nested message with documented fields
				nestedPaths, err := getNestedFieldMaskPaths(oneofField, oneofFullPath)
				if err != nil {
					return nil, err
				}

				if len(nestedPaths) > 0 {
					paths = append(paths, nestedPaths...)
				} else {
					paths = append(paths, oneofFullPath)
				}
			}
			continue
		}

		// Check if this field has a nested message with documented fields
		nestedPaths, err := getNestedFieldMaskPaths(field, fullPath)
		if err != nil {
			return nil, err
		}

		if len(nestedPaths) > 0 {
			paths = append(paths, nestedPaths...)
		} else {
			paths = append(paths, fullPath)
		}
	}

	return paths, nil
}

// getNestedFieldMaskPaths returns field mask paths for a nested message field.
// If the field is not a message or has no LLM options, returns nil.
func getNestedFieldMaskPaths(field *protogen.Field, prefix string) ([]string, error) {
	// Only process message fields (not lists/maps of messages for now)
	if field.Message == nil {
		return nil, nil
	}

	// Skip well-known types that we don't recurse into
	fullName := string(field.Message.Desc.FullName())
	if isWellKnownType(fullName) {
		return nil, nil
	}

	// Get LLM options for the nested message
	nestedOpts, err := getLLMOpts(field.Message)
	if err != nil {
		return nil, err
	}

	if nestedOpts == nil {
		return nil, nil
	}

	// Create a parsed message for the nested type
	nestedParsed := &ParsedLLMMessage{
		Message:              field.Message,
		Opts:                 nestedOpts,
		DocumentedFieldNames: make(map[string]bool),
	}

	// Build documented field names for nested message
	if nestedOpts.All {
		for _, f := range field.Message.Fields {
			nestedParsed.DocumentedFieldNames[string(f.Desc.Name())] = true
		}
	} else {
		for _, fieldName := range nestedOpts.Fields {
			nestedParsed.DocumentedFieldNames[fieldName] = true
		}
	}

	// Recursively compute paths
	return nestedParsed.computeFieldMaskPaths(prefix)
}

var wellKnownTypeSet = map[string]struct{}{
	"google.protobuf.Timestamp":   {},
	"google.protobuf.Duration":    {},
	"google.protobuf.Any":         {},
	"google.protobuf.Struct":      {},
	"google.protobuf.Value":       {},
	"google.protobuf.ListValue":   {},
	"google.protobuf.FieldMask":   {},
	"google.protobuf.Empty":       {},
	"google.type.PostalAddress":   {},
	"google.type.Date":            {},
	"google.type.TimeOfDay":       {},
	"google.type.LatLng":          {},
	"google.type.Money":           {},
	"google.protobuf.StringValue": {},
	"google.protobuf.Int32Value":  {},
	"google.protobuf.Int64Value":  {},
	"google.protobuf.UInt32Value": {},
	"google.protobuf.UInt64Value": {},
	"google.protobuf.FloatValue":  {},
	"google.protobuf.DoubleValue": {},
	"google.protobuf.BoolValue":   {},
	"google.protobuf.BytesValue":  {},
}

// isWellKnownType returns true if the type is a well-known protobuf type
// that we don't recurse into for field mask paths.
func isWellKnownType(fullName string) bool {
	_, ok := wellKnownTypeSet[fullName]
	return ok
}

// ParsedLLMField wraps a protogen.Field with additional LLM-specific info.
type ParsedLLMField struct {
	Field    *protogen.Field
	FullPath string

	// For message fields, this contains the parsed nested message if it has LLM options.
	NestedMessage *ParsedLLMMessage
}

// parseLLMField parses a field and returns additional LLM-specific info.
func parseLLMField(field *protogen.Field, prefix string) (*ParsedLLMField, error) {
	fullPath := string(field.Desc.Name())
	if prefix != "" {
		fullPath = prefix + "." + fullPath
	}

	parsed := &ParsedLLMField{
		Field:    field,
		FullPath: fullPath,
	}

	// Check for nested message with LLM options
	if field.Message != nil && !isWellKnownType(string(field.Message.Desc.FullName())) {
		nestedParsed, err := parseLLMMessage(field.Message)
		if err != nil {
			return nil, err
		}
		parsed.NestedMessage = nestedParsed
	}

	return parsed, nil
}

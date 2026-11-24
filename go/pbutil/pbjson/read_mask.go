package pbjson

import (
	"fmt"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/malonaz/core/go/pbutil"
)

const (
	responseReadMaskBaseDescription = "Comma-separated snake_case field paths controlling which fields are returned. Uses dot notation for nested fields. Use the wildcard `*` to return all fields."
	// Bounds each inline subfield list so one wide message cannot bloat every tool description.
	maxReadMaskSubfields = 12
)

// buildResponseReadMaskDescription describes the response_read_mask field: the message the mask
// applies to, found as the read-mask middleware finds it, and every path the middleware accepts on
// it down to one level of nesting.
func buildResponseReadMaskDescription(responseDescriptor protoreflect.MessageDescriptor) string {
	if responseDescriptor == nil {
		return responseReadMaskBaseDescription
	}
	target, relativeTo := readMaskTarget(responseDescriptor)
	description := responseReadMaskBaseDescription + " " + relativeTo
	paths, nested := describeReadMaskPaths(target)
	if paths == "" {
		return description
	}
	description += " Fields: " + paths + "."
	if nested {
		description += " A message field alone returns all of its subfields."
	}
	return description
}

// readMaskTarget mirrors readMaskTarget in go/grpc/middleware: the response if it is a resource,
// else its first resource-typed field, else the response itself.
func readMaskTarget(responseDescriptor protoreflect.MessageDescriptor) (protoreflect.MessageDescriptor, string) {
	if hasResourceAnnotation(responseDescriptor) {
		return responseDescriptor, fmt.Sprintf("Paths are relative to the %s resource.", responseDescriptor.Name())
	}
	fields := responseDescriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Kind() != protoreflect.MessageKind || !hasResourceAnnotation(field.Message()) {
			continue
		}
		if field.IsList() {
			return field.Message(), fmt.Sprintf("Paths are relative to each %s resource in the response (not the response envelope).", field.Message().Name())
		}
		return field.Message(), fmt.Sprintf("Paths are relative to the %s resource in the response (not the response envelope).", field.Message().Name())
	}
	return responseDescriptor, fmt.Sprintf("Paths are relative to the %s message.", responseDescriptor.Name())
}

// describeReadMaskPaths lists the message's fields, each message-typed one followed by its subfields,
// and reports whether it listed any. Paths are spelled out: models copied a f.{a,b} shorthand verbatim.
func describeReadMaskPaths(descriptor protoreflect.MessageDescriptor) (string, bool) {
	fields := descriptor.Fields()
	paths := make([]string, 0, fields.Len())
	var nested bool
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		paths = append(paths, string(field.Name()))
		if field.Kind() == protoreflect.MessageKind && !field.IsMap() && !isOpaqueMessage(field.Message()) {
			subfields := describeSubfields(field)
			paths = append(paths, subfields...)
			nested = nested || len(subfields) > 0
		}
	}
	return strings.Join(paths, ", "), nested
}

// describeSubfields lists the paths of a message field's first subfields; the field itself covers the rest.
func describeSubfields(field protoreflect.FieldDescriptor) []string {
	subfields := field.Message().Fields()
	paths := make([]string, 0, min(subfields.Len(), maxReadMaskSubfields))
	for i := 0; i < subfields.Len() && i < maxReadMaskSubfields; i++ {
		paths = append(paths, string(field.Name())+"."+string(subfields.Get(i).Name()))
	}
	return paths
}

// isOpaqueMessage reports whether a message reads as a value rather than a structure: well-known
// types, google.type and google.rpc messages, and JSON schemas.
func isOpaqueMessage(descriptor protoreflect.MessageDescriptor) bool {
	return strings.HasPrefix(string(descriptor.FullName()), "google.") || descriptor.FullName() == jsonSchemaFullName
}

func hasResourceAnnotation(descriptor protoreflect.MessageDescriptor) bool {
	_, err := pbutil.GetExtension[*annotations.ResourceDescriptor](descriptor.Options(), annotations.E_Resource)
	return err == nil
}

package pbfieldmask

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Update updates fields in dst with values from src according to the provided field mask.
// Nested messages are recursively updated in the same manner.
// Repeated fields and maps are copied by reference from src to dst.
//
// For map fields, individual keys can be targeted using "map_field.key" syntax.
// Non-standard keys can be quoted with backticks: "map_field.`my key`".
// If the key is absent in src, it is removed from dst.
//
// If the special value "*" is provided as the field mask, a full replacement of all fields in dst is done.
//
// See: https://google.aip.dev/134 (Standard methods: Update).
func update(mask *fieldmaskpb.FieldMask, dst, src proto.Message) {
	dstReflect := dst.ProtoReflect()
	srcReflect := src.ProtoReflect()
	if dstReflect.Descriptor() != srcReflect.Descriptor() {
		panic(fmt.Sprintf(
			"dst (%s) and src (%s) messages have different types",
			dstReflect.Descriptor().FullName(),
			srcReflect.Descriptor().FullName(),
		))
	}
	switch {
	case IsFullReplacement(mask):
		proto.Reset(dst)
		proto.Merge(dst, src)
	default:
		for _, path := range mask.GetPaths() {
			segments := splitPathSegments(path)
			updateNamedField(dstReflect, srcReflect, segments)
		}
	}
}

func updateNamedField(dst, src protoreflect.Message, segments []string) {
	if len(segments) == 0 {
		return
	}
	field := src.Descriptor().Fields().ByName(protoreflect.Name(segments[0]))
	if field == nil {
		return
	}

	if len(segments) == 1 {
		if !src.Has(field) {
			dst.Clear(field)
		} else {
			dst.Set(field, src.Get(field))
		}
		return
	}

	// Handle map field with key-level updates.
	if field.IsMap() && field.MapKey().Kind() == protoreflect.StringKind {
		mapKey := segments[1]
		srcMap := src.Get(field).Map()
		dstMap := dst.Mutable(field).Map()
		srcMapKey := protoreflect.ValueOfString(mapKey).MapKey()

		if len(segments) == 2 {
			if srcMap.Has(srcMapKey) {
				dstMap.Set(srcMapKey, srcMap.Get(srcMapKey))
			} else {
				dstMap.Clear(srcMapKey)
			}
			return
		}

		// Deeper traversal into map values that are messages.
		if field.MapValue().Kind() == protoreflect.MessageKind {
			if !dstMap.Has(srcMapKey) {
				dstMap.Set(srcMapKey, dstMap.NewValue())
			}
			dstMsg := dstMap.Get(srcMapKey).Message()
			var srcMsg protoreflect.Message
			if srcMap.Has(srcMapKey) {
				srcMsg = srcMap.Get(srcMapKey).Message()
			} else {
				srcMsg = dstMap.NewValue().Message()
			}
			updateNamedField(dstMsg, srcMsg, segments[2:])
		}
		return
	}

	switch {
	case field.IsList():
		return
	case field.Message() != nil:
		if !dst.Has(field) {
			dst.Set(field, dst.NewField(field))
		}
		if !src.Has(field) {
			src.Set(field, src.NewField(field))
		}
		updateNamedField(dst.Get(field).Message(), src.Get(field).Message(), segments[1:])
	default:
		return
	}
}

// splitPathSegments splits a dot-separated field mask path into segments,
// respecting backtick-quoted segments for non-standard map keys.
func splitPathSegments(path string) []string {
	var segments []string
	for len(path) > 0 {
		if path[0] == '`' {
			end := strings.IndexByte(path[1:], '`')
			if end == -1 {
				segments = append(segments, path[1:])
				break
			}
			segments = append(segments, path[1:end+1])
			path = path[end+2:]
			path = strings.TrimPrefix(path, ".")
		} else {
			i := strings.IndexByte(path, '.')
			if i == -1 {
				segments = append(segments, path)
				break
			}
			segment := path[:i]
			path = path[i+1:]
			if segment == "" {
				continue
			}
			segments = append(segments, segment)
			// Peek ahead for backtick after dot — handled by next iteration.
		}
	}
	return segments
}

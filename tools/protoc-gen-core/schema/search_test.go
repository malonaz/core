package schema_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

// searchDocument compiles a synthetic resource searching the given fields and
// resolves its search document:
//
//	message Doc   { string title; Meta meta [json]; repeated Part parts [json]; }
//	message Meta  { string label; repeated string tags; repeated Part parts; }
//	message Part  { string text; repeated string tags; Inner inner; }
//	message Inner { string value; }
func searchDocument(t *testing.T, fields ...*aippb.SearchOptions_Field) (*protogen.Message, *schema.SearchDoc, error) {
	t.Helper()
	jsonOptions := &descriptorpb.FieldOptions{}
	proto.SetExtension(jsonOptions, modelpb.E_FieldOpts, &modelpb.FieldOpts{AsJsonBytes: true})
	messageOptions := &descriptorpb.MessageOptions{}
	proto.SetExtension(messageOptions, aippb.E_Search, &aippb.SearchOptions{Fields: fields})

	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	field := func(name string, number int32, label *descriptorpb.FieldDescriptorProto_Label, typeName string, options *descriptorpb.FieldOptions) *descriptorpb.FieldDescriptorProto {
		fieldDescriptor := &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Label: label, Type: stringType, Options: options}
		if typeName != "" {
			fieldDescriptor.Type = messageType
			fieldDescriptor.TypeName = proto.String(".searchtest." + typeName)
		}
		return fieldDescriptor
	}
	file := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("searchtest/search.proto"),
		Package: proto.String("searchtest"),
		Syntax:  proto.String("proto3"),
		Options: &descriptorpb.FileOptions{GoPackage: proto.String("example.com/searchtest")},
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Doc"), Options: messageOptions, Field: []*descriptorpb.FieldDescriptorProto{
				field("title", 1, optional, "", nil),
				field("meta", 2, optional, "Meta", jsonOptions),
				field("parts", 3, repeated, "Part", jsonOptions),
			}},
			{Name: proto.String("Meta"), Field: []*descriptorpb.FieldDescriptorProto{
				field("label", 1, optional, "", nil),
				field("tags", 2, repeated, "", nil),
				field("parts", 3, repeated, "Part", nil),
			}},
			{Name: proto.String("Part"), Field: []*descriptorpb.FieldDescriptorProto{
				field("text", 1, optional, "", nil),
				field("tags", 2, repeated, "", nil),
				field("inner", 3, optional, "Inner", nil),
			}},
			{Name: proto.String("Inner"), Field: []*descriptorpb.FieldDescriptorProto{
				field("value", 1, optional, "", nil),
			}},
		},
	}
	plugin, err := protogen.Options{}.New(&pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{file.GetName()},
		ProtoFile:      []*descriptorpb.FileDescriptorProto{file},
	})
	require.NoError(t, err)
	message := plugin.Files[0].Messages[0]
	searchDoc, err := schema.SearchDocument(message)
	return message, searchDoc, err
}

func TestSearchDocumentPaths(t *testing.T) {
	for _, tc := range []struct {
		name     string
		field    *aippb.SearchOptions_Field
		wantBase string
		// wantSnippet, when set, is the expected column-qualified snippet expression.
		wantSnippet string
		wantError   string
	}{
		{
			name:     "TopLevel",
			field:    &aippb.SearchOptions_Field{Path: "title"},
			wantBase: "coalesce(title, '')",
		},
		{
			// Non-repeated paths keep plain JSONB extraction, so existing
			// migrated expressions are unchanged.
			name:     "NestedScalar",
			field:    &aippb.SearchOptions_Field{Path: "meta.label"},
			wantBase: "coalesce(meta #>> '{label}', '')",
		},
		{
			name:     "NestedRepeatedStringTerminal",
			field:    &aippb.SearchOptions_Field{Path: "meta.tags"},
			wantBase: "coalesce(meta #>> '{tags}', '')",
		},
		{
			name:        "RepeatedColumn",
			field:       &aippb.SearchOptions_Field{Path: "parts.text"},
			wantBase:    "core_jsonb_path_text(parts, 'lax $[*].text')",
			wantSnippet: "core_jsonb_path_text(doc.parts, 'lax $[*].text')",
		},
		{
			name:        "RepeatedIntermediate",
			field:       &aippb.SearchOptions_Field{Path: "meta.parts.inner.value"},
			wantBase:    "core_jsonb_path_text(meta, 'lax $.parts[*].inner.value')",
			wantSnippet: "core_jsonb_path_text(doc.meta, 'lax $.parts[*].inner.value')",
		},
		{
			name:     "RepeatedStringTerminalAfterRepeated",
			field:    &aippb.SearchOptions_Field{Path: "parts.tags"},
			wantBase: "core_jsonb_path_text(parts, 'lax $[*].tags[*]')",
		},
		{
			name:        "MaxLength",
			field:       &aippb.SearchOptions_Field{Path: "parts.text", MaxLength: 100},
			wantBase:    "left(core_jsonb_path_text(parts, 'lax $[*].text'), 100)",
			wantSnippet: "left(core_jsonb_path_text(doc.parts, 'lax $[*].text'), 100)",
		},
		{
			name:      "MessageTerminalAfterRepeated",
			field:     &aippb.SearchOptions_Field{Path: "parts.inner"},
			wantError: "must end in a string or repeated string",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, searchDoc, err := searchDocument(t, tc.field)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "setweight(to_tsvector('simple', "+tc.wantBase+"), 'D')", searchDoc.Expression)
			require.Len(t, searchDoc.SnippetFields, 1)
			require.Equal(t, tc.field.GetMaxLength(), searchDoc.SnippetFields[0].MaxLength)

			// Snippets over repeated traversals headline the indexed text, qualified.
			if tc.wantSnippet != "" {
				expression, err := schema.SearchFieldExpression(message, searchDoc.SnippetFields[0], "doc.")
				require.NoError(t, err)
				require.Equal(t, tc.wantSnippet, expression)
			}
		})
	}
}

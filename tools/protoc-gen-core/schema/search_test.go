package schema_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

// searchDocument compiles the library protos with the named message's search
// options replaced by fields, and resolves its search document.
func searchDocument(t *testing.T, messageName protoreflect.FullName, fields ...*aippb.SearchOptions_Field) (*protogen.Message, *schema.SearchDoc, error) {
	t.Helper()
	message := compile(t, messageName, func(messageProto func(protoreflect.FullName) *descriptorpb.DescriptorProto) {
		mutated := messageProto(messageName)
		if mutated.Options == nil {
			mutated.Options = &descriptorpb.MessageOptions{}
		}
		proto.SetExtension(mutated.Options, aippb.E_Search, &aippb.SearchOptions{Fields: fields})
	})
	searchDoc, err := schema.SearchDocument(message)
	return message, searchDoc, err
}

func TestSearchDocument_ID(t *testing.T) {
	const book = "malonaz.test.library.v1.Book"
	id := func(weight aippb.SearchOptions_Weight) *aippb.SearchOptions_Field {
		return &aippb.SearchOptions_Field{Source: &aippb.SearchOptions_Field_Id{Id: true}, Weight: weight}
	}
	path := func(path string) *aippb.SearchOptions_Field {
		return &aippb.SearchOptions_Field{Source: &aippb.SearchOptions_Field_Path{Path: path}, Weight: aippb.SearchOptions_WEIGHT_B}
	}

	t.Run("IndexesResourceIDColumn", func(t *testing.T) {
		message, searchDoc, err := searchDocument(t, book, path("title"), id(aippb.SearchOptions_WEIGHT_A))
		require.NoError(t, err)
		require.Equal(t, "setweight(to_tsvector('simple', coalesce(title, '')), 'B') || setweight(to_tsvector('simple', book_id), 'A')", searchDoc.Expression)
		// Snippets key the id match on "id", weight-ordered.
		require.Equal(t, []schema.SnippetField{
			{Path: schema.SearchIDSnippetPath, ID: true, Expression: "book_id", Weight: "A"},
			{Path: "title", Expression: "coalesce(title, '')", Weight: "B"},
		}, searchDoc.SnippetFields)

		expression, err := schema.SnippetExpression(message, searchDoc.SnippetFields[0], "book.")
		require.NoError(t, err)
		require.Equal(t, "book.book_id", expression)
	})

	t.Run("SplitRejected", func(t *testing.T) {
		field := id(aippb.SearchOptions_WEIGHT_A)
		field.Split = aippb.SearchOptions_SPLIT_PHONE_NUMBER
		_, _, err := searchDocument(t, book, field)
		require.ErrorContains(t, err, "takes no split")
	})

	t.Run("SingletonRejected", func(t *testing.T) {
		_, _, err := searchDocument(t, "malonaz.test.library.v1.AuthorProfile", id(aippb.SearchOptions_WEIGHT_A))
		require.ErrorContains(t, err, "singleton has no resource ID")
	})

	t.Run("DeclaredTwiceRejected", func(t *testing.T) {
		_, _, err := searchDocument(t, book, id(aippb.SearchOptions_WEIGHT_A), id(aippb.SearchOptions_WEIGHT_B))
		require.ErrorContains(t, err, "declared twice")
	})

	t.Run("FalseRejected", func(t *testing.T) {
		_, _, err := searchDocument(t, book, &aippb.SearchOptions_Field{Source: &aippb.SearchOptions_Field_Id{}})
		require.ErrorContains(t, err, "validating extension")
	})

	t.Run("SourceRequired", func(t *testing.T) {
		_, _, err := searchDocument(t, book, &aippb.SearchOptions_Field{Weight: aippb.SearchOptions_WEIGHT_A})
		require.ErrorContains(t, err, "validating extension")
	})
}

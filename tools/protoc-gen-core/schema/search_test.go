package schema_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

// searchDocument compiles the library protos with the named message's search
// options replaced by fields, and resolves its search document.
func searchDocument(t *testing.T, messageName protoreflect.FullName, fields ...*aippb.SearchOptions_Field) (*schema.SearchDoc, error) {
	t.Helper()
	return searchDocumentOf(t, messageName, &aippb.SearchOptions{Fields: fields})
}

// searchDocumentOf compiles the library protos with the named message's search
// options replaced, and resolves its search document.
func searchDocumentOf(t *testing.T, messageName protoreflect.FullName, searchOptions *aippb.SearchOptions) (*schema.SearchDoc, error) {
	t.Helper()
	message := compile(t, messageName, func(messageProto func(protoreflect.FullName) *descriptorpb.DescriptorProto) {
		mutated := messageProto(messageName)
		if mutated.Options == nil {
			mutated.Options = &descriptorpb.MessageOptions{}
		}
		proto.SetExtension(mutated.Options, aippb.E_Search, searchOptions)
	})
	return schema.SearchDocument(message)
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
		searchDoc, err := searchDocument(t, book, path("title"), id(aippb.SearchOptions_WEIGHT_A))
		require.NoError(t, err)
		require.Equal(t, "setweight(to_tsvector('simple', coalesce(title, '')), 'B') || setweight(to_tsvector('simple', book_id), 'A')", searchDoc.Expression)
		// Snippets key the id match on "id", weight-ordered, and headline the table's columns.
		require.Equal(t, []schema.SnippetField{
			{Path: schema.SearchIDSnippetPath, ID: true, Expression: "book.book_id", Weight: "A"},
			{Path: "title", Expression: "coalesce(book.title, '')", Weight: "B"},
		}, searchDoc.SnippetFields)
	})

	t.Run("SplitRejected", func(t *testing.T) {
		field := id(aippb.SearchOptions_WEIGHT_A)
		field.Split = aippb.SearchOptions_SPLIT_PHONE_NUMBER
		_, err := searchDocument(t, book, field)
		require.ErrorContains(t, err, "takes no split")
	})

	t.Run("SingletonRejected", func(t *testing.T) {
		_, err := searchDocument(t, "malonaz.test.library.v1.AuthorProfile", id(aippb.SearchOptions_WEIGHT_A))
		require.ErrorContains(t, err, "singleton has no resource ID")
	})

	t.Run("DeclaredTwiceRejected", func(t *testing.T) {
		_, err := searchDocument(t, book, id(aippb.SearchOptions_WEIGHT_A), id(aippb.SearchOptions_WEIGHT_B))
		require.ErrorContains(t, err, "declared twice")
	})

	t.Run("FalseRejected", func(t *testing.T) {
		_, err := searchDocument(t, book, &aippb.SearchOptions_Field{Source: &aippb.SearchOptions_Field_Id{}})
		require.ErrorContains(t, err, "validating extension")
	})

	t.Run("SourceRequired", func(t *testing.T) {
		_, err := searchDocument(t, book, &aippb.SearchOptions_Field{Weight: aippb.SearchOptions_WEIGHT_A})
		require.ErrorContains(t, err, "validating extension")
	})
}

func TestSearchDocument_Ancestor(t *testing.T) {
	const (
		book      = "malonaz.test.library.v1.Book"
		bookmark  = "malonaz.test.library.v1.Bookmark"
		shelf     = "malonaz.test.library.v1.Shelf"
		bookType  = "library.test.malonaz.com/Book"
		shelfType = "library.test.malonaz.com/Shelf"
	)
	title := &aippb.SearchOptions_Field{Source: &aippb.SearchOptions_Field_Path{Path: "title"}, Weight: aippb.SearchOptions_WEIGHT_A}
	searchedBy := func(ancestorType string) *aippb.SearchOptions {
		return &aippb.SearchOptions{Fields: []*aippb.SearchOptions_Field{title}, Ancestor: ancestorType}
	}

	t.Run("JoinsAncestorRow", func(t *testing.T) {
		searchDoc, err := searchDocumentOf(t, book, searchedBy(shelfType))
		require.NoError(t, err)
		// The stored document stays the resource's own.
		require.Equal(t, "setweight(to_tsvector('simple', coalesce(title, '')), 'A')", searchDoc.Expression)
		require.Equal(t, &schema.Join{
			Key:   "ancestor:" + shelfType,
			Table: schema.Table{Schema: "library", Name: "shelf"},
			Alias: "shelf",
			Conditions: []schema.JoinCondition{
				{SourceColumn: "organization_id", ChildExpr: "%s.organization_id"},
				{SourceColumn: "shelf_id", ChildExpr: "%s.shelf_id"},
			},
		}, searchDoc.Ancestor)
		// The ancestor's fields follow the resource's own of the same weight, under its singular.
		require.Equal(t, []schema.SnippetField{
			{Path: "title", Expression: "coalesce(book.title, '')", Weight: "A"},
			{Path: "shelf.display_name", Expression: "coalesce(shelf.display_name, '')", Weight: "A"},
		}, searchDoc.SnippetFields)
	})

	t.Run("AncestorOwnFieldsOnly", func(t *testing.T) {
		// Book is itself searched by its shelf, whose fields a bookmark does not inherit.
		searchDoc, err := schema.SearchDocument(compile(t, bookmark, func(func(protoreflect.FullName) *descriptorpb.DescriptorProto) {}))
		require.NoError(t, err)
		require.Equal(t, "ancestor:"+bookType, searchDoc.Ancestor.Key)
		require.Equal(t, []schema.SnippetField{
			{Path: "display_name", Expression: "coalesce(bookmark.display_name, '')", Weight: "A"},
			{Path: "book.title", Expression: "coalesce(book.title, '')", Weight: "A"},
			{Path: "book.id", ID: true, Expression: "book.book_id", Weight: "A"},
			{Path: "note", Expression: "coalesce(bookmark.note, '')", Weight: "B"},
			{Path: "book.metadata.summary", Expression: "coalesce(book.metadata #>> '{summary}', '')", Weight: "B"},
		}, searchDoc.SnippetFields)
	})

	t.Run("NotAnAncestorRejected", func(t *testing.T) {
		_, err := searchDocumentOf(t, book, searchedBy("library.test.malonaz.com/Author"))
		require.ErrorContains(t, err, "is not an ancestor")
	})

	t.Run("UnsearchableAncestorRejected", func(t *testing.T) {
		message := compile(t, book, func(messageProto func(protoreflect.FullName) *descriptorpb.DescriptorProto) {
			proto.SetExtension(messageProto(book).Options, aippb.E_Search, searchedBy(shelfType))
			proto.ClearExtension(messageProto(shelf).Options, aippb.E_Search)
		})
		_, err := schema.SearchDocument(message)
		require.ErrorContains(t, err, "declares no search options")
	})

	t.Run("MultiPatternRejected", func(t *testing.T) {
		_, err := searchDocumentOf(t, "malonaz.test.library.v1.Note", &aippb.SearchOptions{
			Fields:   []*aippb.SearchOptions_Field{{Source: &aippb.SearchOptions_Field_Path{Path: "content"}}},
			Ancestor: shelfType,
		})
		require.ErrorContains(t, err, "multi-pattern resources are not supported")
	})
}

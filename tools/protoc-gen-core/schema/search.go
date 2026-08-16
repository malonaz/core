package schema

import (
	"errors"
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/go/pbutil"
)

// SearchDocumentColumn is the database column holding a searchable resource's
// tsvector document. Migrations must declare it as a stored generated column
// whose expression matches the codegen-emitted SearchDocumentExpression.
const SearchDocumentColumn = "search_document"

// SearchArrayToStringFunction is an IMMUTABLE wrapper around array_to_string
// (which is only STABLE and therefore unusable in generated columns) that
// migrations must declare alongside array-bearing search documents:
//
//	CREATE OR REPLACE FUNCTION core_array_to_string(text[], text) RETURNS text
//	LANGUAGE sql IMMUTABLE PARALLEL SAFE
//	AS $$ SELECT array_to_string($1, $2) $$;
const SearchArrayToStringFunction = "core_array_to_string"

// SearchDocument resolves a resource message's search options into the SQL
// expression composing its tsvector search document. Returns nil when the
// message declares no search option.
func SearchDocument(message *protogen.Message) (*SearchDoc, error) {
	searchOptions, err := pbutil.GetExtension[*aippb.SearchOptions](message.Desc.Options(), aippb.E_Search)
	if err != nil {
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting search options for %s: %w", message.GoIdent.GoName, err)
	}

	expressions := make([]string, 0, len(searchOptions.GetFields()))
	var snippetFields []SnippetField
	for _, searchField := range searchOptions.GetFields() {
		field := fieldByName(message, searchField.GetPath())
		if field == nil {
			return nil, fmt.Errorf("search field %q not found on %s", searchField.GetPath(), message.GoIdent.GoName)
		}
		if field.Desc.Kind() != protoreflect.StringKind || field.Desc.IsMap() {
			return nil, fmt.Errorf("search field %q on %s must be a string or repeated string", searchField.GetPath(), message.GoIdent.GoName)
		}

		fieldOpts, err := pbutil.GetExtension[*modelpb.FieldOpts](field.Desc.Options(), modelpb.E_FieldOpts)
		if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
			return nil, fmt.Errorf("getting field opts for %s: %w", searchField.GetPath(), err)
		}

		column := xstrings.ToSnakeCase(searchField.GetPath())
		expression, err := searchFieldExpression(column, field.Desc.IsList(), fieldOpts.GetNullable(), searchField.GetSplit())
		if err != nil {
			return nil, fmt.Errorf("search field %q on %s: %w", searchField.GetPath(), message.GoIdent.GoName, err)
		}
		expressions = append(expressions, fmt.Sprintf(
			"setweight(to_tsvector('simple', %s), '%s')", expression, weightLetter(searchField.GetWeight()),
		))

		if searchField.GetSnippet() {
			// Snippets headline the raw text (no split variant), or fragments
			// would surface mangled tokenized text.
			raw, err := searchFieldExpression(column, field.Desc.IsList(), fieldOpts.GetNullable(), aippb.SearchOptions_SPLIT_UNSPECIFIED)
			if err != nil {
				return nil, fmt.Errorf("snippet field %q on %s: %w", searchField.GetPath(), message.GoIdent.GoName, err)
			}
			snippetFields = append(snippetFields, SnippetField{Path: searchField.GetPath(), Expression: raw})
		}
	}
	return &SearchDoc{Expression: strings.Join(expressions, " || "), SnippetFields: snippetFields}, nil
}

// SearchDoc holds the resolved search document of a resource.
type SearchDoc struct {
	// Expression is the SQL expression composing the tsvector document.
	Expression string
	// SnippetFields are the fields producing highlighted snippets in Search responses.
	SnippetFields []SnippetField
}

// SnippetField is one field headlined into a search-result snippet.
type SnippetField struct {
	// Path is the resource field path, e.g. "biography".
	Path string
	// Expression is the raw text SQL expression to headline.
	Expression string
}

// searchFieldExpression returns the text expression contributing one field to
// the search document, applying the field's tokenization behavior.
func searchFieldExpression(column string, isList, nullable bool, split aippb.SearchOptions_Split) (string, error) {
	// Base text: the raw column, arrays joined on spaces, nulls coalesced away.
	base := fmt.Sprintf("coalesce(%s, '')", column)
	if isList {
		base = fmt.Sprintf("%s(%s, ' ')", SearchArrayToStringFunction, column)
		if nullable {
			base = fmt.Sprintf("%s(coalesce(%s, ARRAY[]::text[]), ' ')", SearchArrayToStringFunction, column)
		}
	}

	switch split {
	case aippb.SearchOptions_SPLIT_UNSPECIFIED:
		return base, nil
	case aippb.SearchOptions_SPLIT_EMAIL_ADDRESS:
		// Index the raw text plus its components: local part, domain labels.
		return fmt.Sprintf("%s || ' ' || translate(%s, '@._-+', '     ')", base, base), nil
	case aippb.SearchOptions_SPLIT_PHONE_NUMBER:
		// Index the raw text plus a digits-only variant, preserving spaces as
		// separators so array elements stay distinct tokens.
		return fmt.Sprintf("%s || ' ' || regexp_replace(%s, '[^0-9 ]', '', 'g')", base, base), nil
	default:
		return "", fmt.Errorf("unsupported split %v", split)
	}
}

// weightLetter maps a search weight to its postgres tsvector letter,
// defaulting to the lowest relevance.
func weightLetter(weight aippb.SearchOptions_Weight) string {
	switch weight {
	case aippb.SearchOptions_WEIGHT_A:
		return "A"
	case aippb.SearchOptions_WEIGHT_B:
		return "B"
	case aippb.SearchOptions_WEIGHT_C:
		return "C"
	default:
		return "D"
	}
}

func fieldByName(message *protogen.Message, name string) *protogen.Field {
	for _, field := range message.Fields {
		if string(field.Desc.Name()) == name {
			return field
		}
	}
	return nil
}

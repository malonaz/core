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
		base, err := searchFieldBase(message, searchField.GetPath())
		if err != nil {
			return nil, err
		}

		expression, err := applySplit(base, searchField.GetSplit())
		if err != nil {
			return nil, fmt.Errorf("search field %q on %s: %w", searchField.GetPath(), message.GoIdent.GoName, err)
		}
		expressions = append(expressions, fmt.Sprintf(
			"setweight(to_tsvector('simple', %s), '%s')", expression, weightLetter(searchField.GetWeight()),
		))

		if searchField.GetSnippet() {
			// Snippets headline the raw text (no split variant), or fragments
			// would surface mangled tokenized text.
			snippetFields = append(snippetFields, SnippetField{Path: searchField.GetPath(), Expression: base})
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

// searchFieldBase resolves a search field path to the raw text SQL expression
// contributing that field to the search document (before split tokenization).
// A dotted path reaches into an as_json_bytes message column via JSONB
// extraction (which is IMMUTABLE, so generated columns keep working).
func searchFieldBase(message *protogen.Message, path string) (string, error) {
	segments := strings.Split(path, ".")
	field := fieldByName(message, segments[0])
	if field == nil {
		return "", fmt.Errorf("search field %q not found on %s", path, message.GoIdent.GoName)
	}

	fieldOpts, err := pbutil.GetExtension[*modelpb.FieldOpts](field.Desc.Options(), modelpb.E_FieldOpts)
	if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
		return "", fmt.Errorf("getting field opts for %s: %w", path, err)
	}
	if fieldOpts.GetJoin() != nil {
		// The search document is a stored generated column: it can only
		// reference columns of its own row, never data projected from a
		// joined table.
		return "", fmt.Errorf("search field %q on %s is a joined field: joined fields cannot be indexed into the search document (denormalize the value onto this resource, or search the parent resource instead)", path, message.GoIdent.GoName)
	}

	column := xstrings.ToSnakeCase(segments[0])
	if len(segments) == 1 {
		if field.Desc.Kind() != protoreflect.StringKind || field.Desc.IsMap() {
			return "", fmt.Errorf("search field %q on %s must be a string or repeated string", path, message.GoIdent.GoName)
		}
		// Base text: the raw column, arrays joined on spaces, nulls coalesced away.
		if field.Desc.IsList() {
			if fieldOpts.GetNullable() {
				return fmt.Sprintf("%s(coalesce(%s, ARRAY[]::text[]), ' ')", SearchArrayToStringFunction, column), nil
			}
			return fmt.Sprintf("%s(%s, ' ')", SearchArrayToStringFunction, column), nil
		}
		return fmt.Sprintf("coalesce(%s, '')", column), nil
	}

	// Dotted path: the first segment must be a message column stored as JSONB.
	if !fieldOpts.GetAsJsonBytes() || field.Desc.IsMap() || field.Message == nil {
		return "", fmt.Errorf("search field %q on %s: first segment %q must be a message field with as_json_bytes = true", path, message.GoIdent.GoName, segments[0])
	}

	// Walk the message definition. JSONB keys are proto field names
	// (pbutil.JSONMarshal sets UseProtoNames), which the segments already are.
	current := field.Message
	for i, segment := range segments[1:] {
		next := fieldByName(current, segment)
		if next == nil {
			return "", fmt.Errorf("search field %q on %s: segment %q not found on %s", path, message.GoIdent.GoName, segment, current.GoIdent.GoName)
		}
		terminal := i == len(segments)-2
		if !terminal {
			if next.Message == nil || next.Desc.IsMap() {
				return "", fmt.Errorf("search field %q on %s: segment %q on %s is not a message field", path, message.GoIdent.GoName, segment, current.GoIdent.GoName)
			}
			current = next.Message
			continue
		}
		// Terminal segment: string, repeated string, or a message (whose whole
		// JSON subtree is indexed: to_tsvector('simple', ...) tokenizes JSON
		// text fine, punctuation and quotes being separators).
		if next.Message == nil && next.Desc.Kind() != protoreflect.StringKind {
			return "", fmt.Errorf("search field %q on %s: terminal segment %q must be a string, repeated string or message field", path, message.GoIdent.GoName, segment)
		}
	}
	return fmt.Sprintf("coalesce(%s #>> '{%s}', '')", column, strings.Join(segments[1:], ",")), nil
}

// applySplit layers a field's extra tokenization behavior on its raw text expression.
func applySplit(base string, split aippb.SearchOptions_Split) (string, error) {
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

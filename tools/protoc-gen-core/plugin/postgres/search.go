package postgres

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"

	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

// headlineOptions configures ts_headline fragments: matches wrapped in **
// markers, at most two short fragments per field.
const headlineOptions = "StartSel=**, StopSel=**, MaxFragments=2, MaxWords=12, MinWords=4"

// snippetIdent flattens a snippet field path into an identifier-safe form for
// the Go struct field, the db tag and the SQL column alias. Dotted JSONB
// paths (e.g. "metadata.body") would otherwise emit invalid Go and require
// quoted SQL aliases; the response map keeps the raw dotted path as its key.
func snippetIdent(path string) string {
	return strings.ReplaceAll(path, ".", "_")
}

// generateSearch emits the full-text search store method and the search
// document expression constant. The table must declare a stored generated
// `search_document` tsvector column matching the emitted expression.
func (mc *msgCtx) generateSearch(searchDoc *schema.SearchDoc) {
	g := mc.g
	pluralGoName := mc.pr.PluralGoName()
	pluralUntitled := untitle(pluralGoName)

	colPrefix := ""
	if mc.hasJoins {
		colPrefix = mc.bareTableName + "."
	}

	// The search document expression, for migrations to declare the generated column.
	g.P(fmt.Sprintf("// %sSearchDocumentExpression is the SQL expression composing the resource's", mc.goType))
	g.P(fmt.Sprintf("// tsvector search document. The %q column must be declared in migrations as:", schema.SearchDocumentColumn))
	g.P(fmt.Sprintf("//   %s tsvector GENERATED ALWAYS AS (<expression>) STORED", schema.SearchDocumentColumn))
	g.P(fmt.Sprintf("const %sSearchDocumentExpression = `%s`", mc.goType, searchDoc.Expression))
	g.P()

	hasSnippets := len(searchDoc.SnippetFields) > 0
	rowType := untitle(mc.goType) + "SearchRow"
	if hasSnippets {
		// Scan target: the model plus one nullable column per snippet field.
		g.P(fmt.Sprintf("type %s struct {", rowType))
		g.P(fmt.Sprintf("  %s", mc.goTypeFqi))
		for _, snippetField := range searchDoc.SnippetFields {
			g.P(fmt.Sprintf("  Snippet%s *string `db:\"__snippet_%s\"`", xstrings.ToPascalCase(snippetIdent(snippetField.Path)), snippetIdent(snippetField.Path)))
		}
		g.P("}")
		g.P()
	}

	parentParam := ""
	if len(mc.parentBindings) > 0 {
		names := make([]string, len(mc.parentBindings))
		for i, binding := range mc.parentBindings {
			names[i] = untitle(xstrings.ToCamelCase(binding.Variable)) + "Id"
		}
		parentParam = strings.Join(names, ", ") + " string, "
	}
	showDeletedParam := ""
	if mc.hasDeleteTime {
		showDeletedParam = "showDeleted bool, "
	}

	returnTypes := fmt.Sprintf("([]*%s, error)", mc.goTypeFqi)
	if hasSnippets {
		returnTypes = fmt.Sprintf("([]*%s, []map[string]string, error)", mc.goTypeFqi)
	}
	g.P(fmt.Sprintf("func (s *Store) Search%s(ctx context.Context, %s%stsQuery, whereClause, paginationClause string, columns []string, params ...any) %s {",
		pluralGoName, parentParam, showDeletedParam, returnTypes))

	g.P("  if columns == nil {")
	if mc.hasJoins {
		g.P(fmt.Sprintf("    columns = %s", mc.writeColumns()))
	} else {
		g.P(fmt.Sprintf("    columns = %sPostgresColumns", mc.goType))
	}
	g.P("  }")
	g.P()

	if len(mc.parentBindings) > 0 {
		for _, binding := range mc.parentBindings {
			paramName := untitle(xstrings.ToCamelCase(binding.Variable))
			if binding.Shared {
				g.P(fmt.Sprintf("  if %sId != \"-\" && %sId != \"\" {", paramName, paramName))
				g.P(fmt.Sprintf("    whereClause = %s(whereClause, %s(\"%s%s = $%%d\", len(params) + 1))",
					mc.postgres("AddToWhereClause"), mc.fmtI("Sprintf"), colPrefix, binding.Column))
				g.P(fmt.Sprintf("    params = append(params, %sId)", paramName))
				g.P("  }")
				continue
			}
			// Non-shared parent identifiers: "" means the matched parent
			// pattern lacks this segment, so the row must not populate it;
			// "-" means any populated value.
			g.P(fmt.Sprintf("  if %sId == \"-\" {", paramName))
			g.P(fmt.Sprintf("    whereClause = %s(whereClause, \"%s%s IS NOT NULL\")", mc.postgres("AddToWhereClause"), colPrefix, binding.Column))
			g.P(fmt.Sprintf("  } else if %sId != \"\" {", paramName))
			g.P(fmt.Sprintf("    whereClause = %s(whereClause, %s(\"%s%s = $%%d\", len(params) + 1))",
				mc.postgres("AddToWhereClause"), mc.fmtI("Sprintf"), colPrefix, binding.Column))
			g.P(fmt.Sprintf("    params = append(params, %sId)", paramName))
			g.P("  } else {")
			g.P(fmt.Sprintf("    whereClause = %s(whereClause, \"%s%s IS NULL\")", mc.postgres("AddToWhereClause"), colPrefix, binding.Column))
			g.P("  }")
		}
		g.P()
	}

	if mc.hasDeleteTime {
		g.P("  if !showDeleted {")
		g.P(fmt.Sprintf("    whereClause = %s(whereClause, \"%sdelete_time IS NULL\")", mc.postgres("AddToWhereClause"), colPrefix))
		g.P("  }")
		g.P()
	}

	// The search predicate: relevance-ranked when a query is present, list
	// semantics otherwise.
	g.P(fmt.Sprintf("  orderByClause := \"ORDER BY %screate_time DESC\"", colPrefix))
	g.P("  if tsQuery != \"\" {")
	g.P(fmt.Sprintf("    whereClause = %s(whereClause, %s(\"%s%s @@ to_tsquery('simple', $%%d)\", len(params) + 1))",
		mc.postgres("AddToWhereClause"), mc.fmtI("Sprintf"), colPrefix, schema.SearchDocumentColumn))
	g.P(fmt.Sprintf("    orderByClause = %s(\"ORDER BY ts_rank(%s%s, to_tsquery('simple', $%%d)) DESC, %screate_time DESC\", len(params) + 1)",
		mc.fmtI("Sprintf"), colPrefix, schema.SearchDocumentColumn, colPrefix))
	if hasSnippets {
		for _, snippetField := range searchDoc.SnippetFields {
			g.P(fmt.Sprintf("    columns = append(columns, %s(`ts_headline('simple', %s, to_tsquery('simple', $%%d), '%s') AS __snippet_%s`, len(params) + 1))",
				mc.fmtI("Sprintf"), snippetField.Expression, headlineOptions, snippetIdent(snippetField.Path)))
		}
	}
	g.P("    params = append(params, tsQuery)")
	g.P("  }")
	g.P()

	if mc.hasJoins {
		g.P(fmt.Sprintf("  query := %s(\"SELECT %%s FROM %s \" + %sJoinClause + \" #where# #orderby# #pagination#\", \"#where#\", whereClause)",
			mc.stringsI("ReplaceAll"), mc.tableName, mc.goName))
		g.P(fmt.Sprintf("  query = %s(query, \"#orderby#\", orderByClause)", mc.stringsI("ReplaceAll")))
		g.P(fmt.Sprintf("  query = %s(query, \"#pagination#\", paginationClause)", mc.stringsI("ReplaceAll")))
		g.P(fmt.Sprintf("  query = %s(query, %s(columns, %q) + %sJoinSelectExprs)",
			mc.fmtI("Sprintf"), mc.postgres("QualifyColumns"), mc.bareTableName, mc.goName))
	} else {
		g.P(fmt.Sprintf("  query := %s(\"SELECT %%s FROM %s #where# #orderby# #pagination#\", \"#where#\", whereClause)",
			mc.stringsI("ReplaceAll"), mc.tableName))
		g.P(fmt.Sprintf("  query = %s(query, \"#orderby#\", orderByClause)", mc.stringsI("ReplaceAll")))
		g.P(fmt.Sprintf("  query = %s(query, \"#pagination#\", paginationClause)", mc.stringsI("ReplaceAll")))
		g.P(fmt.Sprintf("  query = %s(query, columns)", mc.postgres("SelectQuery")))
	}
	g.P()

	if hasSnippets {
		g.P(fmt.Sprintf("  var searchRows []*%s", rowType))
		g.P(fmt.Sprintf("  transactionFN := func(tx %s) error {", mc.postgres("Tx")))
		g.P("    searchRows = nil")
		g.P("    rows, err := tx.Query(ctx, query, params...)")
		g.P("    if err != nil {")
		g.P(fmt.Sprintf("      if err == %s {", mc.pgx("ErrNoRows")))
		g.P("        return nil")
		g.P("      }")
		g.P(fmt.Sprintf("      return %s(\"selecting %s: %%w\", err)", mc.fmtI("Errorf"), pluralUntitled))
		g.P("    }")
		g.P(fmt.Sprintf("    searchRows, err = %s(rows, %s[%s])", mc.pgx("CollectRows"), mc.pgx("RowToAddrOfStructByNameLax"), rowType))
		g.P("    if err != nil {")
		g.P(fmt.Sprintf("      return %s(\"collecting rows: %%w\", err)", mc.fmtI("Errorf")))
		g.P("    }")
		g.P("    return nil")
		g.P("  }")
		g.P(fmt.Sprintf("  if err := s.client.ExecuteTransaction(ctx, %s, transactionFN); err != nil {", mc.postgres("RepeatableRead")))
		g.P("    return nil, nil, err")
		g.P("  }")
		g.P(fmt.Sprintf("  %s := make([]*%s, 0, len(searchRows))", pluralUntitled, mc.goTypeFqi))
		g.P("  snippets := make([]map[string]string, 0, len(searchRows))")
		g.P("  for _, searchRow := range searchRows {")
		g.P(fmt.Sprintf("    row := searchRow.%s", mc.goType))
		g.P(fmt.Sprintf("    %s = append(%s, &row)", pluralUntitled, pluralUntitled))
		g.P("    snippet := map[string]string{}")
		for _, snippetField := range searchDoc.SnippetFields {
			goName := xstrings.ToPascalCase(snippetIdent(snippetField.Path))
			// Only surface fragments that actually contain a highlighted match.
			g.P(fmt.Sprintf("    if searchRow.Snippet%s != nil && %s(*searchRow.Snippet%s, \"**\") {",
				goName, mc.stringsI("Contains"), goName))
			g.P(fmt.Sprintf("      snippet[%q] = *searchRow.Snippet%s", snippetField.Path, goName))
			g.P("    }")
		}
		g.P("    snippets = append(snippets, snippet)")
		g.P("  }")
		g.P(fmt.Sprintf("  return %s, snippets, nil", pluralUntitled))
		g.P("}")
		g.P()
		return
	}

	g.P(fmt.Sprintf("  var %s []*%s", pluralUntitled, mc.goTypeFqi))
	g.P(fmt.Sprintf("  transactionFN := func(tx %s) error {", mc.postgres("Tx")))
	g.P(fmt.Sprintf("    %s = nil", pluralUntitled))
	g.P("    rows, err := tx.Query(ctx, query, params...)")
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      if err == %s {", mc.pgx("ErrNoRows")))
	g.P("        return nil")
	g.P("      }")
	g.P(fmt.Sprintf("      return %s(\"selecting %s: %%w\", err)", mc.fmtI("Errorf"), pluralUntitled))
	g.P("    }")
	g.P(fmt.Sprintf("    %s, err = %s(rows, %s[%s])", pluralUntitled, mc.pgx("CollectRows"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("    if err != nil {")
	g.P(fmt.Sprintf("      return %s(\"collecting rows: %%w\", err)", mc.fmtI("Errorf")))
	g.P("    }")
	g.P("    return nil")
	g.P("  }")
	g.P(fmt.Sprintf("  return %s, s.client.ExecuteTransaction(ctx, %s, transactionFN)", pluralUntitled, mc.postgres("RepeatableRead")))
	g.P("}")
	g.P()
}

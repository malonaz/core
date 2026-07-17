package postgres

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"

	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

func (mc *msgCtx) generateBatchGet() {
	if mc.multiPattern {
		mc.generateMultiPatternBatchGet()
		return
	}

	g := mc.g
	bindings := mc.columnBindings

	params := make([]string, len(bindings))
	for i, binding := range bindings {
		params[i] = untitle(xstrings.ToCamelCase(binding.Variable)) + "Ids []string"
	}

	g.P(fmt.Sprintf("func (s *Store) BatchGet%s(ctx context.Context, %s) ([]*%s, error) {",
		mc.pr.PluralGoName(), strings.Join(params, ", "), mc.goTypeFqi))

	firstName := untitle(xstrings.ToCamelCase(bindings[0].Variable)) + "Ids"
	g.P(fmt.Sprintf("  n := len(%s)", firstName))
	for _, binding := range bindings[1:] {
		name := untitle(xstrings.ToCamelCase(binding.Variable)) + "Ids"
		g.P(fmt.Sprintf("  if len(%s) != n {", name))
		g.P(fmt.Sprintf("    return nil, %s(\"mismatched slice lengths\")", mc.fmtI("Errorf")))
		g.P("  }")
	}

	g.P("  if n == 0 {")
	g.P("    return nil, nil")
	g.P("  }")
	g.P()

	g.P("  orClauses := make([]string, n)")
	g.P(fmt.Sprintf("  params := make([]any, 0, n*%d)", len(bindings)))
	g.P("  for i := 0; i < n; i++ {")
	g.P(fmt.Sprintf("    base := i*%d", len(bindings)))
	g.P(fmt.Sprintf("    conditions := make([]string, %d)", len(bindings)))

	colPrefix := ""
	if mc.hasJoins {
		colPrefix = mc.bareTableName + "."
	}

	for idx, binding := range bindings {
		g.P(fmt.Sprintf("    conditions[%d] = %s(\"%s%s = $%%d\", base+%d)",
			idx, mc.fmtI("Sprintf"), colPrefix, binding.Column, idx+1))
	}

	for _, binding := range bindings {
		paramName := untitle(xstrings.ToCamelCase(binding.Variable)) + "Ids"
		g.P(fmt.Sprintf("    params = append(params, %s[i])", paramName))
	}

	g.P(fmt.Sprintf("    orClauses[i] = \"(\" + %s(conditions, \" AND \") + \")\"", mc.stringsI("Join")))
	g.P("  }")
	g.P(fmt.Sprintf("  whereClause := \"WHERE \" + %s(orClauses, \" OR \")", mc.stringsI("Join")))
	g.P()

	if mc.hasJoins {
		g.P(fmt.Sprintf("  query := %s(\"SELECT %%s FROM %s \" + %sJoinClause + \" %%s\", %s(%s, %q) + %sJoinSelectExprs, whereClause)",
			mc.fmtI("Sprintf"), mc.tableName, mc.goName, mc.postgres("QualifyColumns"), mc.writeColumns(), mc.bareTableName, mc.goName))
	} else {
		g.P(fmt.Sprintf("  query := %s(\"SELECT %%s FROM %s %%s\", %s(\"%%s\", %sPostgresColumns), whereClause)",
			mc.fmtI("Sprintf"), mc.tableName, mc.postgres("SelectQuery"), mc.goType))
	}
	g.P()

	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(\"batch getting %s: %%w\", err)", mc.fmtI("Errorf"), mc.goName))
	g.P("  }")
	g.P(fmt.Sprintf("  return %s(rows, %s[%s])", mc.pgx("CollectRows"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("}")
	g.P()
}

// generateMultiPatternBatchGet builds per-row conditions at runtime, matching
// unset pattern-specific identifiers against NULL.
func (mc *msgCtx) generateMultiPatternBatchGet() {
	g := mc.g
	bindings := mc.columnBindings

	params := make([]string, len(bindings))
	for i, binding := range bindings {
		params[i] = untitle(xstrings.ToCamelCase(binding.Variable)) + "Ids []string"
	}

	g.P(fmt.Sprintf("func (s *Store) BatchGet%s(ctx context.Context, %s) ([]*%s, error) {",
		mc.pr.PluralGoName(), strings.Join(params, ", "), mc.goTypeFqi))

	firstName := untitle(xstrings.ToCamelCase(bindings[0].Variable)) + "Ids"
	g.P(fmt.Sprintf("  n := len(%s)", firstName))
	for _, binding := range bindings[1:] {
		name := untitle(xstrings.ToCamelCase(binding.Variable)) + "Ids"
		g.P(fmt.Sprintf("  if len(%s) != n {", name))
		g.P(fmt.Sprintf("    return nil, %s(\"mismatched slice lengths\")", mc.fmtI("Errorf")))
		g.P("  }")
	}

	g.P("  if n == 0 {")
	g.P("    return nil, nil")
	g.P("  }")
	g.P()

	g.P("  orClauses := make([]string, n)")
	g.P(fmt.Sprintf("  params := make([]any, 0, n*%d)", len(bindings)))
	g.P("  for i := 0; i < n; i++ {")
	g.P(fmt.Sprintf("    conditions := make([]string, 0, %d)", len(bindings)))
	mc.emitIDConditionAppends("    ", func(binding schema.ColumnBinding) string {
		return untitle(xstrings.ToCamelCase(binding.Variable)) + "Ids[i]"
	})
	g.P(fmt.Sprintf("    orClauses[i] = \"(\" + %s(conditions, \" AND \") + \")\"", mc.stringsI("Join")))
	g.P("  }")
	g.P(fmt.Sprintf("  whereClause := \"WHERE \" + %s(orClauses, \" OR \")", mc.stringsI("Join")))
	g.P()

	g.P(fmt.Sprintf("  query := %s(\"SELECT %%s FROM %s %%s\", %s(\"%%s\", %sPostgresColumns), whereClause)",
		mc.fmtI("Sprintf"), mc.tableName, mc.postgres("SelectQuery"), mc.goType))
	g.P()

	g.P("  rows, err := s.client.Query(ctx, query, params...)")
	g.P("  if err != nil {")
	g.P(fmt.Sprintf("    return nil, %s(\"batch getting %s: %%w\", err)", mc.fmtI("Errorf"), mc.goName))
	g.P("  }")
	g.P(fmt.Sprintf("  return %s(rows, %s[%s])", mc.pgx("CollectRows"), mc.pgx("RowToAddrOfStructByNameLax"), mc.goTypeFqi))
	g.P("}")
	g.P()
}

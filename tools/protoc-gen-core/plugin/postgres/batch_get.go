package postgres

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
)

func (mc *msgCtx) generateBatchGet() {
	g := mc.g
	patternVars := mc.pr.PatternVariables
	lastIdx := len(patternVars) - 1

	var params []string
	for _, v := range patternVars {
		params = append(params, untitle(xstrings.ToCamelCase(v))+"Ids []string")
	}
	paramList := strings.Join(params, ", ")

	g.P(fmt.Sprintf("func (s *Store) BatchGet%s(ctx context.Context, %s) ([]*%s, error) {",
		mc.pr.PluralGoName(), paramList, mc.goTypeFqi))

	firstName := untitle(xstrings.ToCamelCase(patternVars[0])) + "Ids"
	g.P(fmt.Sprintf("  n := len(%s)", firstName))
	for _, v := range patternVars[1:] {
		name := untitle(xstrings.ToCamelCase(v)) + "Ids"
		g.P(fmt.Sprintf("  if len(%s) != n {", name))
		g.P(fmt.Sprintf("    return nil, %s(\"mismatched slice lengths\")", mc.fmtI("Errorf")))
		g.P("  }")
	}

	g.P("  if n == 0 {")
	g.P("    return nil, nil")
	g.P("  }")
	g.P()

	g.P("  orClauses := make([]string, n)")
	g.P(fmt.Sprintf("  params := make([]any, 0, n*%d)", len(patternVars)))
	g.P("  for i := 0; i < n; i++ {")
	g.P(fmt.Sprintf("    base := i*%d", len(patternVars)))
	g.P(fmt.Sprintf("    conditions := make([]string, %d)", len(patternVars)))

	colPrefix := ""
	if mc.hasJoins {
		colPrefix = mc.bareTableName + "."
	}

	for idx, v := range patternVars {
		col := v + "_id"
		if idx == lastIdx && mc.modelOpts.GetIdColumnName() != "" {
			col = mc.modelOpts.GetIdColumnName()
		}
		g.P(fmt.Sprintf("    conditions[%d] = %s(\"%s%s = $%%d\", base+%d)",
			idx, mc.fmtI("Sprintf"), colPrefix, col, idx+1))
	}

	for _, v := range patternVars {
		paramName := untitle(xstrings.ToCamelCase(v)) + "Ids"
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

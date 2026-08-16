package aip

import (
	"strings"
	"unicode"
)

// maxSearchTokens caps the number of tokens fed into a tsquery, keeping
// adversarial queries cheap for the database to evaluate.
const maxSearchTokens = 8

// BuildPrefixTSQuery normalizes a free-text query into a postgres tsquery
// expression (to be passed as a parameter to to_tsquery('simple', $n)).
// Tokens are lowercased, split on any non-alphanumeric rune — which also
// decomposes emails and phone numbers the way search documents index them —
// and AND-ed together with prefix matching, e.g. "John Smi" -> "john:* & smi:*".
// Returns "" when the query holds no token.
func BuildPrefixTSQuery(query string) string {
	tokens := strings.FieldsFunc(strings.ToLower(query), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	if len(tokens) > maxSearchTokens {
		tokens = tokens[:maxSearchTokens]
	}
	parts := make([]string, 0, len(tokens))
	for _, token := range tokens {
		parts = append(parts, token+":*")
	}
	return strings.Join(parts, " & ")
}

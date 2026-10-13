// Package goident derives Go identifiers for generated code from proto names.
package goident

import (
	"go/token"

	"github.com/huandu/xstrings"
)

// Var returns the lowerCamelCase variable name for a proto name ("case_id" → "caseId", "Shelf" →
// "shelf"). A name that is a Go keyword gets a trailing underscore ("Case" → "case_"), so a
// resource may be named after one.
func Var(name string) string {
	variable := xstrings.ToCamelCase(name)
	if token.IsKeyword(variable) {
		return variable + "_"
	}
	return variable
}

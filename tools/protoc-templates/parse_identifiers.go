package main

import (
	"strings"
)

// ParseIdentifierNames extracts identifier names from a resource name pattern.
// Example 1 [topline collection]: pattern="users/{user}"
//
//	=> returns ["user"]
//
// Example 2 [nested collection]: pattern="users/{user}/contacts/{contact}"
//
//	=> returns ["user", "contact"]
//
// Example 3 [singleton resource]: pattern="users/{user}/config"
//
//	=> returns ["user"]
//
// Example 4 [child of a nested singleton resource]: pattern="users/{user}/config/files/{file}"
//
//	=> returns ["user"]
//
// Example 5 [no variables]: pattern="publishers"
//
//	=> returns []
func parseIdentifiersFromPattern(pattern string) []string {
	var names []string
	inBrace := false
	var currentVar strings.Builder

	for _, ch := range pattern {
		if ch == '{' {
			inBrace = true
			currentVar.Reset()
		} else if ch == '}' {
			if inBrace {
				names = append(names, currentVar.String())
			}
			inBrace = false
		} else if inBrace {
			currentVar.WriteRune(ch)
		}
	}

	return names
}

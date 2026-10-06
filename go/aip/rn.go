package aip

import (
	"encoding"
	"fmt"
	"strings"

	"go.einride.tech/aip/resourcename"
)

// Wildcard is the segment standing for "any" in a resource name, e.g. "organizations/-".
const Wildcard = resourcename.Wildcard

// Rn is a typed resource name, as generated for every resource declared in a proto
// package (`{Type}Rn`). Multi-pattern resources generate an interface of this name
// implemented by one struct per pattern.
type Rn interface {
	fmt.Stringer
	encoding.TextMarshaler
	// Validate reports whether every segment is set and free of '/'.
	Validate() error
	// ContainsWildcard reports whether any segment is the Wildcard.
	ContainsWildcard() bool
	// Type is the resource type, e.g. "library.com/Book".
	Type() string
	// Pattern is the pattern this name follows, e.g. "shelves/{shelf}/books/{book}".
	Pattern() string
	// ID is the resource's own identifier (the last variable); "" for singletons.
	ID() string
	// Parent is the parent resource name; "" at the root.
	Parent() string
}

// ParentName is the parent of a resource name: the name minus its trailing
// collection and ID segments; "" at the root.
func ParentName(name string) string {
	index := strings.LastIndex(name, "/")
	if index < 0 {
		return ""
	}
	index = strings.LastIndex(name[:index], "/")
	if index < 0 {
		return ""
	}
	return name[:index]
}

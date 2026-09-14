package aip

import (
	"fmt"

	"go.einride.tech/aip/resourcename"
	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
)

type ParsedResource struct {
	Desc *annotationspb.ResourceDescriptor
	Type string
	// Pattern is the first declared pattern.
	Pattern string
	// Patterns holds all declared patterns.
	Patterns  []string
	Singular  string
	Plural    string
	Singleton bool
	HasParent bool
	// PatternVariables is the union of the variables across all patterns,
	// ordered by first appearance, with the resource's own identifier last.
	PatternVariables []string
}

// parsedPattern is the scan result of a single resource name pattern.
type parsedPattern struct {
	value     string
	variables []string
	singleton bool
}

// ParseResource parses a resource descriptor into a structured ParsedResource.
// It validates the descriptor and extracts the union of pattern variables
// across all declared patterns. Multi-pattern singleton resources are not
// supported.
func ParseResource(resourceDescriptor *annotationspb.ResourceDescriptor) (*ParsedResource, error) {
	if resourceDescriptor == nil {
		return nil, fmt.Errorf("resource descriptor is nil")
	}

	if resourceDescriptor.Singular == "" {
		return nil, fmt.Errorf("resource descriptor %s must define `singular`", resourceDescriptor.Type)
	}
	if resourceDescriptor.Plural == "" {
		return nil, fmt.Errorf("resource descriptor %s must define `plural`", resourceDescriptor.Type)
	}

	t := resourceDescriptor.GetType()
	if t == "" {
		return nil, fmt.Errorf("no resource type")
	}

	if len(resourceDescriptor.Pattern) == 0 {
		return nil, fmt.Errorf("resource descriptor %s declares no patterns", t)
	}

	// Scan every pattern. A pattern is a singleton iff it ends in a literal
	// segment (e.g. ".../activity"); the variable name does not need to match
	// the singular (e.g. {revision} on singular "quoteRevision").
	patterns := make([]*parsedPattern, 0, len(resourceDescriptor.Pattern))
	for _, pattern := range resourceDescriptor.Pattern {
		var sc resourcename.Scanner
		var variables []string
		lastSegmentIsVariable := false
		sc.Init(pattern)
		for sc.Scan() {
			lastSegmentIsVariable = sc.Segment().IsVariable()
			if sc.Segment().IsVariable() {
				variables = append(variables, string(sc.Segment().Literal()))
			}
		}
		patterns = append(patterns, &parsedPattern{
			value:     pattern,
			variables: variables,
			singleton: !lastSegmentIsVariable,
		})
	}

	if len(patterns) == 1 {
		pattern := patterns[0]
		// Parents are all variables except the resource's own (the last one, when not a singleton).
		hasParent := len(pattern.variables) > 0
		if !pattern.singleton {
			hasParent = len(pattern.variables) > 1
		}
		return &ParsedResource{
			Desc:             resourceDescriptor,
			Type:             t,
			Pattern:          pattern.value,
			Patterns:         []string{pattern.value},
			Singular:         resourceDescriptor.Singular,
			Plural:           resourceDescriptor.Plural,
			Singleton:        pattern.singleton,
			HasParent:        hasParent,
			PatternVariables: pattern.variables,
		}, nil
	}

	// Multi-pattern: all patterns must be non-singleton and end on the same
	// identifier variable. The union of variables is ordered by first
	// appearance, with the resource's own identifier forced last.
	for _, pattern := range patterns {
		if pattern.singleton {
			return nil, fmt.Errorf("multi-pattern singleton resource %s is not supported", t)
		}
		if len(pattern.variables) == 0 {
			return nil, fmt.Errorf("pattern %q of resource %s has no variables", pattern.value, t)
		}
	}
	ownID := patterns[0].variables[len(patterns[0].variables)-1]
	seen := map[string]bool{}
	var unionVariables []string
	patternValues := make([]string, len(patterns))
	for i, pattern := range patterns {
		patternValues[i] = pattern.value
		lastVariable := pattern.variables[len(pattern.variables)-1]
		if lastVariable != ownID {
			return nil, fmt.Errorf("patterns of resource %s disagree on the identifier variable: %q vs %q", t, ownID, lastVariable)
		}
		for _, variable := range pattern.variables {
			if seen[variable] || variable == ownID {
				continue
			}
			seen[variable] = true
			unionVariables = append(unionVariables, variable)
		}
	}
	unionVariables = append(unionVariables, ownID)

	return &ParsedResource{
		Desc:             resourceDescriptor,
		Type:             t,
		Pattern:          patternValues[0],
		Patterns:         patternValues,
		Singular:         resourceDescriptor.Singular,
		Plural:           resourceDescriptor.Plural,
		Singleton:        false,
		HasParent:        len(unionVariables) > 1,
		PatternVariables: unionVariables,
	}, nil
}

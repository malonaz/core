package aip

import (
	"fmt"

	"go.einride.tech/aip/resourcename"
	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
)

type ParsedResource struct {
	Desc             *annotationspb.ResourceDescriptor
	Type             string
	Pattern          string
	Singular         string
	Plural           string
	Singleton        bool
	HasParent        bool
	PatternVariables []string
}

// ParseResource parses a resource descriptor into a structured ParsedResource.
// It validates the descriptor, extracts pattern variables, and recursively
// resolves parent and child relationships.
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

	// Parse the pattern.
	if len(resourceDescriptor.Pattern) != 1 {
		return nil, fmt.Errorf("we only support resources with single patterns")
	}
	pattern := resourceDescriptor.Pattern[0]

	// Extract pattern variables. A resource is a singleton iff its pattern ends
	// in a literal segment (e.g. ".../activity"); the variable name does not
	// need to match the singular (e.g. {revision} on singular "quoteRevision").
	var sc resourcename.Scanner
	var patternVariables []string
	lastSegmentIsVariable := false
	sc.Init(pattern)
	for sc.Scan() {
		lastSegmentIsVariable = sc.Segment().IsVariable()
		if sc.Segment().IsVariable() {
			patternVariables = append(patternVariables, string(sc.Segment().Literal()))
		}
	}
	singleton := !lastSegmentIsVariable
	// Parents are all variables except the resource's own (the last one, when not a singleton).
	hasParent := len(patternVariables) > 0
	if !singleton {
		hasParent = len(patternVariables) > 1
	}

	parsedResource := &ParsedResource{
		Desc:             resourceDescriptor,
		Type:             t,
		Pattern:          pattern,
		Singular:         resourceDescriptor.Singular,
		Plural:           resourceDescriptor.Plural,
		Singleton:        singleton,
		HasParent:        hasParent,
		PatternVariables: patternVariables,
	}

	return parsedResource, nil
}

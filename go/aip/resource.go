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

	// Extract pattern variables.
	var sc resourcename.Scanner
	singleton := true
	hasParent := false
	var patternVariables []string
	sc.Init(pattern)
	for sc.Scan() {
		if sc.Segment().IsVariable() {
			patternVariable := string(sc.Segment().Literal())
			patternVariables = append(patternVariables, patternVariable)
			if patternVariable == resourceDescriptor.Singular {
				singleton = false
			} else {
				hasParent = true
			}
		}
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

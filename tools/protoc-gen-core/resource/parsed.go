package resource

import (
	"fmt"
	"sort"
	"strings"

	"github.com/huandu/xstrings"
	"go.einride.tech/aip/reflect/aipreflect"
	"go.einride.tech/aip/resourcename"
	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/internal/strs"
)

// ParsedPattern is one resource name pattern of a resource, with its variables
// and its position in the resource hierarchy resolved.
type ParsedPattern struct {
	// Resource is the resource this pattern belongs to.
	Resource *ParsedResource
	// Value is the raw pattern, e.g. "organizations/{organization}/shelves/{shelf}".
	Value string
	// Variables are the snake_case pattern variables, e.g. ["organization", "shelf"].
	Variables []string
	// Singleton is true when the pattern ends on a literal segment.
	Singleton bool
	// Parent is the parent resource's pattern prefixing this one; nil at the root.
	Parent *ParsedPattern
}

// VariableID returns the pattern's own identifier variable, e.g. "shelfId".
// Singletons have no identifier of their own.
func (p *ParsedPattern) VariableID(goStyle bool) (string, error) {
	if p.Singleton {
		return "", fmt.Errorf("singleton pattern %q has no identifier variable", p.Value)
	}
	val := p.Resource.Desc.Singular + "Id"
	if goStyle {
		return val, nil
	}
	return xstrings.ToSnakeCase(val), nil
}

// VariableIDs returns the comma-separated identifier variables of the pattern.
func (p *ParsedPattern) VariableIDs(goStyle bool) string {
	vals := make([]string, 0, len(p.Variables))
	for _, v := range p.Variables {
		val := v + "_id"
		if goStyle {
			val = xstrings.ToCamelCase(val)
		}
		vals = append(vals, val)
	}
	return strings.Join(vals, ", ")
}

// VariableIDPtrs returns the comma-separated addresses of the pattern's identifier variables.
func (p *ParsedPattern) VariableIDPtrs() string {
	vals := make([]string, 0, len(p.Variables))
	for _, v := range p.Variables {
		vals = append(vals, "&"+xstrings.ToCamelCase(v+"_id"))
	}
	return strings.Join(vals, ", ")
}

// ParsedResource is a fully resolved resource with its patterns and children.
type ParsedResource struct {
	Desc     *annotationspb.ResourceDescriptor
	Type     string
	Patterns []*ParsedPattern
	Children []*ParsedResource
}

// SinglePattern returns the resource's only pattern. Generators that do not
// yet support multi-pattern resources use this as an explicit guard.
func (pr *ParsedResource) SinglePattern() (*ParsedPattern, error) {
	if len(pr.Patterns) != 1 {
		return nil, fmt.Errorf("resource %s has %d patterns; multi-pattern resources are not supported here", pr.Desc.Type, len(pr.Patterns))
	}
	return pr.Patterns[0], nil
}

func (pr *ParsedResource) patternByValue(value string) *ParsedPattern {
	for _, pattern := range pr.Patterns {
		if pattern.Value == value {
			return pattern
		}
	}
	return nil
}

func (pr *ParsedResource) SingularGoName() string {
	return strs.GoCamelCase(pr.Desc.Singular)
}

func (pr *ParsedResource) PluralGoName() string {
	return strs.GoCamelCase(pr.Desc.Plural)
}

func (pr *ParsedResource) SingularSnakeCase() string {
	return strs.JSONSnakeCase(pr.Desc.Singular)
}

// Parse recursively resolves a resource descriptor into a ParsedResource.
func Parse(reg *Registry, resourceDescriptor *annotationspb.ResourceDescriptor) (*ParsedResource, error) {
	if val, ok := reg.ParsedResourceTypeToParsedResource[resourceDescriptor.Type]; ok {
		return val, nil
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
		return nil, fmt.Errorf("resource descriptor %s declares no patterns", resourceDescriptor.Type)
	}

	parsedResource := &ParsedResource{
		Desc: resourceDescriptor,
		Type: aipreflect.ResourceType(t).Type(),
	}
	// Memoize before recursing: parents and children may refer back to this resource.
	reg.ParsedResourceTypeToParsedResource[resourceDescriptor.Type] = parsedResource

	for _, pattern := range resourceDescriptor.Pattern {
		parsedPattern, err := parsePattern(reg, parsedResource, pattern)
		if err != nil {
			return nil, fmt.Errorf("parsing pattern %q of resource %s: %w", pattern, resourceDescriptor.Type, err)
		}
		parsedResource.Patterns = append(parsedResource.Patterns, parsedPattern)
	}

	for childResourceType := range reg.ResourceTypeToChildResourceTypeSet[resourceDescriptor.Type] {
		childResourceDescriptor, ok := reg.ResourceTypeToResourceDescriptor[childResourceType]
		if !ok {
			return nil, fmt.Errorf("resource descriptor %s not found", childResourceType)
		}
		child, err := Parse(reg, childResourceDescriptor)
		if err != nil {
			return nil, fmt.Errorf("parsing (child) resource descriptor %s: %w", childResourceType, err)
		}
		parsedResource.Children = append(parsedResource.Children, child)
	}

	sort.Slice(parsedResource.Children, func(i, j int) bool {
		return parsedResource.Children[i].Type < parsedResource.Children[j].Type
	})
	return parsedResource, nil
}

// parsePattern resolves a single pattern of a resource, linking it to the
// parent resource's pattern that prefixes it.
func parsePattern(reg *Registry, parsedResource *ParsedResource, pattern string) (*ParsedPattern, error) {
	variables, variableEnds, singleton, err := scanPattern(reg, pattern)
	if err != nil {
		return nil, err
	}

	parsedPattern := &ParsedPattern{
		Resource:  parsedResource,
		Value:     pattern,
		Variables: variables,
		Singleton: singleton,
	}

	parentEnd, hasParent := parentPatternEnd(variableEnds, singleton)
	if !hasParent {
		return parsedPattern, nil
	}

	parentPatternValue := pattern[:parentEnd]
	parentDescriptor, ok := reg.ResourcePatternToResourceDescriptor[parentPatternValue]
	if !ok {
		return nil, fmt.Errorf("no resource descriptor found for parent pattern %q", parentPatternValue)
	}
	parentResource, err := Parse(reg, parentDescriptor)
	if err != nil {
		return nil, fmt.Errorf("parsing (parent) resource descriptor %s: %w", parentDescriptor.Type, err)
	}
	parentPattern := parentResource.patternByValue(parentPatternValue)
	if parentPattern == nil {
		return nil, fmt.Errorf("parent resource %s does not declare pattern %q", parentDescriptor.Type, parentPatternValue)
	}
	parsedPattern.Parent = parentPattern
	return parsedPattern, nil
}

// scanPattern resolves a pattern's variables from the registered sub-pattern
// descriptors, alongside each variable's end offset within the pattern and
// whether the pattern is a singleton (i.e. it ends on a literal segment).
func scanPattern(reg *Registry, pattern string) (variables []string, variableEnds []int, singleton bool, err error) {
	var sc resourcename.Scanner
	sc.Init(pattern)
	lastSegmentIsVariable := false
	for sc.Scan() {
		if !sc.Segment().IsVariable() {
			lastSegmentIsVariable = false
			continue
		}
		subPattern := pattern[:sc.End()]
		rd, ok := reg.ResourcePatternToResourceDescriptor[subPattern]
		if !ok {
			return nil, nil, false, fmt.Errorf("no resource descriptor found for sub-pattern %q in pattern %q", subPattern, pattern)
		}
		variables = append(variables, xstrings.ToSnakeCase(rd.Singular))
		variableEnds = append(variableEnds, sc.End())
		lastSegmentIsVariable = true
	}
	return variables, variableEnds, !lastSegmentIsVariable, nil
}

// parentPatternEnd returns the end offset of the pattern prefix identifying
// the immediate parent, if any. A singleton's parent pattern ends at the
// singleton's final variable; a collection resource's parent pattern ends at
// its second-to-last variable.
func parentPatternEnd(variableEnds []int, singleton bool) (int, bool) {
	if singleton {
		if len(variableEnds) == 0 {
			return 0, false
		}
		return variableEnds[len(variableEnds)-1], true
	}
	if len(variableEnds) < 2 {
		return 0, false
	}
	return variableEnds[len(variableEnds)-2], true
}

// ParseFromMessage resolves a ParsedResource from a protogen message.
func ParseFromMessage(message *protogen.Message) (*ParsedResource, error) {
	pkg := message.Desc.ParentFile().Package()
	reg := GetOrCreateRegistry(pkg)
	messageType := string(message.Desc.FullName())
	resourceDescriptor, ok := reg.MessageTypeToResourceDescriptor[messageType]
	if !ok {
		return nil, fmt.Errorf("no resource descriptor found for message type %s", messageType)
	}
	return Parse(reg, resourceDescriptor)
}

// GetMessageByResourceType finds a message across all registries by resource type.
func GetMessageByResourceType(resourceType string) (*protogen.Message, error) {
	for _, registry := range PackageToRegistry {
		message, ok := registry.ResourceTypeToMessage[resourceType]
		if ok {
			return message, nil
		}
	}
	return nil, fmt.Errorf("could not find message %q", resourceType)
}

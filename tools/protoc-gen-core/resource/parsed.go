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

// ParsedResource is a fully resolved resource with its hierarchy and pattern variables.
type ParsedResource struct {
	Desc             *annotationspb.ResourceDescriptor
	Type             string
	Pattern          string
	Singleton        bool
	PatternVariables []string

	Parent   *ParsedResource
	Children []*ParsedResource
}

func (pr *ParsedResource) PatternVariable() (string, error) {
	if pr.Singleton {
		return "", fmt.Errorf("called PatternVariable on singleton resource")
	}
	return pr.Desc.Singular, nil
}

func (pr *ParsedResource) PatternVariableID(goStyle bool) (string, error) {
	val, err := pr.PatternVariable()
	if err != nil {
		return "", err
	}
	val += "Id"
	if goStyle {
		return val, nil
	}
	return xstrings.ToSnakeCase(val), nil
}

func (pr *ParsedResource) PatternVariableIDs(goStyle bool) string {
	vals := make([]string, 0, len(pr.PatternVariables))
	for _, v := range pr.PatternVariables {
		val := v + "_id"
		if goStyle {
			val = xstrings.ToCamelCase(val)
		}
		vals = append(vals, val)
	}
	return strings.Join(vals, ", ")
}

func (pr *ParsedResource) PatternVariableIDPtrs() string {
	vals := make([]string, 0, len(pr.PatternVariables))
	for _, v := range pr.PatternVariables {
		val := "&" + xstrings.ToCamelCase(v+"_id")
		vals = append(vals, val)
	}
	return strings.Join(vals, ", ")
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
	resourceType := aipreflect.ResourceType(t).Type()

	if len(resourceDescriptor.Pattern) != 1 {
		return nil, fmt.Errorf("we only support resources with single patterns")
	}
	pattern := resourceDescriptor.Pattern[0]

	var sc resourcename.Scanner
	var lastSegmentIsVariable bool
	var patternVariables []string
	sc.Init(pattern)
	for sc.Scan() {
		if sc.Segment().IsVariable() {
			subPattern := pattern[:sc.End()]
			rd, ok := reg.ResourcePatternToResourceDescriptor[subPattern]
			if !ok {
				return nil, fmt.Errorf("no resource descriptor found for sub-pattern %q in pattern %q", subPattern, pattern)
			}
			patternVariables = append(patternVariables, xstrings.ToSnakeCase(rd.Singular))
			lastSegmentIsVariable = true
		} else {
			lastSegmentIsVariable = false
		}
	}
	singleton := !lastSegmentIsVariable
	hasParent := len(patternVariables) > 1 || (singleton && len(patternVariables) > 0)

	parsedResource := &ParsedResource{
		Desc:             resourceDescriptor,
		Type:             resourceType,
		Pattern:          pattern,
		Singleton:        singleton,
		PatternVariables: patternVariables,
	}
	reg.ParsedResourceTypeToParsedResource[resourceDescriptor.Type] = parsedResource

	if hasParent {
		parentResourceType, ok := reg.ResourceTypeToParentResourceType[resourceDescriptor.Type]
		if !ok {
			return nil, fmt.Errorf("could not find %s's parent resource type", resourceDescriptor.Type)
		}
		parentResourceDescriptor, ok := reg.ResourceTypeToResourceDescriptor[parentResourceType]
		if !ok {
			return nil, fmt.Errorf("resource descriptor %s not found", parentResourceType)
		}
		var err error
		parsedResource.Parent, err = Parse(reg, parentResourceDescriptor)
		if err != nil {
			return nil, fmt.Errorf("parsing (parent) resource descriptor %s: %v", parentResourceType, err)
		}
	}

	for childResourceType := range reg.ResourceTypeToChildResourceTypeSet[resourceDescriptor.Type] {
		childResourceDescriptor, ok := reg.ResourceTypeToResourceDescriptor[childResourceType]
		if !ok {
			return nil, fmt.Errorf("resource descriptor %s not found", childResourceType)
		}
		child, err := Parse(reg, childResourceDescriptor)
		if err != nil {
			return nil, fmt.Errorf("parsing (child) resource descriptor %s: %v", childResourceType, err)
		}
		parsedResource.Children = append(parsedResource.Children, child)
	}

	sort.Slice(parsedResource.Children, func(i, j int) bool {
		return parsedResource.Children[i].Type < parsedResource.Children[j].Type
	})
	return parsedResource, nil
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

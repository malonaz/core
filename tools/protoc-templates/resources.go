package main

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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
)

// packageRegistry holds all resource mappings scoped to a single proto package,
// preventing resource type/pattern collisions across packages.
type packageRegistry struct {
	messageTypeToResourceDescriptor     map[string]*annotationspb.ResourceDescriptor
	resourceTypeToResourceDescriptor    map[string]*annotationspb.ResourceDescriptor
	resourcePatternToResourceDescriptor map[string]*annotationspb.ResourceDescriptor
	resourceTypeToParentResourceType    map[string]string
	resourceTypeToChildResourceTypeSet  map[string]map[string]struct{}
	resourceTypeToMessage               map[string]*protogen.Message
	parsedResourceTypeToParsedResource  map[string]*ParsedResource
}

func newPackageRegistry() *packageRegistry {
	return &packageRegistry{
		messageTypeToResourceDescriptor:     map[string]*annotationspb.ResourceDescriptor{},
		resourceTypeToResourceDescriptor:    map[string]*annotationspb.ResourceDescriptor{},
		resourcePatternToResourceDescriptor: map[string]*annotationspb.ResourceDescriptor{},
		resourceTypeToParentResourceType:    map[string]string{},
		resourceTypeToChildResourceTypeSet:  map[string]map[string]struct{}{},
		resourceTypeToMessage:               map[string]*protogen.Message{},
		parsedResourceTypeToParsedResource:  map[string]*ParsedResource{},
	}
}

// packageToRegistry maps each proto package to its isolated registry.
var packageToRegistry = map[protoreflect.FullName]*packageRegistry{}

func getOrCreateRegistry(pkg protoreflect.FullName) *packageRegistry {
	if reg, ok := packageToRegistry[pkg]; ok {
		return reg
	}
	reg := newPackageRegistry()
	packageToRegistry[pkg] = reg
	return reg
}

// registerAnnotations scans all files and populates per-package registries with
// resource descriptors from both file-level resource_definition annotations and
// message-level resource annotations.
func registerAnnotations(files []*protogen.File) error {
	// Track pattern origins per package to detect conflicting registrations.
	type patternOrigin struct {
		descriptor *annotationspb.ResourceDescriptor
		filePath   string
	}
	packagePatternToOrigin := map[protoreflect.FullName]map[string]patternOrigin{}

	registerPattern := func(pkg protoreflect.FullName, pattern string, rd *annotationspb.ResourceDescriptor, filePath string) error {
		origins, ok := packagePatternToOrigin[pkg]
		if !ok {
			origins = map[string]patternOrigin{}
			packagePatternToOrigin[pkg] = origins
		}
		// Reject conflicting descriptors for the same pattern within a package.
		if existing, ok := origins[pattern]; ok {
			if !proto.Equal(existing.descriptor, rd) {
				return fmt.Errorf(
					"pattern %q already registered by resource %q (from %s), cannot register again for resource %q (from %s)",
					pattern, existing.descriptor.Type, existing.filePath, rd.Type, filePath,
				)
			}
		}
		origins[pattern] = patternOrigin{descriptor: rd, filePath: filePath}
		getOrCreateRegistry(pkg).resourcePatternToResourceDescriptor[pattern] = rd
		return nil
	}

	for _, f := range files {
		pkg := f.Desc.Package()
		reg := getOrCreateRegistry(pkg)
		filePath := string(f.Desc.Path())

		// Process file-level resource definitions (e.g. for resources without a dedicated message).
		fileOpts := f.Desc.Options()
		if fileOpts != nil && proto.HasExtension(fileOpts, annotationspb.E_ResourceDefinition) {
			ext := proto.GetExtension(fileOpts, annotationspb.E_ResourceDefinition)
			resourceDefinitions := ext.([]*annotationspb.ResourceDescriptor)
			for _, rd := range resourceDefinitions {
				if rd != nil && rd.Type != "" {
					reg.resourceTypeToResourceDescriptor[rd.Type] = rd
				}
				for _, pattern := range rd.Pattern {
					if err := registerPattern(pkg, pattern, rd, filePath); err != nil {
						return err
					}
				}
			}
		}

		// Process message-level resource annotations.
		for _, message := range f.Messages {
			messageType := string(message.Desc.FullName())
			msgOpts := message.Desc.Options()
			if msgOpts == nil {
				continue
			}
			if !proto.HasExtension(msgOpts, annotationspb.E_Resource) {
				continue
			}
			ext := proto.GetExtension(msgOpts, annotationspb.E_Resource)
			rd, ok := ext.(*annotationspb.ResourceDescriptor)
			if !ok || rd == nil || rd.Type == "" {
				continue
			}
			for _, pattern := range rd.Pattern {
				if err := registerPattern(pkg, pattern, rd, filePath); err != nil {
					return err
				}
			}
			reg.resourceTypeToResourceDescriptor[rd.Type] = rd
			reg.messageTypeToResourceDescriptor[messageType] = rd
			reg.resourceTypeToMessage[rd.Type] = message
		}
	}
	return nil
}

// registerAncestors discovers parent-child relationships between resources within each
// proto package by examining resource name patterns for hierarchical nesting.
func registerAncestors(files []*protogen.File) error {
	// Build a file registry so aipreflect can resolve cross-file references.
	registry := &protoregistry.Files{}
	for _, f := range files {
		if err := registry.RegisterFile(f.Desc); err != nil {
			return fmt.Errorf("failed to register file %s: %w", f.Desc.Path(), err)
		}
	}

	for _, f := range files {
		packageName := f.Desc.Package()
		reg := getOrCreateRegistry(packageName)

		aipreflect.RangeResourceDescriptorsInFile(f.Desc, func(resource *annotationspb.ResourceDescriptor) bool {
			if len(resource.Pattern) == 0 {
				return true
			}

			// Use the first pattern as canonical.
			childPattern := resource.Pattern[0]

			// Find the closest ancestor by picking the parent pattern with the most slashes.
			var immediateParent *annotationspb.ResourceDescriptor
			maxSlashCount := -1

			aipreflect.RangeParentResourcesInPackage(
				registry,
				packageName,
				childPattern,
				func(parent *annotationspb.ResourceDescriptor) bool {
					for _, p := range parent.GetPattern() {
						if resourcename.HasParent(childPattern, p) {
							depth := strings.Count(p, "/")
							if depth > maxSlashCount {
								maxSlashCount = depth
								immediateParent = parent
							}
							break
						}
					}
					return true
				},
			)

			if immediateParent != nil {
				reg.resourceTypeToParentResourceType[resource.Type] = immediateParent.Type
				if reg.resourceTypeToChildResourceTypeSet[immediateParent.Type] == nil {
					reg.resourceTypeToChildResourceTypeSet[immediateParent.Type] = map[string]struct{}{}
				}
				reg.resourceTypeToChildResourceTypeSet[immediateParent.Type][resource.Type] = struct{}{}
			}

			return true
		})
	}

	return nil
}

// ParsedResource is a fully resolved resource with its hierarchy (parent/children)
// and pattern variables extracted from the canonical resource name pattern.
type ParsedResource struct {
	Desc             *annotationspb.ResourceDescriptor
	Type             string
	Pattern          string
	Singleton        bool
	PatternVariables []string

	Parent   *ParsedResource
	Children []*ParsedResource
}

// PatternVariable returns the self-identifying variable name (the singular form).
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

func (pr *ParsedResource) PluralSnakeCase() string {
	return strs.JSONSnakeCase(pr.Desc.Plural)
}

// parseResource recursively resolves a resource descriptor into a ParsedResource,
// walking up to the parent and down to children. Results are cached in the registry.
func parseResource(reg *packageRegistry, resourceDescriptor *annotationspb.ResourceDescriptor) (*ParsedResource, error) {
	if val, ok := reg.parsedResourceTypeToParsedResource[resourceDescriptor.Type]; ok {
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

	// Walk the pattern segments to extract variable names, mapping each sub-pattern
	// back to its resource descriptor to derive the snake_case singular name.
	var sc resourcename.Scanner
	var lastSegmentIsVariable bool
	var patternVariables []string
	sc.Init(pattern)
	for sc.Scan() {
		if sc.Segment().IsVariable() {
			literal := string(sc.Segment().Literal())
			subPattern := pattern[:sc.End()]
			rd, ok := reg.resourcePatternToResourceDescriptor[subPattern]
			if !ok {
				return nil, fmt.Errorf("no resource descriptor found for sub-pattern %q (variable %q) in pattern %q", subPattern, literal, pattern)
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
	// Cache before recursing to break potential cycles.
	reg.parsedResourceTypeToParsedResource[resourceDescriptor.Type] = parsedResource

	if hasParent {
		parentResourceType, ok := reg.resourceTypeToParentResourceType[resourceDescriptor.Type]
		if !ok {
			return nil, fmt.Errorf("could not [%s]'s parent resource type", resourceDescriptor.Type)
		}
		parentResourceDescriptor, ok := reg.resourceTypeToResourceDescriptor[parentResourceType]
		if !ok {
			return nil, fmt.Errorf("resource descriptor %s not found", parentResourceType)
		}
		var err error
		parsedResource.Parent, err = parseResource(reg, parentResourceDescriptor)
		if err != nil {
			return nil, fmt.Errorf("parsing (parent) resource descriptor %s: %v", parentResourceType, err)
		}
	}

	for childResourceType := range reg.resourceTypeToChildResourceTypeSet[resourceDescriptor.Type] {
		childResourceDescriptor, ok := reg.resourceTypeToResourceDescriptor[childResourceType]
		if !ok {
			return nil, fmt.Errorf("resource descriptor %s not found", childResourceType)
		}
		child, err := parseResource(reg, childResourceDescriptor)
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

func parseResourceFromMessage(message *protogen.Message) (*ParsedResource, error) {
	pkg := message.Desc.ParentFile().Package()
	reg := getOrCreateRegistry(pkg)
	messageType := string(message.Desc.FullName())
	resourceDescriptor, ok := reg.messageTypeToResourceDescriptor[messageType]
	if !ok {
		return nil, fmt.Errorf("no resource descriptor found for message type %s", messageType)
	}
	return parseResource(reg, resourceDescriptor)
}

func getMessageUsingResourceType(resourceType string) (*protogen.Message, error) {
	for _, registry := range packageToRegistry {
		message, ok := registry.resourceTypeToMessage[resourceType]
		if ok {
			return message, nil
		}
	}
	return nil, fmt.Errorf("could not find message %q", resourceType)
}

// RPC represents a parsed standard AIP method (Create/Get/Update/Delete/List/BatchGet)
// bound to its target resource.
type RPC struct {
	StandardMethod                              *aippb.StandardMethod
	Message                                     *protogen.Message
	ParsedResource                              *ParsedResource
	Create, Update, Delete, Get, BatchGet, List bool
}

// parseRPC extracts the standard method annotation from a gRPC method and resolves
// it to a parsed resource. The package registry is derived from the method's parent file.
func parseRPC(method *protogen.Method) (*RPC, error) {
	methodOpts := method.Desc.Options()
	if methodOpts == nil {
		return nil, nil
	}
	if !proto.HasExtension(methodOpts, aippb.E_StandardMethod) {
		return nil, nil
	}

	standardMethodExt := proto.GetExtension(methodOpts, aippb.E_StandardMethod)
	standardMethod, ok := standardMethodExt.(*aippb.StandardMethod)
	if !ok || standardMethod == nil {
		return nil, fmt.Errorf("method %s has invalid standard_method annotation", method.Desc.Name())
	}

	resourceType := standardMethod.Resource
	if resourceType == "" {
		return nil, fmt.Errorf("method %s must define a resource type", method.Desc.Name())
	}

	message, err := getMessageUsingResourceType(resourceType)
	if !ok {
		return nil, fmt.Errorf("cannot find message type for resource %s", resourceType)
	}

	parsedResource, err := parseResourceFromMessage(message)
	if err != nil {
		return nil, fmt.Errorf("parsing resource %s: %w", resourceType, err)
	}

	resourceNameSingular := parsedResource.SingularGoName()
	resourceNamePlural := parsedResource.PluralGoName()
	create := method.GoName == "Create"+resourceNameSingular
	get := method.GoName == "Get"+resourceNameSingular
	batchGet := method.GoName == "BatchGet"+resourceNamePlural
	update := method.GoName == "Update"+resourceNameSingular
	delete := method.GoName == "Delete"+resourceNameSingular
	list := method.GoName == "List"+resourceNamePlural
	if !(create || get || update || delete || batchGet || list) {
		return nil, fmt.Errorf("method %s does not match any standard CRUD pattern for resource %s", method.GoName, resourceType)
	}

	return &RPC{
		StandardMethod: standardMethod,
		Message:        message,
		ParsedResource: parsedResource,
		Create:         create,
		Get:            get,
		BatchGet:       batchGet,
		Update:         update,
		Delete:         delete,
		List:           list,
	}, nil
}

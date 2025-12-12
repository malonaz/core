package main

import (
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"go.einride.tech/aip/reflect/aipreflect"
	"go.einride.tech/aip/resourcename"
	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

var (
	messageTypeToResourceDescriptor    = map[string]*annotationspb.ResourceDescriptor{}
	resourceTypeToResourceDescriptor   = map[string]*annotationspb.ResourceDescriptor{}
	resourceTypeToParentResourceType   = map[string]string{}
	resourceTypeToChildResourceTypeSet = map[string]map[string]struct{}{}
	resourceTypeToMessage              = map[string]*protogen.Message{}
)

func registerAnnotations(files []*protogen.File) error {
	for _, f := range files {
		// Register file-level resource definitions
		fileOpts := f.Desc.Options()
		if fileOpts != nil && proto.HasExtension(fileOpts, annotationspb.E_ResourceDefinition) {
			// The extension can contain multiple resource definitions
			ext := proto.GetExtension(fileOpts, annotationspb.E_ResourceDefinition)

			// Handle both single and repeated resource definitions
			switch v := ext.(type) {
			case *annotationspb.ResourceDescriptor:
				if v != nil && v.Type != "" {
					resourceTypeToResourceDescriptor[v.Type] = v
				}
			case []*annotationspb.ResourceDescriptor:
				for _, rd := range v {
					if rd != nil && rd.Type != "" {
						resourceTypeToResourceDescriptor[rd.Type] = rd
					}
				}
			}
		}

		// Register message-level resources
		for _, message := range f.Messages {
			messageType := string(message.Desc.FullName())
			msgOpts := message.Desc.Options()
			if msgOpts != nil {
				if proto.HasExtension(msgOpts, annotationspb.E_Resource) {
					ext := proto.GetExtension(msgOpts, annotationspb.E_Resource)
					rd, ok := ext.(*annotationspb.ResourceDescriptor)
					if ok && rd != nil && rd.Type != "" {
						resourceTypeToResourceDescriptor[rd.Type] = rd
						messageTypeToResourceDescriptor[messageType] = rd
						resourceTypeToMessage[rd.Type] = message
					}
				}
			}
		}
	}
	return nil
}

func registerAncestors(files []*protogen.File) error {
	// Build a registry from the files
	registry := &protoregistry.Files{}
	for _, f := range files {
		if err := registry.RegisterFile(f.Desc); err != nil {
			return fmt.Errorf("failed to register file %s: %w", f.Desc.Path(), err)
		}
	}

	// Iterate over all files to find resources and their parents
	for _, f := range files {
		packageName := f.Desc.Package()

		aipreflect.RangeResourceDescriptorsInFile(f.Desc, func(resource *annotationspb.ResourceDescriptor) bool {
			if len(resource.Pattern) == 0 {
				return true // continue
			}

			// Use the first pattern as the canonical one (consistent with the rest of the code).
			childPattern := resource.Pattern[0]

			// Track the closest (immediate) parent we can find.
			var immediateParent *annotationspb.ResourceDescriptor
			maxSlashCount := -1

			aipreflect.RangeParentResourcesInPackage(
				registry,
				packageName,
				childPattern,
				func(parent *annotationspb.ResourceDescriptor) bool {
					// A parent resource can have multiple patterns; pick the one that is an ancestor
					// of the child and has the highest depth (slash count).
					for _, p := range parent.GetPattern() {
						if resourcename.HasParent(childPattern, p) {
							// Use slash count as a depth proxy; deeper = closer.
							depth := strings.Count(p, "/")
							if depth > maxSlashCount {
								maxSlashCount = depth
								immediateParent = parent
							}
							// No need to check other patterns of this same parent once we matched one.
							break
						}
					}
					// Keep iterating to ensure we find the closest parent.
					return true
				},
			)

			// Store only the immediate parent relationship (if any).
			if immediateParent != nil {
				resourceTypeToParentResourceType[resource.Type] = immediateParent.Type
				if resourceTypeToChildResourceTypeSet[immediateParent.Type] == nil {
					resourceTypeToChildResourceTypeSet[immediateParent.Type] = map[string]struct{}{}
				}
				resourceTypeToChildResourceTypeSet[immediateParent.Type][resource.Type] = struct{}{}
			}

			return true
		})
	}

	return nil
}

type ParsedResource struct {
	Desc             *annotationspb.ResourceDescriptor
	Type             string
	Pattern          string
	Singleton        bool
	PatternVariables []string

	Parent   *ParsedResource
	Children []*ParsedResource
}

// Returns the self pattern variable.
func (pr *ParsedResource) PatternVariable() (string, error) {
	if pr.Singleton {
		return "", fmt.Errorf("called PatternVariable on singleton resource")
	}
	return pr.Desc.Singular, nil
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

var parsedResourceTypeToParsedResource = map[string]*ParsedResource{}

func parseResource(resourceDescriptor *annotationspb.ResourceDescriptor) (*ParsedResource, error) {
	if val, ok := parsedResourceTypeToParsedResource[resourceDescriptor.Type]; ok {
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

	// Parse the pattern.
	if len(resourceDescriptor.Pattern) != 1 {
		return nil, fmt.Errorf("we only support resources with single patterns")
	}
	pattern := resourceDescriptor.Pattern[0]

	// set pattern variables.
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
		Type:             resourceType,
		Pattern:          pattern,
		Singleton:        singleton,
		PatternVariables: patternVariables,
	}
	// Store it before recursing to avoid infinite loops.
	parsedResourceTypeToParsedResource[resourceDescriptor.Type] = parsedResource

	// Fetch the parent resource descriptor.
	if hasParent {
		parentResourceType, ok := resourceTypeToParentResourceType[resourceDescriptor.Type]
		if !ok {
			return nil, fmt.Errorf("could not [%s]'s parent resource type", resourceDescriptor.Type)
		}
		parentResourceDescriptor, ok := resourceTypeToResourceDescriptor[parentResourceType]
		if !ok {
			return nil, fmt.Errorf("resource descriptor %s not found", parentResourceType)
		}
		var err error
		parsedResource.Parent, err = parseResource(parentResourceDescriptor)
		if err != nil {
			return nil, fmt.Errorf("parsing (parent) resource descriptor %s: %v", parentResourceType, err)
		}
	}

	for childResourceType := range resourceTypeToChildResourceTypeSet[resourceDescriptor.Type] {
		childResourceDescriptor, ok := resourceTypeToResourceDescriptor[childResourceType]
		if !ok {
			return nil, fmt.Errorf("resource descriptor %s not found", childResourceType)
		}
		child, err := parseResource(childResourceDescriptor)
		if err != nil {
			return nil, fmt.Errorf("parsing (child) resource descriptor %s: %v", childResourceType, err)
		}
		parsedResource.Children = append(parsedResource.Children, child)
	}

	return parsedResource, nil
}

func parseResourceFromMessage(message *protogen.Message) (*ParsedResource, error) {
	// Parse the resource Descriptor.
	messageType := string(message.Desc.FullName())
	resourceDescriptor, ok := messageTypeToResourceDescriptor[messageType]
	if !ok {
		return nil, fmt.Errorf("no resource descriptor found for message type %s", messageType)
	}

	return parseResource(resourceDescriptor)
}

// CompiledResource.
type RPC struct {
	StandardMethod                    *aippb.StandardMethod
	Message                           *protogen.Message
	ParsedResource                    *ParsedResource
	Create, Update, Delete, Get, BatchGet, List  bool
}

// Given a protogen.Method
// 1. Grab the malonaz.codegen.rpc.v1.message_type annotation.
// 2. Use the 'messageTypeToMessage' to understand what message we're building a CRUD method for.
// 3. Grab the 'resource descriptor' of that mesasge.
// 4. Parse the cannonical pattern of the resource descriptor (the first one), and identify which identifiers fully characterise the 'resource'.
// 5. If we have identifiers other than the 'self' / id one, then we must have a parent field. Check that and set the parent field.
// 6. Parent field *must* have a 'resource_reference' annotation. get it and save it.
// 7. Using the resource_reference.type => find the resourceDescriptor for that referenced resource.
// 8. Save it as a field on 'StandardRPC'.
func parseRPC(method *protogen.Method) (*RPC, error) {
	// Check if we are using a standard method.
	methodOpts := method.Desc.Options()
	if methodOpts == nil {
		return nil, nil
	}
	if !proto.HasExtension(methodOpts, aippb.E_StandardMethod) {
		return nil, nil
	}

	// 1. Get the message_type annotation
	standardMethodExt := proto.GetExtension(methodOpts, aippb.E_StandardMethod)
	standardMethod, ok := standardMethodExt.(*aippb.StandardMethod)
	if !ok || standardMethod == nil {
		return nil, fmt.Errorf("method %s has invalid standard_method annotation", method.Desc.Name())
	}

	resourceType := standardMethod.Resource
	if resourceType == "" {
		return nil, fmt.Errorf("method %s must define a resource type", method.Desc.Name())
	}

	// 2. Get the message.
	message, ok := resourceTypeToMessage[resourceType]
	if !ok {
		return nil, fmt.Errorf("cannot find message type for resource %s", resourceType)
	}

	// 3. Parse the resource.
	parsedResource, err := parseResourceFromMessage(message)
	if err != nil {
		return nil, fmt.Errorf("parsing resource %s: %w", resourceType, err)
	}

	// 4. Determine the method type based on the method name
	resourceNameSingular := xstrings.ToPascalCase(parsedResource.Desc.Singular)
	resourceNamePlural := xstrings.ToPascalCase(parsedResource.Desc.Plural)
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

package main

import (
	"fmt"

	"github.com/huandu/xstrings"
	rpcpb "github.com/malonaz/core/genproto/codegen/rpc"
	"go.einride.tech/aip/reflect/aipreflect"
	"go.einride.tech/aip/resourcename"
	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

var (
	messageTypeToMessage             = map[string]*protogen.Message{}
	messageTypeToResourceDescriptor  = map[string]*annotationspb.ResourceDescriptor{}
	resourceTypeToResourceDescriptor = map[string]*annotationspb.ResourceDescriptor{}
	resourceTypeToParentResourceType = map[string]string{}
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
			messageTypeToMessage[messageType] = message

			msgOpts := message.Desc.Options()
			if msgOpts != nil {
				if proto.HasExtension(msgOpts, annotationspb.E_Resource) {
					ext := proto.GetExtension(msgOpts, annotationspb.E_Resource)
					rd, ok := ext.(*annotationspb.ResourceDescriptor)
					if ok && rd != nil && rd.Type != "" {
						resourceTypeToResourceDescriptor[rd.Type] = rd
						messageTypeToResourceDescriptor[messageType] = rd
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

		// Process all resource descriptors in this file
		aipreflect.RangeResourceDescriptorsInFile(f.Desc, func(resource *annotationspb.ResourceDescriptor) bool {
			if len(resource.Pattern) == 0 {
				return true // continue
			}

			// Use the first pattern
			pattern := resource.Pattern[0]

			// Find the immediate parent for this resource
			aipreflect.RangeParentResourcesInPackage(
				registry,
				packageName,
				pattern,
				func(parent *annotationspb.ResourceDescriptor) bool {
					// Store the parent relationship
					resourceTypeToParentResourceType[resource.Type] = parent.Type
					// Return false to stop after finding the first (immediate) parent
					return false
				},
			)

			return true // continue processing other resources
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

	Parent *ParsedResource
}

// Returns the self pattern variable.
func (pr *ParsedResource) PatternVariable() (string, error) {
	if pr.Singleton {
		return "", fmt.Errorf("called PatternVariable on singleton resource")
	}
	return pr.Desc.Singular, nil
}

func parseResource(resourceDescriptor *annotationspb.ResourceDescriptor) (*ParsedResource, error) {
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

	// Fetch the parent resource descriptor.
	var parent *ParsedResource
	if hasParent {
		parentResourceType, ok := resourceTypeToParentResourceType[resourceDescriptor.Type]
		if !ok {
			x := ""
			for k, v := range resourceTypeToParentResourceType {
				x += fmt.Sprintf("%s => %s\n", k, v)
			}
			return nil, fmt.Errorf("could not [%s]'s parent resource type: %s", resourceDescriptor.Type, x)
		}
		parentResourceDescriptor, ok := resourceTypeToResourceDescriptor[parentResourceType]
		if !ok {
			return nil, fmt.Errorf("resource descriptor %s not found", parentResourceType)
		}
		var err error
		parent, err = parseResource(parentResourceDescriptor)
		if err != nil {
			return nil, fmt.Errorf("parsing (parent) resource descriptor %s: %v", parentResourceType, err)
		}
	}

	return &ParsedResource{
		Desc:             resourceDescriptor,
		Type:             resourceType,
		Pattern:          pattern,
		Singleton:        singleton,
		PatternVariables: patternVariables,

		Parent: parent,
	}, nil
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
	MethodType     rpcpb.MethodType
	Message        *protogen.Message
	ParsedResource *ParsedResource
}

// Given a protogen.Method
// 1. Grab the malonaz.core.codegen.rpc.v1.message_type annotation.
// 2. Use the 'messageTypeToMessage' to understand what message we're building a CRUD method for.
// 3. Grab the 'resource descriptor' of that mesasge.
// 4. Parse the cannonical pattern of the resource descriptor (the first one), and identify which identifiers fully characterise the 'resource'.
// 5. If we have identifiers other than the 'self' / id one, then we must have a parent field. Check that and set the parent field.
// 6. Parent field *must* have a 'resource_reference' annotation. get it and save it.
// 7. Using the resource_reference.type => find the resourceDescriptor for that referenced resource.
// 8. Save it as a field on 'StandardRPC'.
func parseRPC(method *protogen.Method) (*RPC, error) {
	// 1. Get the message_type annotation
	methodOpts := method.Desc.Options()
	if methodOpts == nil {
		return nil, fmt.Errorf("method %s has no options", method.Desc.Name())
	}
	if !proto.HasExtension(methodOpts, rpcpb.E_MessageType) {
		return nil, fmt.Errorf("method %s has no message_type annotation", method.Desc.Name())
	}
	messageTypeExt := proto.GetExtension(methodOpts, rpcpb.E_MessageType)
	messageType, ok := messageTypeExt.(string)
	if !ok || messageType == "" {
		return nil, fmt.Errorf("method %s has invalid message_type annotation", method.Desc.Name())
	}

	// 2. Get the message using messageTypeToMessage
	message, ok := messageTypeToMessage[messageType]
	if !ok {
		return nil, fmt.Errorf("message type %s not found for method %s", messageType, method.Desc.Name())
	}

	// 3. Parse the resource.
	parsedResource, err := parseResourceFromMessage(message)
	if err != nil {
		return nil, fmt.Errorf("parsing resource for message type %s: %w", messageType, err)
	}

	// 4. Determine the method type based on the method name
	resourceNameSingular := xstrings.ToPascalCase(parsedResource.Desc.Singular)
	resourceNamePlural := xstrings.ToPascalCase(parsedResource.Desc.Plural)
	var methodType rpcpb.MethodType
	switch method.GoName {
	case "Create" + resourceNameSingular:
		methodType = rpcpb.MethodType_METHOD_TYPE_CREATE
	case "Get" + resourceNameSingular:
		methodType = rpcpb.MethodType_METHOD_TYPE_GET
	case "Update" + resourceNameSingular:
		methodType = rpcpb.MethodType_METHOD_TYPE_UPDATE
	case "Delete" + resourceNameSingular:
		methodType = rpcpb.MethodType_METHOD_TYPE_DELETE
	case "List" + resourceNamePlural:
		methodType = rpcpb.MethodType_METHOD_TYPE_LIST
	default:
		return nil, fmt.Errorf("method %s does not match any standard method pattern for resource %s", method.GoName, resourceNameSingular)
	}

	return &RPC{
		MethodType:     methodType,
		Message:        message,
		ParsedResource: parsedResource,
	}, nil

}

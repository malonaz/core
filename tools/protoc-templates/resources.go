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
	for _, f := range files {
		for _, service := range f.Services {
			for _, method := range service.Methods {
				// Check if method has message_type annotation
				methodOpts := method.Desc.Options()
				if methodOpts == nil {
					continue
				}
				if !proto.HasExtension(methodOpts, rpcpb.E_MessageType) {
					continue
				}

				// Get the message type
				messageTypeExt := proto.GetExtension(methodOpts, rpcpb.E_MessageType)
				messageType, ok := messageTypeExt.(string)
				if !ok || messageType == "" {
					continue
				}

				// Get the resource descriptor for this message
				resourceDescriptor, ok := messageTypeToResourceDescriptor[messageType]
				if !ok {
					continue
				}

				// Check if this is a LIST method by checking the method name
				resourceNamePlural := xstrings.ToPascalCase(resourceDescriptor.Plural)
				if method.GoName != "List"+resourceNamePlural {
					continue
				}

				// Check if input message has a 'parent' field
				parentField := method.Input.Desc.Fields().ByName("parent")
				if parentField == nil {
					continue
				}

				// Get the resource_reference annotation from parent field
				parentFieldOpts := parentField.Options()
				if parentFieldOpts == nil || !proto.HasExtension(parentFieldOpts, annotationspb.E_ResourceReference) {
					return fmt.Errorf("parent field in LIST method %s must have resource_reference annotation", method.Desc.Name())
				}

				parentResourceRefExt := proto.GetExtension(parentFieldOpts, annotationspb.E_ResourceReference)
				parentResourceRef, ok := parentResourceRefExt.(*annotationspb.ResourceReference)
				if !ok || parentResourceRef == nil {
					return fmt.Errorf("invalid resource_reference on parent field in LIST method %s", method.Desc.Name())
				}

				// Store the mapping: child resource type -> parent resource type
				resourceTypeToParentResourceType[resourceDescriptor.Type] = parentResourceRef.Type
			}
		}
	}
	return nil
}

type ParsedResource struct {
	Message *protogen.Message
	RD      *annotationspb.ResourceDescriptor
	PRD     *annotationspb.ResourceDescriptor

	// Pattern variables
	Pattern                string
	Singleton              bool
	ParentPatternVariables []string
	PatternVariables       []string
}

func (pr *ParsedResource) ResourceType() (string, error) {
	t := pr.RD.GetType()
	if t == "" {
		return "", fmt.Errorf("no resource type")
	}
	return aipreflect.ResourceType(t).Type(), nil
}

func (pr *ParsedResource) ResourceName() (string, error) {
	resourceType, err := pr.ResourceType()
	if err != nil {
		return "", err
	}
	return resourceType + "ResourceName", nil
}

func (pr *ParsedResource) ParentResourceType() (string, error) {
	t := pr.RD.GetType()
	if t == "" {
		return "", fmt.Errorf("no parent resource type")
	}
	return aipreflect.ResourceType(t).Type(), nil
}

func (pr *ParsedResource) ParentResourceName() (string, error) {
	parentResourceType, err := pr.ParentResourceType()
	if err != nil {
		return "", err
	}
	return parentResourceType + "ResourceName", nil
}

// Retursn the self pattern variable.
func (pr *ParsedResource) PatternVariable() (string, error) {
	if pr.Singleton {
		return "", fmt.Errorf("called PatternVariable on singleton resource")
	}
	return pr.RD.Singular, nil
}

func (pr *ParsedResource) HasParent() bool {
	return len(pr.ParentPatternVariables) > 0
}

func parseResource(message *protogen.Message) (*ParsedResource, error) {
	// Parse the resource Descriptor.
	messageType := string(message.Desc.FullName())
	resourceDescriptor, ok := messageTypeToResourceDescriptor[messageType]
	if !ok {
		return nil, fmt.Errorf("no resource descriptor found for message type %s", messageType)
	}

	// Parse the pattern.
	if len(resourceDescriptor.Pattern) != 1 {
		return nil, fmt.Errorf("we only support resources with single patterns")
	}
	pattern := resourceDescriptor.Pattern[0]

	// set pattern variables.
	var sc resourcename.Scanner
	var singleton bool
	var parentPatternVariables []string
	var patternVariables []string
	sc.Init(pattern)
	for sc.Scan() {
		if sc.Segment().IsVariable() {
			patternVariable := string(sc.Segment().Literal())
			if patternVariable == resourceDescriptor.Singular {
				singleton = true
			} else {
				parentPatternVariables = append(parentPatternVariables, patternVariable)
			}
			patternVariables = append(patternVariables, patternVariable)
		}
	}

	// Fetch the parent resource descriptor.
	var parentResourceDescriptor *annotationspb.ResourceDescriptor
	if len(parentPatternVariables) > 0 {
		parentResourceType, ok := resourceTypeToParentResourceType[resourceDescriptor.Type]
		if !ok {
			return nil, fmt.Errorf("parent resource type %s not found", resourceDescriptor.Type)
		}
		parentResourceDescriptor, ok = resourceTypeToResourceDescriptor[parentResourceType]
		if !ok {
			return nil, fmt.Errorf("resource descriptor %s not found", parentResourceType)
		}
	}

	return &ParsedResource{
		Message: message,
		RD:      resourceDescriptor,
		PRD:     parentResourceDescriptor,

		Pattern:                pattern,
		Singleton:              singleton,
		ParentPatternVariables: parentPatternVariables,
		PatternVariables:       patternVariables,
	}, nil

}

type ParsedResourceDescriptor struct {
	ResourceDescriptor *annotationspb.ResourceDescriptor
	Identifier         string
	ParentIdentifiers  []string
	AllIdentifiers     []string
}

func parseResourceDescriptor(message *protogen.Message) (*ParsedResourceDescriptor, error) {
	// 1. Get the resource descriptor for this message
	messageType := string(message.Desc.FullName())
	resourceDescriptor, ok := messageTypeToResourceDescriptor[messageType]
	if !ok {
		return nil, fmt.Errorf("no resource descriptor found for message type %s", messageType)
	}
	if resourceDescriptor.Type == "" {
		return nil, fmt.Errorf("%s has no type defined", resourceDescriptor.Singular)
	}
	if len(resourceDescriptor.Pattern) != 1 {
		return nil, fmt.Errorf("expected 1 pattern got %d for %s", len(resourceDescriptor.Pattern), resourceDescriptor.Singular)
	}

	allIdentifiers := parseIdentifiersFromPattern(resourceDescriptor.Pattern[0])
	identifier := ""
	parentIdentifiers := make([]string, 0, len(allIdentifiers))
	// Iterate in reverse.
	for i := len(allIdentifiers) - 1; i >= 0; i-- {
		currentIdentifier := allIdentifiers[i]
		if currentIdentifier == resourceDescriptor.Singular {
			identifier = currentIdentifier
		} else {
			parentIdentifiers = append(parentIdentifiers, currentIdentifier)
		}
	}

	return &ParsedResourceDescriptor{
		ResourceDescriptor: resourceDescriptor,
		Identifier:         identifier,
		ParentIdentifiers:  parentIdentifiers,
		AllIdentifiers:     allIdentifiers,
	}, nil
}

// CompiledResource.
type RPC struct {
	MethodType               rpcpb.MethodType
	ParsedResource           *ParsedResource
	Message                  *protogen.Message
	ParsedResourceDescriptor *ParsedResourceDescriptor
	ParentResourceDescriptor *annotationspb.ResourceDescriptor
}

func (rpc *RPC) ResourceType() (string, error) {
	t := rpc.ParsedResourceDescriptor.ResourceDescriptor.GetType()
	if t == "" {
		return "", fmt.Errorf("no resource type")
	}
	return aipreflect.ResourceType(t).Type(), nil
}

func (rpc *RPC) ResourceName() (string, error) {
	resourceType, err := rpc.ResourceType()
	if err != nil {
		return "", err
	}
	return resourceType + "ResourceName", nil
}

func (rpc *RPC) ParentResourceType() (string, error) {
	t := rpc.ParentResourceDescriptor.GetType()
	if t == "" {
		return "", fmt.Errorf("no parent resource type")
	}
	return aipreflect.ResourceType(t).Type(), nil
}

func (rpc *RPC) ParentResourceName() (string, error) {
	parentResourceType, err := rpc.ParentResourceType()
	if err != nil {
		return "", err
	}
	return parentResourceType + "ResourceName", nil
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
	parsedResource, err := parseResource(message)
	if err != nil {
		return nil, fmt.Errorf("parsing resource for message type %s: %w", messageType, err)
	}
	_ = parsedResource

	// 3. Parse the resource descriptor.
	prd, err := parseResourceDescriptor(message)
	if err != nil {
		return nil, fmt.Errorf("parsing resource descriptor: %v", err)
	}

	// 4. Find the parent resource descriptor if it exists.
	var parentResourceDescriptor *annotationspb.ResourceDescriptor
	if len(prd.ParentIdentifiers) > 0 {
		parentResourceType, ok := resourceTypeToParentResourceType[prd.ResourceDescriptor.Type]
		if !ok {
			return nil, fmt.Errorf("parent resource type not found for method %s", method.Desc.Name())
		}
		parentResourceDescriptor, ok = resourceTypeToResourceDescriptor[parentResourceType]
		if !ok {
			return nil, fmt.Errorf("parent resource descriptor not found for method %s", method.Desc.Name())
		}
	}

	// 5. Determine the method type based on the method name
	resourceNameSingular := xstrings.ToPascalCase(prd.ResourceDescriptor.Singular)
	resourceNamePlural := xstrings.ToPascalCase(prd.ResourceDescriptor.Plural)
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

	// 4. Grab the parent resource descriptor.
	return &RPC{
		MethodType:               methodType,
		Message:                  message,
		ParsedResourceDescriptor: prd,
		ParentResourceDescriptor: parentResourceDescriptor,
	}, nil

}
